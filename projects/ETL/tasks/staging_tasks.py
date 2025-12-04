"""
============================================================================
Tasks STAGING - RAW → STAGING_ETL avec support ConfigName
============================================================================
[FIX] Support ConfigName pour tables multi-configurations (lisval)
============================================================================
"""

from prefect import task
from prefect.logging import get_run_logger
import psycopg2
from typing import Dict, List
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))
from flows.config.pg_config import config
from utils.metadata_helper import get_columns_metadata
from utils.custom_types import build_table_columns_sql


@task(name="[BUILD] Créer table STAGING typée")
def create_staging_table(table_name: str):
    """
    Crée staging_etl.stg_{table_name} à partir de metadata
    
    Args:
        table_name: Peut être:
            - 'client' (simple)
            - 'lisval_fou_production' (avec ConfigName)
    """
    logger = get_run_logger()
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()

    # Lookup metadata : ConfigName-first

    # 1. Essayer correspondance exacte sur ConfigName
    cur.execute("""
        SELECT "TableName", "ConfigName"
        FROM metadata.etl_tables
        WHERE "ConfigName" = %s
        LIMIT 1
    """, (table_name,))
    row = cur.fetchone()

    # 2. Sinon fallback sur TableName
    if not row:
        cur.execute("""
            SELECT "TableName", "ConfigName"
            FROM metadata.etl_tables
            WHERE "TableName" = %s
            LIMIT 1
        """, (table_name,))
        row = cur.fetchone()

    if row:
        base_table = row[0]               # colonnes métier
        physical_name = row[1] or row[0]  # nom physique RAW/STAGING
    else:
        base_table = table_name
        physical_name = table_name
        logger.warning(f"[WARN] Pas de metadata pour {table_name}, utilisation directe")

    stg_table = f"staging_etl.stg_{physical_name.lower()}"

    logger.info(f"[CONFIG] table={table_name} → base={base_table}, physical={physical_name}")

    try:
        cols_meta = get_columns_metadata(base_table)
        if not cols_meta:
            logger.error(f"[ERROR] Aucune metadata colonnes pour {base_table}")
            return

        columns_sql = build_table_columns_sql(cols_meta)
        columns_sql += """,
    _etl_hashdiff TEXT,
    _etl_valid_from TIMESTAMP,
    _etl_run_id TEXT
        """

        logger.info(f"[DROP] DROP TABLE IF EXISTS {stg_table}")
        cur.execute(f"DROP TABLE IF EXISTS {stg_table} CASCADE")

        logger.info(f"[BUILD] CREATE TABLE {stg_table}")
        create_sql = f"""
            CREATE SCHEMA IF NOT EXISTS staging_etl;
            CREATE TABLE {stg_table} (
                {columns_sql}
            );
        """
        cur.execute(create_sql)
        conn.commit()

        logger.info(f"[OK] Table {stg_table} créée")

    except Exception as e:
        conn.rollback()
        logger.error(f"[ERROR] Erreur création STAGING {stg_table}: {e}")
        raise

    finally:
        cur.close()
        conn.close()


@task(name="📥 Charger RAW → STAGING avec nettoyage + hashdiff")
def load_raw_to_staging(table_name: str, run_id: str):
    logger = get_run_logger()
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()

    # Lookup metadata : ConfigName-first
    cur.execute("""
        SELECT "TableName", "ConfigName"
        FROM metadata.etl_tables
        WHERE "ConfigName" = %s OR "TableName" = %s
        LIMIT 1
    """, (table_name, table_name))

    row = cur.fetchone()
    if row:
        base_table = row[0]               # colonnes métier
        physical_name = row[1] or row[0]  # nom physique RAW/STAGING
    else:
        base_table = table_name
        physical_name = table_name

    raw_table = f"raw.raw_{physical_name.lower()}"
    stg_table = f"staging_etl.stg_{physical_name.lower()}"

    logger.info(f"[DATA] RAW={raw_table}, STAGING={stg_table}, BASE={base_table}")

    try:
        # Vérifier existence RAW
        cur.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'raw'
                AND table_name = %s
            )
        """, (f"raw_{physical_name.lower()}",))

        if not cur.fetchone()[0]:
            logger.error(f"[ERROR] Table RAW introuvable : {raw_table}")
            return

        # Métadonnées colonnes
        cols_meta = get_columns_metadata(base_table)
        business_cols = list(cols_meta.keys())

        if not business_cols:
            logger.error(f"[ERROR] Colonnes non trouvées pour {base_table}")
            return

        # ==============================
        #  Construction SELECT typé
        # ==============================
        select_exprs = []

        for col, info in cols_meta.items():
            pt = (info.get("ProgressType") or "").lower()
            dt = (info.get("DataType") or "").lower()
            extent = info.get("Extent", 0) or 0

            source = f'"{col}"'

            # BOOLEAN
            if pt in ("logical", "bit") and extent == 0:
                expr = f"""
CASE 
    WHEN UPPER(BTRIM({source}::text)) IN ('1','Y','YES','TRUE','OUI') THEN TRUE
    WHEN UPPER(BTRIM({source}::text)) IN ('0','N','NO','FALSE','NON') THEN FALSE
    ELSE NULL
END AS "{col}"
                """.strip()

            # INTEGER
            elif (pt in ("integer", "int", "int64") or dt in ("integer", "int")) and extent == 0:
                expr = f"NULLIF(BTRIM({source}::text), '')::INTEGER AS \"{col}\""

            # NUMERIC
            elif (pt in ("decimal", "numeric") or dt in ("decimal", "numeric")) and extent == 0:
                expr = f"NULLIF(BTRIM({source}::text), '')::NUMERIC AS \"{col}\""

            # DATE
            elif (pt == "date" or dt == "date") and extent == 0:
                expr = f"""
CASE 
    WHEN {source} IN ('', '00/00/00', '00-00-00') THEN NULL
    ELSE NULLIF(BTRIM({source}::text), '')::DATE
END AS "{col}"
                """.strip()

            # TIMESTAMP
            elif pt in ("datetime", "timestamp") or dt == "timestamp":
                expr = f"NULLIF(BTRIM({source}::text), '')::TIMESTAMP AS \"{col}\""

            # TEXT / fallback
            else:
                expr = f"NULLIF(BTRIM({source}::text), '') AS \"{col}\""

            select_exprs.append(expr)

        # ==============================
        #  HASHDIFF
        # ==============================
        if len(business_cols) <= 95:
            hash_concat = ", ".join([f"COALESCE(\"{c}\"::text, '')" for c in business_cols])
            hash_expr = f"MD5(CONCAT_WS('|', {hash_concat})) AS _etl_hashdiff"
        else:
            chunk_size = 95
            chunks = []
            for i in range(0, len(business_cols), chunk_size):
                chunk_cols = business_cols[i:i+chunk_size]
                chunk_concat = ", ".join([f"COALESCE(\"{c}\"::text, '')" for c in chunk_cols])
                chunks.append(f"CONCAT_WS('|', {chunk_concat})")

            all_chunks = " || '|' || ".join(chunks)
            hash_expr = f"MD5({all_chunks}) AS _etl_hashdiff"

        # ==============================
        #  SELECT COMPLET
        # ==============================
        select_sql = ",\n                ".join(select_exprs + [
            hash_expr,
            "CURRENT_TIMESTAMP AS _etl_valid_from",
            f"'{run_id}' AS _etl_run_id"
        ])

        # ==============================
        #  INSERT INTO STAGING
        # ==============================
        insert_sql = f"""
            INSERT INTO {stg_table} (
                {', '.join(f'"{c}"' for c in business_cols)},
                _etl_hashdiff,
                _etl_valid_from,
                _etl_run_id
            )
            SELECT
                {select_sql}
            FROM {raw_table}
        """

        logger.info(f"📥 INSERT → {stg_table}")
        cur.execute(insert_sql)
        conn.commit()

        logger.info(f"[OK] {cur.rowcount:,} lignes insérées dans {stg_table}")
        return cur.rowcount

    except Exception as e:
        conn.rollback()
        logger.error(f"[ERROR] load_raw_to_staging({table_name}): {e}")
        raise

    finally:
        cur.close()
        conn.close()


