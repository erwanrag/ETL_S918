"""
============================================================================
Tasks STAGING - RAW → STAGING_ETL
============================================================================
Responsabilités :
- Créer les tables staging_etl.stg_{table} typées (à partir metadata)
- Charger les données depuis raw.raw_{table} avec nettoyage SQL typé
- Calculer _etl_hashdiff, _etl_valid_from, _etl_run_id
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
from utils.types import build_table_columns_sql


# ============================================================================
# CREATE TABLE STAGING
# ============================================================================

@task(name="🧱 Créer table STAGING typée")
def create_staging_table(table_name: str):
    """
    Crée staging_etl.stg_{table} à partir de metadata.proginovcolumns
    """
    logger = get_run_logger()
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()

    base = table_name.split("_", 1)[-1].lower()
    stg_table = f"staging_etl.stg_{base}"

    try:
        # Récupérer métadonnées complètes
        cols_meta: Dict[str, Dict] = get_columns_metadata(base)
        if not cols_meta:
            logger.error(f"❌ Aucune metadata trouvée pour {base}")
            return

        # Colonnes typées
        columns_sql = build_table_columns_sql(cols_meta)

        # Colonnes ETL
        columns_sql += """,
    _etl_hashdiff TEXT,
    _etl_valid_from TIMESTAMP,
    _etl_run_id TEXT
        """

        # Supprime l'ancienne table
        logger.info(f"🧨 DROP TABLE IF EXISTS {stg_table}")
        cur.execute(f"DROP TABLE IF EXISTS {stg_table} CASCADE")

        # Crée la nouvelle table typée
        logger.info(f"🧱 CREATE TABLE {stg_table}")
        create_sql = f"""
            CREATE SCHEMA IF NOT EXISTS staging_etl;

            CREATE TABLE {stg_table} (
                {columns_sql}
            );
        """
        cur.execute(create_sql)
        conn.commit()

        logger.info(f"✅ Table {stg_table} créée")

    except Exception as e:
        logger.error(f"❌ Erreur création {stg_table}: {e}")
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()



# ============================================================================
# INSERT RAW → STAGING
# ============================================================================

@task(name="📥 Charger RAW → STAGING avec nettoyage + hashdiff")
def load_raw_to_staging(table_name: str, run_id: str):
    """
    Insert des données depuis raw.raw_{table} vers staging_etl.stg_{table}
    Nettoyage :
      - TRIM sur les chaînes
      - CAST typés selon metadata
      - Gestion NULL
    Génère :
      - _etl_hashdiff
      - _etl_valid_from
      - _etl_run_id
    """
    logger = get_run_logger()
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()

    base = table_name.split("_", 1)[-1].lower()
    raw_table = f"raw.raw_{base}"
    stg_table = f"staging_etl.stg_{base}"

    try:
        # Vérifier existence RAW
        cur.execute("""
            SELECT EXISTS (
                SELECT 1 
                FROM information_schema.tables
                WHERE table_schema = 'raw'
                  AND table_name = %s
            )
        """, (f"raw_{base}",))
        
        if not cur.fetchone()[0]:
            logger.error(f"❌ Table {raw_table} introuvable")
            return

        # Métadata colonnes
        cols_meta = get_columns_metadata(base)
        business_cols = list(cols_meta.keys())

        if not business_cols:
            logger.error(f"❌ Aucune colonne business pour {base}")
            return

        # ===================================================================
        # Construire SELECT typé & nettoyé
        # ===================================================================
        select_exprs: List[str] = []

        for col, info in cols_meta.items():
            pt = (info.get("ProgressType") or "").lower()
            dt = (info.get("DataType") or "").lower()

            source = f"\"{col}\""

            # -----------------------
            # BOOLEAN
            # -----------------------
            if pt in ("logical", "bit"):
                expr = f"""
CASE 
    WHEN UPPER(BTRIM({source}::text)) IN ('1','Y','YES','TRUE','OUI') THEN TRUE
    WHEN UPPER(BTRIM({source}::text)) IN ('0','N','NO','FALSE','NON') THEN FALSE
    ELSE NULL
END AS "{col}"
                """.strip()

            # -----------------------
            # INTEGER
            # -----------------------
            elif pt in ("integer", "int", "int64") or dt in ("integer", "int"):
                expr = f"NULLIF(BTRIM({source}::text), '')::INTEGER AS \"{col}\""

            # -----------------------
            # NUMERIC
            # -----------------------
            elif pt in ("decimal", "numeric") or dt in ("decimal", "numeric"):
                expr = f"NULLIF(BTRIM({source}::text), '')::NUMERIC AS \"{col}\""

            # -----------------------
            # DATE
            # -----------------------
            elif pt == "date" or dt == "date":
                expr = f"""
CASE 
    WHEN {source} IN ('', '00/00/00', '00-00-00') THEN NULL
    ELSE NULLIF(BTRIM({source}::text), '')::DATE
END AS "{col}"
                """.strip()

            # -----------------------
            # TIMESTAMP
            # -----------------------
            elif pt in ("datetime", "timestamp") or dt == "timestamp":
                expr = f"NULLIF(BTRIM({source}::text), '')::TIMESTAMP AS \"{col}\""

            # -----------------------
            # TEXT / CHARACTER
            # -----------------------
            elif pt == "character" or dt in ("varchar", "character", "text"):
                expr = f"NULLIF(BTRIM({source}::text), '') AS \"{col}\""

            # -----------------------
            # FALLBACK = TEXT
            # -----------------------
            else:
                expr = f"NULLIF(BTRIM({source}::text), '') AS \"{col}\""

            select_exprs.append(expr)

        # ===================================================================
        # HASHDIFF
        # ===================================================================
        hash_concat = ", ".join([f"COALESCE(\"{c}\"::text, '')" for c in business_cols])
        hash_expr = f"MD5(CONCAT_WS('|', {hash_concat})) AS _etl_hashdiff"

        # ===================================================================
        # SELECT complet
        # ===================================================================
        select_sql = ",\n                ".join(select_exprs + [
            hash_expr,
            "CURRENT_TIMESTAMP AS _etl_valid_from",
            f"'{run_id}' AS _etl_run_id"
        ])

        # INSERT
        insert_sql = f"""
            INSERT INTO {stg_table} (
                {', '.join(['"' + c + '"' for c in business_cols])},
                _etl_hashdiff,
                _etl_valid_from,
                _etl_run_id
            )
            SELECT
                {select_sql}
            FROM {raw_table}
        """

        logger.info(f"📥 INSERT RAW → {stg_table}")
        cur.execute(insert_sql)
        conn.commit()

        logger.info(f"✅ {cur.rowcount:,} lignes insérées dans {stg_table}")

        return cur.rowcount

    except Exception as e:
        logger.error(f"❌ Erreur load_raw_to_staging({table_name}): {e}")
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()
