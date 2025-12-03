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
from utils.custom_types import build_table_columns_sql


# ============================================================================
# CREATE TABLE STAGING
# ============================================================================

@task(name="[BUILD] Créer table STAGING typée")
def create_staging_table(table_name: str):
    """
    Crée staging_etl.stg_{table} à partir de metadata.proginovcolumns
    Les colonnes avec Extent > 0 sont automatiquement en TEXT (géré par types.py)
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
            logger.error(f"[ERROR] Aucune metadata trouvée pour {base}")
            return

        # Colonnes typées (types.py gère automatiquement Extent → TEXT)
        columns_sql = build_table_columns_sql(cols_meta)

        # Colonnes ETL
        columns_sql += """,
    _etl_hashdiff TEXT,
    _etl_valid_from TIMESTAMP,
    _etl_run_id TEXT
        """

        # Supprime l'ancienne table
        logger.info(f"[DROP] DROP TABLE IF EXISTS {stg_table}")
        cur.execute(f"DROP TABLE IF EXISTS {stg_table} CASCADE")

        # Crée la nouvelle table typée
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
        logger.error(f"[ERROR] Erreur création {stg_table}: {e}")
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
    
    IMPORTANT : Gestion intelligente des colonnes Extent
    - Extent > 0 : garde en TEXT (multi-values "val1;val2;val3")
    - Extent = 0 : cast selon ProgressType
    
    Nettoyage :
      - TRIM sur les chaînes
      - CAST typés selon metadata + Extent
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
            logger.error(f"[ERROR] Table {raw_table} introuvable")
            return

        # Métadata colonnes
        cols_meta = get_columns_metadata(base)
        business_cols = list(cols_meta.keys())

        if not business_cols:
            logger.error(f"[ERROR] Aucune colonne business pour {base}")
            return

        # Debug : afficher colonnes Extent
        extent_count = 0
        for col, meta in cols_meta.items():
            extent = meta.get("Extent", 0) or 0
            if extent > 0:
                extent_count += 1
                logger.info(f"  [MERGE] {col}: Extent={extent}, ProgressType={meta.get('ProgressType')}")
        
        if extent_count > 0:
            logger.info(f"[DATA] {extent_count} colonne(s) avec Extent détectée(s)")

        # ===================================================================
        # Construire SELECT typé & nettoyé
        # ===================================================================
        select_exprs: List[str] = []

        for col, info in cols_meta.items():
            pt = (info.get("ProgressType") or "").lower()
            dt = (info.get("DataType") or "").lower()
            extent = info.get("Extent", 0) or 0  # [CRITICAL] RÉCUPÉRER EXTENT

            source = f'"{col}"'

            # -----------------------
            # BOOLEAN (seulement si Extent = 0)
            # -----------------------
            if pt in ("logical", "bit") and extent == 0:
                expr = f"""
CASE 
    WHEN UPPER(BTRIM({source}::text)) IN ('1','Y','YES','TRUE','OUI') THEN TRUE
    WHEN UPPER(BTRIM({source}::text)) IN ('0','N','NO','FALSE','NON') THEN FALSE
    ELSE NULL
END AS "{col}"
                """.strip()

            # -----------------------
            # INTEGER (seulement si Extent = 0)
            # -----------------------
            elif (pt in ("integer", "int", "int64") or dt in ("integer", "int")) and extent == 0:
                expr = f"NULLIF(BTRIM({source}::text), '')::INTEGER AS \"{col}\""

            # -----------------------
            # NUMERIC (seulement si Extent = 0)
            # -----------------------
            elif (pt in ("decimal", "numeric") or dt in ("decimal", "numeric")) and extent == 0:
                expr = f"NULLIF(BTRIM({source}::text), '')::NUMERIC AS \"{col}\""

            # -----------------------
            # DATE (seulement si Extent = 0)
            # -----------------------
            elif (pt == "date" or dt == "date") and extent == 0:
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
            # TEXT / CHARACTER (ou Extent > 0)
            # -----------------------
            elif pt == "character" or dt in ("varchar", "character", "text"):
                expr = f"NULLIF(BTRIM({source}::text), '') AS \"{col}\""

            # -----------------------
            # FALLBACK = TEXT (y compris colonnes Extent)
            # -----------------------
            else:
                expr = f"NULLIF(BTRIM({source}::text), '') AS \"{col}\""

            select_exprs.append(expr)

        # ===================================================================
        # HASHDIFF (avec gestion limite 100 arguments PostgreSQL)
        # ===================================================================
        # PostgreSQL limite CONCAT_WS à 100 arguments
        # Si plus de 95 colonnes, on concatène par chunks puis on hash le résultat
        if len(business_cols) <= 95:
            # Méthode simple : tout en un coup
            hash_concat = ", ".join([f"COALESCE(\"{c}\"::text, '')" for c in business_cols])
            hash_expr = f"MD5(CONCAT_WS('|', {hash_concat})) AS _etl_hashdiff"
        else:
            # Méthode par chunks de 95 colonnes
            chunk_size = 95
            chunks = []
            for i in range(0, len(business_cols), chunk_size):
                chunk_cols = business_cols[i:i+chunk_size]
                chunk_concat = ", ".join([f"COALESCE(\"{c}\"::text, '')" for c in chunk_cols])
                chunks.append(f"CONCAT_WS('|', {chunk_concat})")
            
            # Concaténer les chunks puis hasher
            all_chunks = " || '|' || ".join(chunks)
            hash_expr = f"MD5({all_chunks}) AS _etl_hashdiff"
            
            logger.info(f"[WARN] Table large : {len(business_cols)} colonnes → {len(chunks)} chunks pour hashdiff")

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

        logger.info(f"[OK] {cur.rowcount:,} lignes insérées dans {stg_table}")

        return cur.rowcount

    except Exception as e:
        logger.error(f"[ERROR] Erreur load_raw_to_staging({table_name}): {e}")
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()