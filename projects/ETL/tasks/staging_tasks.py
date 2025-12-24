"""
============================================================================
Tasks STAGING - RAW → STAGING_ETL avec EXTENT ÉCLATÉ
============================================================================
[FIX FINAL] Hashdiff calculé APRÈS éclatement extent dans sous-requête
============================================================================
"""

from prefect import task
from prefect.logging import get_run_logger
import psycopg2
from typing import Dict, List
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from shared.config import config
from utils.metadata_helper import get_columns_metadata
from utils.custom_types import build_table_columns_sql

# [NEW] Import extent handler
from utils.extent_handler import (
    has_extent_columns,
    get_extent_columns_with_metadata,
    get_pg_type_for_extent_column,
    build_ods_select_with_extent_typed
)


def _should_recreate_staging_table(
    cur, 
    table_name: str, 
    load_mode: str,
    logger
) -> bool:
    """
    Détermine si la table STAGING doit être DROP/CREATE
    
    Returns:
        True si doit être recréée
        False si peut être réutilisée
    """
    
    if load_mode not in ("INCREMENTAL", "AUTO"):
        # Mode FULL : toujours recréer
        return True
    
    stg_table = f"stg_{table_name.lower()}"
    
    # Vérifier si table existe
    cur.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'staging_etl' 
            AND table_name = %s
        )
    """, (stg_table,))
    
    table_exists = cur.fetchone()[0]
    
    if not table_exists:
        logger.info(f"[CREATE] Table {stg_table} n'existe pas → création")
        return True
    
    # Table existe, vérifier colonnes critiques
    cur.execute("""
        SELECT COUNT(*) 
        FROM information_schema.columns 
        WHERE table_schema = 'staging_etl' 
          AND table_name = %s
          AND column_name IN ('_etl_hashdiff', '_etl_valid_from', '_etl_run_id')
    """, (stg_table,))
    
    critical_cols_count = cur.fetchone()[0]
    
    if critical_cols_count < 3:
        logger.warning(f"[REBUILD] Colonnes ETL manquantes → recréation")
        return True
    
    # Tout est OK, pas besoin de recréer
    logger.info(f"[SKIP CREATE] Table {stg_table} existe et valide → réutilisation")
    return False

@task(name="[BUILD] Créer table STAGING typée (avec extent éclaté)")
def create_staging_table(table_name: str, load_mode: str = "AUTO"):
    """
    Crée staging_etl.stg_{table_name} à partir de metadata
    [OPTIMISÉ] Skip DROP/CREATE si table existe et mode INCREMENTAL
    """
    logger = get_run_logger()
    logger.info(f"[CONFIG] table={table_name}, mode={load_mode}")
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()

    # Lookup metadata : ConfigName-first
    cur.execute("""
        SELECT "TableName", "ConfigName"
        FROM metadata.etl_tables
        WHERE "ConfigName" = %s
        LIMIT 1
    """, (table_name,))
    row = cur.fetchone()

    if not row:
        cur.execute("""
            SELECT "TableName", "ConfigName"
            FROM metadata.etl_tables
            WHERE "TableName" = %s
            LIMIT 1
        """, (table_name,))
        row = cur.fetchone()

    if row:
        base_table = row[0]
        physical_name = row[1] or row[0]
    else:
        base_table = table_name
        physical_name = table_name
        logger.warning(f"[WARN] Pas de metadata pour {table_name}, utilisation directe")

    stg_table = f"staging_etl.stg_{physical_name.lower()}"

    logger.info(f"[CONFIG] table={table_name} → base={base_table}, physical={physical_name}")

    try:
        # ✅ OPTIMISATION : Vérifier si recréation nécessaire
        if not _should_recreate_staging_table(cur, physical_name, load_mode, logger):
            # Table OK, skip création
            cur.close()
            conn.close()
            return
        
        # Si on arrive ici, il faut créer la table
        cols_meta = get_columns_metadata(base_table)
        if not cols_meta:
            logger.error(f"[ERROR] Aucune metadata colonnes pour {base_table}")
            return

        logger.info(f"[DROP] DROP TABLE IF EXISTS {stg_table}")
        cur.execute(f"DROP TABLE IF EXISTS {stg_table} CASCADE")

        # ============================================================
        # [NEW] VÉRIFIER SI EXTENT PRÉSENT
        # ============================================================
        if has_extent_columns(base_table):
            logger.info(f"[EXTENT] Table avec extent - Éclatement dans STAGING")
            
            # Récupérer metadata extent
            extent_metadata = get_extent_columns_with_metadata(base_table)
            
            # Construire colonnes STAGING avec extent éclatés
            col_defs = []
            
            for col_name, col_info in cols_meta.items():
                
                # Si colonne extent : l'éclater
                if col_name in extent_metadata:
                    meta = extent_metadata[col_name]
                    extent = meta['extent']
                    pg_type = get_pg_type_for_extent_column(
                        meta['progress_type'],
                        meta['data_type'],
                        meta['width'],
                        meta['scale']
                    )
                    
                    logger.info(f"[EXTENT] {col_name} → {extent} colonnes ({pg_type})")
                    
                    # Créer col_1, col_2, ..., col_n
                    for i in range(1, extent + 1):
                        expanded_col = f"{col_name}_{i}"
                        col_defs.append(f'    "{expanded_col}" {pg_type}')
                
                # Sinon colonne normale
                else:
                    from utils.custom_types import build_column_definition
                    col_def = build_column_definition(col_info)
                    col_defs.append(f"    {col_def}")
            
            # Colonnes techniques
            col_defs.extend([
                '    _etl_hashdiff TEXT',
                '    _etl_valid_from TIMESTAMP',
                '    _etl_run_id TEXT'
            ])
            
            columns_sql = ',\n'.join(col_defs)
            
            logger.info(f"[EXTENT] {len(col_defs)} colonnes totales (extent éclaté)")
            
        else:
            # Pas d'extent, DDL normal
            logger.info(f"[BUILD] Table sans extent - DDL classique")
            columns_sql = build_table_columns_sql(cols_meta)
            columns_sql += """,
    _etl_hashdiff TEXT,
    _etl_valid_from TIMESTAMP,
    _etl_run_id TEXT
            """

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


@task(name="[LOAD] RAW → STAGING (SNAPSHOT, EXTENT, HASHDIFF)")
def load_raw_to_staging(table_name: str, run_id: str, load_mode: str = "AUTO"):
    """
    RAW -> STAGING
    - STAGING = snapshot exact de RAW
    - TRUNCATE systématique
    - Hashdiff calculé après éclatement extent
    """
    logger = get_run_logger()
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()

    try:
        # =============================
        # Metadata table
        # =============================
        cur.execute("""
            SELECT "TableName", "ConfigName"
            FROM metadata.etl_tables
            WHERE "ConfigName" = %s OR "TableName" = %s
            LIMIT 1
        """, (table_name, table_name))

        row = cur.fetchone()
        base_table = row[0] if row else table_name
        physical_name = row[1] if row and row[1] else table_name

        raw_table = f"raw.raw_{physical_name.lower()}"
        stg_table = f"staging_etl.stg_{physical_name.lower()}"

        logger.info(f"[DATA] RAW={raw_table} → STAGING={stg_table}")

        # =============================
        # Vérifier RAW
        # =============================
        cur.execute("""
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = 'raw'
                  AND table_name = %s
            )
        """, (f"raw_{physical_name.lower()}",))

        if not cur.fetchone()[0]:
            logger.error(f"[ERROR] Table RAW inexistante : {raw_table}")
            return 0

        # =============================
        # TRUNCATE STAGING (OBLIGATOIRE)
        # =============================
        logger.info(f"[STAGING] TRUNCATE {stg_table}")
        cur.execute(f"TRUNCATE TABLE {stg_table}")
        conn.commit()

        # =============================
        # Colonnes métier
        # =============================
        cols_meta = get_columns_metadata(base_table)
        if not cols_meta:
            logger.error(f"[ERROR] Pas de metadata colonnes pour {base_table}")
            return 0

        # =============================
        # EXTENT
        # =============================
        if has_extent_columns(base_table):
            logger.info("[EXTENT] Éclatement extent")

            cur.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'raw'
                  AND table_name = %s
                ORDER BY ordinal_position
            """, (f"raw_{physical_name.lower()}",))

            raw_cols = [
                r[0] for r in cur.fetchall()
                if r[0] not in ('_loaded_at', '_source_file', '_sftp_log_id')
            ]

            select_clause, expanded_cols, _ = build_ods_select_with_extent_typed(
                base_table,
                raw_cols
            )

            # Hashdiff post-extent
            if len(expanded_cols) <= 95:
                cols_str = ', '.join([f'"{c}"' for c in expanded_cols])
                hash_expr = f"MD5(ROW({cols_str})::TEXT)"
            else:
                chunks = []
                for i in range(0, len(expanded_cols), 95):
                    part = expanded_cols[i:i+95]
                    chunks.append(
                        "CONCAT_WS('|', " +
                        ", ".join([f'COALESCE("{c}"::text, \'\')' for c in part]) +
                        ")"
                    )
                hash_expr = "MD5(" + " || '|' || ".join(chunks) + ")"

            insert_sql = f"""
                INSERT INTO {stg_table} (
                    {', '.join([f'"{c}"' for c in expanded_cols])},
                    _etl_hashdiff,
                    _etl_valid_from,
                    _etl_run_id
                )
                SELECT
                    extent_data.*,
                    {hash_expr} AS _etl_hashdiff,
                    CURRENT_TIMESTAMP AS _etl_valid_from,
                    '{run_id}' AS _etl_run_id
                FROM (
                    SELECT {select_clause}
                    FROM {raw_table}
                ) AS extent_data
            """

        else:
            # =============================
            # Sans EXTENT
            # =============================
            business_cols = list(cols_meta.keys())
            cols_str = ', '.join([f'"{c}"' for c in business_cols])

            if len(business_cols) <= 95:
                hash_expr = f"MD5(ROW({cols_str})::TEXT)"
            else:
                chunks = []
                for i in range(0, len(business_cols), 95):
                    part = business_cols[i:i+95]
                    chunks.append(
                        "CONCAT_WS('|', " +
                        ", ".join([f'COALESCE("{c}"::text, \'\')' for c in part]) +
                        ")"
                    )
                hash_expr = "MD5(" + " || '|' || ".join(chunks) + ")"

            insert_sql = f"""
                INSERT INTO {stg_table} (
                    {cols_str},
                    _etl_hashdiff,
                    _etl_valid_from,
                    _etl_run_id
                )
                SELECT
                    {cols_str},
                    {hash_expr} AS _etl_hashdiff,
                    CURRENT_TIMESTAMP AS _etl_valid_from,
                    '{run_id}' AS _etl_run_id
                FROM {raw_table}
            """

        # =============================
        # INSERT SNAPSHOT
        # =============================
        cur.execute(insert_sql)
        rows = cur.rowcount
        conn.commit()

        logger.info(f"[OK] {rows:,} lignes chargées en STAGING")
        return rows

    except Exception as e:
        conn.rollback()
        logger.error(f"[ERROR] load_raw_to_staging({table_name}): {e}")
        raise

    finally:
        cur.close()
        conn.close()

