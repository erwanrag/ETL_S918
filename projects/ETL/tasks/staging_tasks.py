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

sys.path.append(str(Path(__file__).parent.parent))
from flows.config.pg_config import config
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


@task(name="[LOAD] Charger RAW to STAGING avec nettoyage + hashdiff + UPSERT + EXTENT")
def load_raw_to_staging(table_name: str, run_id: str, load_mode: str = "AUTO"):
    """
    Charge RAW -> STAGING avec UPSERT en mode INCREMENTAL
    [FIX FINAL] Hashdiff calculé APRÈS éclatement dans sous-requête
    
    Args:
        table_name: Nom physique de la table
        run_id: ID du run
        load_mode: FULL, INCREMENTAL, ou AUTO
    """
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
        base_table = row[0]
        physical_name = row[1] or row[0]
    else:
        base_table = table_name
        physical_name = table_name

    raw_table = f"raw.raw_{table_name.lower()}"
    stg_table = f"staging_etl.stg_{table_name.lower()}"

    logger.info(f"[DATA] RAW={raw_table}, STAGING={stg_table}, BASE={base_table}")
    logger.info(f"[MODE] Load mode : {load_mode}")

    try:
        # Verifier existence RAW
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

        # Metadonnees colonnes
        cols_meta = get_columns_metadata(base_table)
        business_cols = list(cols_meta.keys())

        if not business_cols:
            logger.error(f"[ERROR] Colonnes non trouvees pour {base_table}")
            return

        # Recuperer PK
        from flows.config.table_metadata import get_primary_keys
        pk_columns = get_primary_keys(base_table)

        # ============================================================
        # [NEW] VÉRIFIER SI EXTENT PRÉSENT
        # ============================================================
        if has_extent_columns(base_table):
            logger.info(f"[EXTENT] Éclatement extent dans SELECT")
            
            # Récupérer colonnes RAW
            cur.execute(f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = 'raw' 
                  AND table_name = 'raw_{table_name.lower()}'
                ORDER BY ordinal_position
            """)
            raw_columns = [row[0] for row in cur.fetchall() 
                          if row[0] not in ['_loaded_at', '_source_file', '_sftp_log_id']]
            # Harmoniser les noms RAW <-> metadata
            raw_columns_normalized = []
            for col in raw_columns:
                upper = col.upper()
                if upper in cols_meta:
                    raw_columns_normalized.append(upper)
                else:
                    raw_columns_normalized.append(col)
            # Utiliser build_ods_select_with_extent_typed pour éclater
            select_clause, expanded_columns, column_types = build_ods_select_with_extent_typed(
                base_table,
                raw_columns_normalized
            )
            
            logger.info(f"[EXTENT] {len(raw_columns)} colonnes RAW → {len(expanded_columns)} colonnes STAGING")
            
            # [CRITICAL FIX] Le hashdiff doit être calculé dans une DOUBLE sous-requête
            # 1. Sous-requête interne : éclatement extent
            # 2. Sous-requête externe : calcul hashdiff sur colonnes déjà éclatées
            
            inner_select = f"""
                SELECT
                    {select_clause}
                    
                FROM {raw_table}
            """
            
            # Construire hashdiff sur les colonnes DÉJÀ éclatées (de la sous-requête interne)
            # [CRITICAL] Ajouter des guillemets pour respecter la casse PostgreSQL
            if len(expanded_columns) <= 95:
                hash_parts = [f"COALESCE(CAST(\"{col}\" AS TEXT), '')" for col in expanded_columns]
                hash_concat = ", ".join(hash_parts)
                hash_calc = f"MD5(CONCAT_WS('|', {hash_concat}))"
            else:
                chunk_size = 95
                chunks = []
                for i in range(0, len(expanded_columns), chunk_size):
                    chunk_cols = expanded_columns[i:i+chunk_size]
                    chunk_concat = ", ".join([f"COALESCE(CAST(\"{col}\" AS TEXT), '')" for col in chunk_cols])
                    chunks.append(f"CONCAT_WS('|', {chunk_concat})")
                all_chunks = " || '|' || ".join(chunks)
                hash_calc = f"MD5({all_chunks})"
            
            # Sous-requête complète avec DOUBLE niveau
            subquery_select = f"""
                SELECT
                    extent_exploded.*,  
                    {hash_calc} AS _etl_hashdiff,
                    CURRENT_TIMESTAMP AS _etl_valid_from,
                    '{run_id}' AS _etl_run_id
                FROM (
                    {inner_select}
                ) AS extent_exploded
            """
            # Colonnes cibles pour INSERT
            target_columns = expanded_columns + ['_etl_hashdiff', '_etl_valid_from', '_etl_run_id']
            
        else:
            # Pas d'extent, SELECT classique avec conversions de type
            logger.info(f"[BUILD] SELECT classique (sans extent)")
            
            select_exprs = []
            for col, info in cols_meta.items():
                pt = (info.get("ProgressType") or "").lower()
                dt = (info.get("DataType") or "").lower()
                extent = info.get("Extent", 0) or 0
                source = f'"{col}"'

                # BOOLEAN
                if pt in ("logical", "bit") and extent == 0:
                    expr = f"""CASE 
    WHEN UPPER(BTRIM({source}::text)) IN ('1','Y','YES','TRUE','OUI') THEN TRUE
    WHEN UPPER(BTRIM({source}::text)) IN ('0','N','NO','FALSE','NON') THEN FALSE
    ELSE NULL
END AS "{col}" """
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
                            WHEN {source} IS NULL THEN NULL
                            WHEN TRIM({source}::text) IN ('', '?', '??', '???', '00000000', '00/00/0000', '00-00-0000', '0000-00-00') THEN NULL
                            WHEN LENGTH(TRIM({source}::text)) = 8 AND {source} ~ '^[0-9]+$'
                                THEN TO_DATE({source}::text, 'YYYYMMDD')
                            WHEN {source} ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
                                THEN {source}::DATE
                            WHEN {source} ~ '^[0-9]{2}/[0-9]{2}/[0-9]{4}$'
                                THEN TO_DATE({source}::text, 'DD/MM/YYYY')
                            ELSE NULL
                        END
                    """

                # TIMESTAMP
                elif pt in ("datetime", "timestamp") or dt == "timestamp":
                    expr = f"NULLIF(BTRIM({source}::text), '')::TIMESTAMP AS \"{col}\""
                # TEXT / fallback
                else:
                    expr = f"NULLIF(BTRIM({source}::text), '') AS \"{col}\""

                select_exprs.append(expr)

            # Hashdiff
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

            select_sql = ",\n                ".join(select_exprs + [
                hash_expr,
                "CURRENT_TIMESTAMP AS _etl_valid_from",
                f"'{run_id}' AS _etl_run_id"
            ])
            
            target_columns = business_cols + ['_etl_hashdiff', '_etl_valid_from', '_etl_run_id']

        # ============================================================
        # STRATEGIE SELON LOAD_MODE
        # ============================================================
        
        if load_mode in ("FULL", "FULL_RESET"):
            # MODE FULL : TRUNCATE + INSERT
            logger.info(f"[FULL] TRUNCATE + INSERT dans {stg_table}")
            
            cur.execute(f"TRUNCATE TABLE {stg_table}")
            conn.commit()
            
            cols_str = ', '.join([f'"{c}"' for c in target_columns])
            
            if has_extent_columns(base_table):
                # Utiliser la sous-requête complète (extent déjà éclaté + hashdiff calculé)
                insert_sql = f"""
                    INSERT INTO {stg_table} ({cols_str})
                    {subquery_select}
                """
            else:
                insert_sql = f"""
                    INSERT INTO {stg_table} ({cols_str})
                    SELECT
                        {select_sql}
                    FROM {raw_table}
                """
            
            cur.execute(insert_sql)
            rows_affected = cur.rowcount
            conn.commit()
            logger.info(f"[OK] {rows_affected:,} lignes inserees (FULL)")
            
        else:
            # MODE INCREMENTAL : UPSERT
            logger.info(f"[INCREMENTAL] UPSERT dans {stg_table}")
            
            if not pk_columns:
                logger.warning(f"[WARN] Pas de PK, fallback sur TRUNCATE + INSERT")
                cur.execute(f"TRUNCATE TABLE {stg_table}")
                conn.commit()
                
                cols_str = ', '.join([f'"{c}"' for c in target_columns])
                
                if has_extent_columns(base_table):
                    # Utiliser la sous-requête complète
                    insert_sql = f"""
                        INSERT INTO {stg_table} ({cols_str})
                        {subquery_select}
                    """
                else:
                    insert_sql = f"""
                        INSERT INTO {stg_table} ({cols_str})
                        SELECT
                            {select_sql}
                        FROM {raw_table}
                    """
                cur.execute(insert_sql)
                rows_affected = cur.rowcount
                conn.commit()
                logger.info(f"[OK] {rows_affected:,} lignes inserees (no PK)")
                
            else:
                # PK join
                pk_join = ' AND '.join([f'target."{pk}" = source."{pk}"' for pk in pk_columns])
                
                cols_str = ', '.join([f'"{c}"' for c in target_columns])
                
                # 1. INSERT nouveaux
                if has_extent_columns(base_table):
                    # [CRITICAL FIX] Ne pas utiliser SELECT *, lister les colonnes avec leur type
                    # Construire le SELECT avec CAST explicite pour chaque colonne
                    
                    # Récupérer les types des colonnes STAGING
                    cur.execute(f"""
                        SELECT column_name, data_type, 
                               character_maximum_length, numeric_precision, numeric_scale
                        FROM information_schema.columns
                        WHERE table_schema = 'staging_etl' 
                          AND table_name = 'stg_{table_name.lower()}'
                        ORDER BY ordinal_position
                    """)
                    staging_col_types = {row[0]: row for row in cur.fetchall()}
                    
                    # Construire SELECT avec CAST pour chaque colonne
                    select_with_cast = []
                    for col in target_columns:
                        if col not in staging_col_types:
                            select_with_cast.append(f'"{col}"')
                            continue
                        
                        col_name, data_type, char_len, num_prec, num_scale = staging_col_types[col]
                        
                        if data_type == 'date':
                            select_with_cast.append(f'"{col}"::DATE')
                        elif data_type == 'timestamp without time zone':
                            select_with_cast.append(f'"{col}"::TIMESTAMP')
                        elif data_type == 'integer':
                            select_with_cast.append(f'"{col}"::INTEGER')
                        elif data_type == 'bigint':
                            select_with_cast.append(f'"{col}"::BIGINT')
                        elif data_type == 'boolean':
                            select_with_cast.append(f'"{col}"::BOOLEAN')
                        elif data_type == 'numeric':
                            select_with_cast.append(f'"{col}"::NUMERIC')
                        elif data_type == 'character varying':
                            select_with_cast.append(f'"{col}"::VARCHAR')
                        else:
                            select_with_cast.append(f'"{col}"')
                    
                    select_str = ', '.join(select_with_cast)
                    
                    insert_sql = f"""
                        INSERT INTO {stg_table} ({cols_str})
                        SELECT {select_str}
                        FROM ({subquery_select}) AS source
                        WHERE NOT EXISTS (
                            SELECT 1 FROM {stg_table} AS target
                            WHERE {pk_join}
                        )
                    """
                else:
                    # pk_join utilise "source_raw" car c'est le nom de la table RAW dans le FROM
                    pk_join_raw = ' AND '.join([f'target."{pk}" = source_raw."{pk}"' for pk in pk_columns])
                    
                    insert_sql = f"""
                        INSERT INTO {stg_table} ({cols_str})
                        SELECT
                            {select_sql}
                        FROM {raw_table} AS source_raw
                        WHERE NOT EXISTS (
                            SELECT 1 FROM {stg_table} AS target
                            WHERE {pk_join_raw}
                        )
                    """
                cur.execute(insert_sql)
                inserted = cur.rowcount
                conn.commit()
                logger.info(f"[ADD] {inserted:,} lignes inserees")
                
                # 2. UPDATE modifies
                update_cols = [c for c in target_columns 
                              if c not in pk_columns 
                              and c not in ['_etl_hashdiff', '_etl_valid_from', '_etl_run_id']]
                
                if update_cols:
                    if has_extent_columns(base_table):
                        # [CRITICAL FIX] SET clause avec CAST explicite pour chaque colonne
                        set_parts = []
                        for col in update_cols:
                            if col not in staging_col_types:
                                set_parts.append(f'"{col}" = source."{col}"')
                                continue
                            
                            col_name, data_type, char_len, num_prec, num_scale = staging_col_types[col]
                            
                            if data_type == 'date':
                                set_parts.append(f'"{col}" = source."{col}"::DATE')
                            elif data_type == 'timestamp without time zone':
                                set_parts.append(f'"{col}" = source."{col}"::TIMESTAMP')
                            elif data_type == 'integer':
                                set_parts.append(f'"{col}" = source."{col}"::INTEGER')
                            elif data_type == 'bigint':
                                set_parts.append(f'"{col}" = source."{col}"::BIGINT')
                            elif data_type == 'boolean':
                                set_parts.append(f'"{col}" = source."{col}"::BOOLEAN')
                            elif data_type == 'numeric':
                                set_parts.append(f'"{col}" = source."{col}"::NUMERIC')
                            elif data_type == 'character varying':
                                set_parts.append(f'"{col}" = source."{col}"::VARCHAR')
                            else:
                                set_parts.append(f'"{col}" = source."{col}"')
                        
                        set_parts.append('"_etl_hashdiff" = source."_etl_hashdiff"')
                        set_parts.append('"_etl_valid_from" = source."_etl_valid_from"::TIMESTAMP')
                        set_parts.append('"_etl_run_id" = source."_etl_run_id"')
                        set_clause = ', '.join(set_parts)
                        
                        update_sql = f"""
                    UPDATE {stg_table} AS target
                    SET {set_clause}
                    FROM ({subquery_select}) AS source
                    WHERE {pk_join}
                      AND target."_etl_hashdiff" != source."_etl_hashdiff"
                        """
                        cur.execute(update_sql)
                        updated = cur.rowcount
                        conn.commit()
                        logger.info(f"[SYNC] {updated:,} lignes mises à jour")
                        rows_affected = inserted + updated
                        logger.info(f"[OK] Total : {rows_affected:,} lignes affectees (UPSERT)")
                    else:
                        set_parts = [f'"{col}" = source."{col}"' for col in update_cols]
                        set_parts.append('"_etl_hashdiff" = source."_etl_hashdiff"')
                        set_parts.append('"_etl_valid_from" = source."_etl_valid_from"')
                        set_parts.append('"_etl_run_id" = source."_etl_run_id"')
                        set_clause = ', '.join(set_parts)
                        
                        # Construire sous-requete source AVEC les conversions (pas d'extent)
                        update_sql = f"""
                    UPDATE {stg_table} AS target
                    SET {set_clause}
                    FROM (
                        SELECT {select_sql}
                        FROM {raw_table}
                    ) AS source
                    WHERE {pk_join}
                      AND target."_etl_hashdiff" != source."_etl_hashdiff"
                        """
                    cur.execute(update_sql)
                    updated = cur.rowcount
                    conn.commit()
                    logger.info(f"[SYNC] {updated:,} lignes mises a jour")
                    
                    rows_affected = inserted + updated
                    logger.info(f"[OK] Total : {rows_affected:,} lignes affectees (UPSERT)")
                else:
                    rows_affected = inserted
                    logger.info(f"[OK] Total : {rows_affected:,} lignes affectees (INSERT only)")
        
        return rows_affected

    except Exception as e:
        conn.rollback()
        logger.error(f"[ERROR] load_raw_to_staging({table_name}): {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise

    finally:
        cur.close()
        conn.close()