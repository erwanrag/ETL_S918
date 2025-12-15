"""
============================================================================
ODS Tasks - STAGING → ODS SIMPLIFIÉ (extent déjà éclaté dans STAGING)
============================================================================
[SIMPLIFIÉ] Plus besoin d'éclater extent (déjà fait dans STAGING)
[OK] INCREMENTAL fonctionne maintenant avec extent !
[OK] Support FULL, INCREMENTAL, FULL_RESET
[FIX] Déduplication STAGING avant INSERT (DISTINCT ON)
============================================================================
"""

import psycopg2
from prefect import task
from prefect.logging import get_run_logger
import sys

sys.path.append(r'E:\Prefect\projects\ETL')
from flows.config.pg_config import config
from flows.config.table_metadata import (
    get_primary_keys, 
    has_primary_key, 
    should_force_full,
    get_table_description
)


def get_load_mode_from_monitoring(table_name: str) -> str:
    """
    Récupérer le load_mode du dernier fichier traité
    
    Returns:
        'INCREMENTAL', 'FULL', 'FULL_RESET', ou 'AUTO'
    """
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    try:
        cur.execute("""
            SELECT load_mode
            FROM sftp_monitoring.sftp_file_log
            WHERE table_name = %s
              AND processing_status IN ('PENDING', 'COMPLETED')
              AND load_mode IS NOT NULL
              AND load_mode != ''
            ORDER BY detected_at DESC
            LIMIT 1
        """, (table_name,))
        
        result = cur.fetchone()
        return result[0] if result and result[0] else "AUTO"
            
    except Exception as e:
        print(f"[WARN] Erreur lecture load_mode pour {table_name}: {e}")
        return "AUTO"
    finally:
        cur.close()
        conn.close()


@task(name="[SAVE] Merge ODS (FULL RESET)", retries=1)
def merge_ods_full_reset(table_name: str, run_id: str):
    """
    Mode FULL RESET : DROP + CREATE + INSERT
    [SIMPLIFIÉ] Copie simple de STAGING (extent déjà éclaté)
    """
    logger = get_run_logger()
    logger.info(f"[SAVE] Merge ODS FULL RESET pour {table_name}")
    
    src_table = f"staging_etl.stg_{table_name.lower()}"
    ods_table = f"ods.{table_name.lower()}"
    
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    try:
        # Compter source
        cur.execute(f"SELECT COUNT(*) FROM {src_table}")
        source_count = cur.fetchone()[0]
        logger.info(f"[DATA] Source : {source_count:,} lignes")
        
        if source_count == 0:
            logger.warning(f"[WARN] Aucune ligne dans {src_table}")
            return {
                "mode": "FULL_RESET",
                "rows_inserted": 0,
                "rows_affected": 0,
                "source_count": 0
            }
        
        # DROP table ODS
        logger.info(f"[DROP] DROP {ods_table} CASCADE")
        cur.execute(f"DROP TABLE IF EXISTS {ods_table} CASCADE")
        conn.commit()
        
        # [SIMPLIFIÉ] Créer ODS comme copie EXACTE de STAGING
        logger.info(f"[CREATE] CREATE {ods_table} (copie structure STAGING)")
        cur.execute(f"""
            CREATE TABLE {ods_table} AS 
            SELECT * FROM {src_table} 
            WHERE FALSE
        """)
        conn.commit()
        logger.info(f"[OK] Table {ods_table} créée")
        
        # INSERT toutes les données
        logger.info(f"[INSERT] INSERT INTO {ods_table}")
        cur.execute(f"INSERT INTO {ods_table} SELECT * FROM {src_table}")
        rows_inserted = cur.rowcount
        conn.commit()
        logger.info(f"[OK] {rows_inserted:,} lignes insérées")
        
        # Ajouter PRIMARY KEY
        pk_columns = get_primary_keys(table_name)
        if pk_columns:
            try:
                pk_cols_str = ', '.join([f'"{col}"' for col in pk_columns])
                logger.info(f"[KEY] Création PRIMARY KEY ({', '.join(pk_columns)})")
                cur.execute(f"""
                    ALTER TABLE {ods_table}
                    ADD PRIMARY KEY ({pk_cols_str})
                """)
                conn.commit()
                logger.info(f"[OK] PRIMARY KEY créée")
            except Exception as e:
                logger.warning(f"[WARN] Impossible de créer PK : {e}")
                conn.rollback()
        
        # Créer index UPSERT (PK + hashdiff)
        if pk_columns:
            pk_list = ', '.join([f'"{col}"' for col in pk_columns])
            try:
                cur.execute(f"""
                    CREATE INDEX IF NOT EXISTS idx_{table_name.lower()}_upsert
                    ON {ods_table} ({pk_list}, _etl_hashdiff)
                """)
                conn.commit()
                logger.info(f"[INDEX] Index UPSERT créé")
            except Exception as e:
                logger.warning(f"[WARN] Index UPSERT : {e}")
                conn.rollback()

        # Créer index sur _etl_valid_from
        try:
            cur.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_{table_name.lower()}_etl_valid_from
                ON {ods_table} (_etl_valid_from)
            """)
            conn.commit()
            logger.info(f"[INDEX] Index créé sur _etl_valid_from")
        except Exception as e:
            logger.warning(f"[WARN] Impossible de créer INDEX : {e}")
            conn.rollback()
        
        logger.info(f"[OK] FULL RESET terminé : {rows_inserted:,} lignes insérées")
        
        return {
            "mode": "FULL_RESET",
            "rows_inserted": rows_inserted,
            "rows_affected": rows_inserted,
            "source_count": source_count
        }
        
    except Exception as e:
        logger.error(f"[ERROR] Erreur FULL RESET : {e}")
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


@task(name="[SAVE] Merge ODS (FULL)", retries=1)
def merge_ods_full(table_name: str, run_id: str):
    """
    Mode FULL : TRUNCATE + INSERT (sans DROP)
    [SIMPLIFIÉ] Si table n'existe pas → fallback FULL_RESET
    """
    logger = get_run_logger()
    logger.info(f"[SAVE] Merge ODS FULL pour {table_name}")
    
    src_table = f"staging_etl.stg_{table_name.lower()}"
    ods_table = f"ods.{table_name.lower()}"
    
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    try:
        # Compter source
        cur.execute(f"SELECT COUNT(*) FROM {src_table}")
        src_count = cur.fetchone()[0]
        logger.info(f"[DATA] Source : {src_count:,} lignes")
        
        # Vérifier si ODS existe
        cur.execute(f"""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_schema = 'ods' 
                AND table_name = '{table_name.lower()}'
            )
        """)
        
        ods_exists = cur.fetchone()[0]
        
        # Si table n'existe pas, faire FULL_RESET
        if not ods_exists:
            logger.info(f"[NEW] Table n'existe pas → FULL_RESET")
            cur.close()
            conn.close()
            return merge_ods_full_reset(table_name, run_id)
        
        # TRUNCATE + INSERT
        logger.info(f"[TRUNCATE] TRUNCATE {ods_table}")
        cur.execute(f"TRUNCATE TABLE {ods_table}")
        conn.commit()
        
        logger.info(f"[INSERT] INSERT INTO {ods_table}")
        
        # Récupérer colonnes communes entre STAGING et ODS
        cur.execute(f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = 'ods' AND table_name = '{table_name.lower()}'
            ORDER BY ordinal_position
        """)
        ods_cols = [row[0] for row in cur.fetchall()]

        cur.execute(f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = 'staging_etl' AND table_name = 'stg_{table_name.lower()}'
            ORDER BY ordinal_position
        """)
        stg_cols = [row[0] for row in cur.fetchall()]

        # Colonnes communes
        common_cols = [c for c in stg_cols if c in ods_cols]
        cols_str = ', '.join(f'"{c}"' for c in common_cols)

        cur.execute(f"INSERT INTO {ods_table} ({cols_str}) SELECT {cols_str} FROM {src_table}")
        inserted_count = cur.rowcount
        conn.commit()
        
        logger.info(f"[OK] {inserted_count:,} lignes insérées")
        
        return {
            'rows_inserted': inserted_count,
            'rows_affected': inserted_count,
            'mode': 'FULL',
            'table': ods_table,
            'run_id': run_id
        }
        
    except Exception as e:
        logger.error(f"[ERROR] Erreur : {e}")
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


@task(name="[SAVE] Merge ODS (INCREMENTAL)", retries=1)
def merge_ods_incremental(table_name: str, run_id: str):
    """
    Mode INCREMENTAL : UPSERT (INSERT nouveaux + UPDATE modifiés)
    [SIMPLIFIÉ] Plus de problème avec extent (déjà éclaté dans STAGING) ✅
    [FIX] Déduplication STAGING avant INSERT avec DISTINCT ON
    """
    logger = get_run_logger()
    logger.info(f"[SAVE] Merge ODS INCREMENTAL pour {table_name}")
    
    pk_columns = get_primary_keys(table_name)
    
    if not pk_columns:
        raise ValueError(f"[ERROR] Aucune PK pour {table_name}")
    
    logger.info(f"[KEY] PK : {', '.join(pk_columns)}")
    
    src_table = f"staging_etl.stg_{table_name.lower()}"
    ods_table = f"ods.{table_name.lower()}"
    
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    try:
        # Vérifier si ODS existe
        cur.execute(f"""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_schema = 'ods' 
                AND table_name = '{table_name.lower()}'
            )
        """)
        
        ods_exists = cur.fetchone()[0]
        
        if not ods_exists:
            logger.info(f"[NEW] Table n'existe pas → FULL_RESET")
            cur.close()
            conn.close()
            return merge_ods_full_reset(table_name, run_id)
        
        # Récupérer colonnes staging
        cur.execute(f"""
            SELECT column_name FROM information_schema.columns 
            WHERE table_schema = 'staging_etl' 
              AND table_name = 'stg_{table_name.lower()}'
              AND column_name NOT IN ('_etl_run_id')
            ORDER BY ordinal_position
        """)
        
        staging_columns = [row[0] for row in cur.fetchall()]
        columns = [col for col in staging_columns if col != '_etl_run_id']
        columns_str = ', '.join([f'"{col}"' for col in columns])
        
        pk_join = ' AND '.join([f'target."{pk}" = source."{pk}"' for pk in pk_columns])
        
        # INSERT nouveaux (avec déduplication STAGING via DISTINCT ON)
        logger.info("[UPSERT] INSERT nouveaux (avec déduplication)")
        
        # Construire DISTINCT ON avec toutes les PK
        pk_cols_str = ', '.join([f'"{pk}"' for pk in pk_columns])
        
        # Vérifier si _etl_valid_from existe dans les colonnes
        has_etl_valid_from = '_etl_valid_from' in columns
        order_by_clause = f"{pk_cols_str}, \"_etl_valid_from\" DESC" if has_etl_valid_from else pk_cols_str
        
        cur.execute(f"""
            INSERT INTO {ods_table} ({columns_str})
            SELECT DISTINCT ON ({pk_cols_str}) {columns_str} 
            FROM {src_table} AS source
            WHERE NOT EXISTS (
                SELECT 1 FROM {ods_table} AS target
                WHERE {pk_join}
            )
            ORDER BY {order_by_clause}
        """)
        inserted_count = cur.rowcount
        conn.commit()
        logger.info(f"[ADD] {inserted_count:,} lignes insérées")
        
        # UPDATE modifiés (si hashdiff présent)
        if '_etl_hashdiff' in columns:
            update_cols = [
                col for col in columns 
                if col not in pk_columns 
                and col not in ['_etl_hashdiff', '_etl_valid_from']
            ]
            
            if update_cols:
                set_clause = ', '.join([f'"{col}" = source."{col}"' for col in update_cols])
                full_set = f'{set_clause}, "_etl_hashdiff" = source."_etl_hashdiff", "_etl_valid_from" = source."_etl_valid_from"'
            else:
                full_set = '"_etl_hashdiff" = source."_etl_hashdiff", "_etl_valid_from" = source."_etl_valid_from"'
            
            # UPDATE avec déduplication via subquery
            logger.info("[UPSERT] UPDATE modifiés (avec déduplication)")
            cur.execute(f"""
                UPDATE {ods_table} AS target
                SET {full_set}
                FROM (
                    SELECT DISTINCT ON ({pk_cols_str}) {columns_str}
                    FROM {src_table}
                    ORDER BY {order_by_clause}
                ) AS source
                WHERE {pk_join}
                  AND target."_etl_hashdiff" != source."_etl_hashdiff"
            """)
            updated_count = cur.rowcount
            conn.commit()
            logger.info(f"[SYNC] {updated_count:,} lignes mises à jour")
        else:
            updated_count = 0
        
        total = inserted_count + updated_count
        logger.info(f"[OK] {total:,} lignes affectées")
        
        return {
            'rows_inserted': inserted_count,
            'rows_updated': updated_count,
            'rows_affected': total,
            'mode': 'INCREMENTAL',
            'table': ods_table,
            'run_id': run_id
        }
        
    except Exception as e:
        logger.error(f"[ERROR] Erreur : {e}")
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


@task(name="[SAVE] Merge ODS auto")
def merge_ods_auto(table_name: str, run_id: str, load_mode: str = "AUTO"):
    """
    [TARGET] ROUTER : Merger STAGING → ODS avec mode automatique ou forcé
    [SIMPLIFIÉ] Plus de fallback extent !
    
    Priorité de détermination du mode :
    1. load_mode passé en paramètre (si != "AUTO")
    2. load_mode depuis sftp_monitoring (dernier fichier)
    3. Détection automatique (PK + timestamps)
    
    Modes disponibles:
        - INCREMENTAL: UPSERT (INSERT nouveaux + UPDATE modifiés)
        - FULL: TRUNCATE + INSERT ALL
        - FULL_RESET: DROP + CREATE + INSERT ALL
        - AUTO: Détection automatique
    """
    logger = get_run_logger()
    
    # Si mode AUTO, lire depuis sftp_monitoring
    if load_mode == "AUTO":
        load_mode = get_load_mode_from_monitoring(table_name)
        logger.info(f"[AUTO] Mode depuis sftp_monitoring : {load_mode}")
    else:
        logger.info(f"[TARGET] Mode forcé : {load_mode}")
    
    # Description table
    table_desc = get_table_description(table_name)
    if table_desc:
        logger.info(f"[LIST] {table_desc}")
    
    # ============================================================
    # ROUTER selon load_mode
    # ============================================================
    
    if load_mode == "FULL_RESET":
        logger.info(f"[SAVE] Mode FULL_RESET - Réinitialisation complète")
        return merge_ods_full_reset(table_name, run_id)
        
    elif load_mode == "FULL":
        logger.info(f"[SAVE] Mode FULL - TRUNCATE + INSERT")
        return merge_ods_full(table_name, run_id)
        
    elif load_mode == "INCREMENTAL":
        pk = get_primary_keys(table_name)
        if not pk:
            logger.warning(f"[WARN] Mode INCREMENTAL demandé mais pas de PK → Fallback sur FULL")
            return merge_ods_full(table_name, run_id)
        else:
            logger.info(f"[SAVE] Mode INCREMENTAL - UPSERT (PK: {pk})")
            return merge_ods_incremental(table_name, run_id)
    
    else:  # AUTO ou mode inconnu
        logger.info(f"[AUTO] Détection automatique (mode={load_mode})")
        
        has_pk = has_primary_key(table_name)
        force_full = should_force_full(table_name)
        
        if has_pk and not force_full:
            pk = get_primary_keys(table_name)
            logger.info(f"[AUTO] AUTO → INCREMENTAL (PK: {pk})")
            return merge_ods_incremental(table_name, run_id)
        else:
            logger.info(f"[AUTO] AUTO → FULL")
            return merge_ods_full(table_name, run_id)


@task(name="[OK] Vérifier ODS")
def verify_ods_after_merge(table_name: str, run_id: str):
    """Vérification post-merge"""
    logger = get_run_logger()
    
    ods_table = f"ods.{table_name.lower()}"
    
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    try:
        cur.execute(f"SELECT COUNT(*) FROM {ods_table}")
        total = cur.fetchone()[0]
        
        logger.info(f"[OK] ODS : {total:,} lignes")
        
        return {'exists': True, 'total_rows': total}
        
    finally:
        cur.close()
        conn.close()