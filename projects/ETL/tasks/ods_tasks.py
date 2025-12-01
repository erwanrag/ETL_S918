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
    get_optimal_load_mode,
    get_table_description
)


@task(name="💾 Merge ODS (FULL)", retries=1)
def merge_ods_full(table_name: str, run_id: str):
    logger = get_run_logger()
    logger.info(f"💾 Merge ODS FULL pour {table_name}")
    
    src_table = f"staging_etl.stg_{table_name.lower()}"
    ods_table = f"ods.{table_name.lower()}"
    
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    try:
        cur.execute(f"SELECT COUNT(*) FROM {src_table}")
        src_count = cur.fetchone()[0]
        logger.info(f"📊 Source : {src_count:,} lignes")
        
        cur.execute(f"""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_schema = 'ods' 
                AND table_name = '{table_name.lower()}'
            )
        """)
        
        if not cur.fetchone()[0]:
            logger.info(f"🆕 Création de {ods_table}")
            cur.execute(f"CREATE TABLE {ods_table} AS SELECT * FROM {src_table} WHERE 1=0")
            conn.commit()
        
        logger.info(f"🧨 TRUNCATE {ods_table}")
        cur.execute(f"TRUNCATE TABLE {ods_table}")
        conn.commit()
        
        cur.execute(f"""
            SELECT column_name FROM information_schema.columns 
            WHERE table_schema = 'staging_etl' 
              AND table_name = 'stg_{table_name.lower()}'
              AND column_name NOT IN ('_etl_run_id')
            ORDER BY ordinal_position
        """)
        
        columns = [row[0] for row in cur.fetchall()]
        columns_str = ', '.join([f'"{col}"' for col in columns])
        
        cur.execute(f"INSERT INTO {ods_table} ({columns_str}) SELECT {columns_str} FROM {src_table}")
        conn.commit()
        
        cur.execute(f"SELECT COUNT(*) FROM {ods_table}")
        inserted_count = cur.fetchone()[0]
        
        logger.info(f"✅ {inserted_count:,} lignes insérées")
        
        return {
            'rows_inserted': inserted_count,
            'mode': 'FULL',
            'table': ods_table,
            'run_id': run_id
        }
        
    except Exception as e:
        logger.error(f"❌ Erreur : {e}")
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


@task(name="💾 Merge ODS (INCREMENTAL)", retries=1)
def merge_ods_incremental(table_name: str, run_id: str):
    logger = get_run_logger()
    logger.info(f"💾 Merge ODS INCREMENTAL pour {table_name}")
    
    pk_columns = get_primary_keys(table_name)
    
    if not pk_columns:
        raise ValueError(f"Aucune PK pour {table_name}")
    
    logger.info(f"🔑 PK : {', '.join(pk_columns)}")
    
    src_table = f"staging_etl.stg_{table_name.lower()}"
    ods_table = f"ods.{table_name.lower()}"
    
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    try:
        cur.execute(f"""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_schema = 'ods' 
                AND table_name = '{table_name.lower()}'
            )
        """)
        
        if not cur.fetchone()[0]:
            logger.info(f"🆕 Création de {ods_table}")
            cur.execute(f"CREATE TABLE {ods_table} AS SELECT * FROM {src_table} WHERE 1=0")
            conn.commit()
        
        cur.execute(f"""
            SELECT column_name FROM information_schema.columns 
            WHERE table_schema = 'staging_etl' 
              AND table_name = 'stg_{table_name.lower()}'
              AND column_name NOT IN ('_etl_run_id')
            ORDER BY ordinal_position
        """)
        
        columns = [row[0] for row in cur.fetchall()]
        columns_str = ', '.join([f'"{col}"' for col in columns])
        
        pk_join = ' AND '.join([f'target."{pk}" = source."{pk}"' for pk in pk_columns])
        
        # ✅ CORRECTION : Exclure AUSSI _etl_valid_from du set_clause
        update_cols = [
            col for col in columns 
            if col not in pk_columns 
            and col not in ['_etl_hashdiff', '_etl_valid_from']
        ]
        set_clause = ', '.join([f'"{col}" = source."{col}"' for col in update_cols])
        
        logger.info("🔄 INSERT nouveaux")
        cur.execute(f"""
            INSERT INTO {ods_table} ({columns_str})
            SELECT {columns_str} FROM {src_table} AS source
            WHERE NOT EXISTS (
                SELECT 1 FROM {ods_table} AS target
                WHERE {pk_join}
            )
        """)
        inserted_count = cur.rowcount
        conn.commit()
        logger.info(f"➕ {inserted_count:,} lignes insérées")
        
        if '_etl_hashdiff' in columns:
            # Ajouter _etl_hashdiff et _etl_valid_from explicitement APRÈS le set_clause
            if set_clause:  # S'il y a d'autres colonnes à updater
                full_set = f'{set_clause}, "_etl_hashdiff" = source."_etl_hashdiff", "_etl_valid_from" = source."_etl_valid_from"'
            else:  # Sinon juste hashdiff et valid_from
                full_set = '"_etl_hashdiff" = source."_etl_hashdiff", "_etl_valid_from" = source."_etl_valid_from"'
            
            cur.execute(f"""
                UPDATE {ods_table} AS target
                SET {full_set}
                FROM {src_table} AS source
                WHERE {pk_join}
                  AND target."_etl_hashdiff" != source."_etl_hashdiff"
            """)
            updated_count = cur.rowcount
            conn.commit()
            logger.info(f"🔄 {updated_count:,} lignes mises à jour")
        else:
            updated_count = 0
        
        total = inserted_count + updated_count
        logger.info(f"✅ {total:,} lignes affectées")
        
        return {
            'rows_inserted': inserted_count,
            'rows_updated': updated_count,
            'rows_affected': total,
            'mode': 'INCREMENTAL',
            'table': ods_table,
            'run_id': run_id
        }
        
    except Exception as e:
        logger.error(f"❌ Erreur : {e}")
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

@task(name="💾 Merge ODS auto")
def merge_ods_auto(table_name: str, run_id: str, load_mode: str = "AUTO"):
    logger = get_run_logger()
    
    desc = get_table_description(table_name)
    if desc:
        logger.info(f"📋 {desc}")
    
    if load_mode == "AUTO":
        load_mode = get_optimal_load_mode(table_name)
        
        if should_force_full(table_name):
            logger.info("🤖 AUTO → FULL")
        elif has_primary_key(table_name):
            pks = get_primary_keys(table_name)
            logger.info(f"🤖 AUTO → INCREMENTAL (PK: {', '.join(pks)})")
    
    logger.info(f"💾 Mode {load_mode}")
    
    if load_mode == "FULL":
        return merge_ods_full(table_name, run_id)
    elif load_mode == "INCREMENTAL":
        return merge_ods_incremental(table_name, run_id)
    else:
        raise ValueError(f"Mode invalide : {load_mode}")


@task(name="✅ Vérifier ODS")
def verify_ods_after_merge(table_name: str, run_id: str):
    logger = get_run_logger()
    
    ods_table = f"ods.{table_name.lower()}"
    
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    try:
        cur.execute(f"SELECT COUNT(*) FROM {ods_table}")
        total = cur.fetchone()[0]
        
        logger.info(f"✅ ODS : {total:,} lignes")
        
        return {'exists': True, 'total_rows': total}
        
    finally:
        cur.close()
        conn.close()