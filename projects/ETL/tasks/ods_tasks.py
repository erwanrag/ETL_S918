"""
============================================================================
ODS Tasks - Merge ODS avec UPSERT natif PostgreSQL
============================================================================
Fichier : tasks/ods_tasks.py

Tasks pour :
- Merge STAGING → ODS (UPSERT natif)
- Support FULL et INCREMENTAL
- Avec hashdiff pour ne mettre à jour que les changements
============================================================================
"""

from prefect import task
from prefect.logging import get_run_logger
import psycopg2
from datetime import datetime
from typing import Dict
import sys

sys.path.append(r'E:\Prefect\projects\ETL')
from flows.config.pg_config import config
from utils.metadata_helper import get_primary_keys, get_business_columns


@task(name="💾 Merge ODS (FULL)", retries=1)
def merge_ods_full(table_name: str, run_id: str) -> Dict:
    """
    Mode FULL : TRUNCATE + INSERT
    
    Returns:
        Dict avec rows_inserted
    """
    logger = get_run_logger()
    
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    try:
        logger.info(f"💾 Merge ODS FULL pour {table_name}")
        
        # 1. TRUNCATE ODS
        logger.info(f"🗑️ TRUNCATE ods.{table_name.lower()}")
        cur.execute(f"TRUNCATE TABLE ods.{table_name.lower()}")
        
        # 2. INSERT FROM STAGING
        logger.info(f"📥 INSERT depuis staging")
        cur.execute(f"""
            INSERT INTO ods.{table_name.lower()}
            SELECT * FROM staging.{table_name.lower()}
            WHERE _etl_run_id = %s
        """, (run_id,))
        
        rows_inserted = cur.rowcount
        conn.commit()
        
        logger.info(f"✅ {rows_inserted} lignes insérées en mode FULL")
        
        return {
            'rows_inserted': rows_inserted,
            'rows_updated': 0,
            'load_mode': 'FULL',
            'table_name': table_name,
            'run_id': run_id
        }
        
    except Exception as e:
        conn.rollback()
        logger.error(f"❌ Erreur merge FULL : {e}")
        raise
        
    finally:
        cur.close()
        conn.close()


@task(name="💾 Merge ODS (INCREMENTAL)", retries=1)
def merge_ods_incremental(table_name: str, run_id: str) -> Dict:
    """
    Mode INCREMENTAL : INSERT ON CONFLICT UPDATE avec hashdiff
    
    Returns:
        Dict avec rows_inserted et rows_updated
    """
    logger = get_run_logger()
    
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    try:
        logger.info(f"💾 Merge ODS INCREMENTAL pour {table_name}")
        
        # 1. Récupérer PK et colonnes métier
        pk_cols = get_primary_keys(table_name)
        business_cols = get_business_columns(table_name)
        
        if not pk_cols:
            raise ValueError(f"Aucune PK définie pour {table_name}")
        
        # 2. Construire requête UPSERT
        pk_constraint = ', '.join([f'"{pk}"' for pk in pk_cols])
        
        # Colonnes à mettre à jour (hors PK et colonnes techniques)
        update_cols = [col for col in business_cols if col not in pk_cols]
        
        update_set = []
        for col in update_cols:
            update_set.append(f'"{col}" = EXCLUDED."{col}"')
        
        # Colonnes techniques à toujours mettre à jour
        update_set.extend([
            '_etl_updated_at = CURRENT_TIMESTAMP',
            '_etl_run_id = EXCLUDED._etl_run_id',
            '_etl_hashdiff = EXCLUDED._etl_hashdiff'
        ])
        
        update_set_str = ',\n        '.join(update_set)
        
        # 3. UPSERT avec condition hashdiff
        sql = f"""
            INSERT INTO ods.{table_name.lower()}
            SELECT * FROM staging.{table_name.lower()}
            WHERE _etl_run_id = %s
            
            ON CONFLICT ({pk_constraint})
            DO UPDATE SET
                {update_set_str}
            WHERE ods.{table_name.lower()}._etl_hashdiff IS DISTINCT FROM EXCLUDED._etl_hashdiff
        """
        
        logger.info(f"📥 UPSERT depuis staging avec hashdiff filter")
        
        cur.execute(sql, (run_id,))
        rows_affected = cur.rowcount
        conn.commit()
        
        # 4. Compter INSERT vs UPDATE (approximatif)
        # En pratique, on détecte ça depuis staging_tasks.detect_changes
        logger.info(f"✅ {rows_affected} lignes affectées (INSERT + UPDATE)")
        
        return {
            'rows_affected': rows_affected,
            'load_mode': 'INCREMENTAL',
            'table_name': table_name,
            'run_id': run_id
        }
        
    except Exception as e:
        conn.rollback()
        logger.error(f"❌ Erreur merge INCREMENTAL : {e}")
        raise
        
    finally:
        cur.close()
        conn.close()


@task(name="💾 Merge ODS auto (FULL/INCREMENTAL)")
def merge_ods_auto(table_name: str, run_id: str, load_mode: str) -> Dict:
    """
    Merge ODS avec mode automatique
    
    Args:
        table_name: Nom table
        run_id: UUID run
        load_mode: 'FULL' ou 'INCREMENTAL'
    
    Returns:
        Dict avec résultats merge
    """
    logger = get_run_logger()
    
    start_time = datetime.now()
    
    logger.info(f"💾 Merge ODS {load_mode} pour {table_name}")
    
    if load_mode.upper() == 'FULL':
        result = merge_ods_full(table_name, run_id)
    elif load_mode.upper() == 'INCREMENTAL':
        result = merge_ods_incremental(table_name, run_id)
    else:
        raise ValueError(f"Load mode invalide : {load_mode}")
    
    duration = (datetime.now() - start_time).total_seconds()
    result['duration_seconds'] = duration
    
    logger.info(f"✅ Merge ODS terminé en {duration:.2f}s")
    
    return result


@task(name="📊 Vérifier ODS après merge")
def verify_ods_after_merge(table_name: str, run_id: str) -> Dict:
    """
    Vérifier l'état ODS après merge
    
    Returns:
        Dict avec stats ODS
    """
    logger = get_run_logger()
    
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    try:
        # 1. Count total lignes ODS
        cur.execute(f"SELECT COUNT(*) FROM ods.{table_name.lower()}")
        total_rows = cur.fetchone()[0]
        
        # 2. Count lignes de ce run
        cur.execute(f"""
            SELECT COUNT(*) 
            FROM ods.{table_name.lower()}
            WHERE _etl_run_id = %s
        """, (run_id,))
        run_rows = cur.fetchone()[0]
        
        # 3. Dernière mise à jour
        cur.execute(f"""
            SELECT MAX(_etl_updated_at)
            FROM ods.{table_name.lower()}
        """)
        last_updated = cur.fetchone()[0]
        
        logger.info(f"📊 ODS {table_name} : {total_rows} lignes totales, {run_rows} de ce run")
        
        return {
            'table_name': table_name,
            'run_id': run_id,
            'total_rows_ods': total_rows,
            'run_rows': run_rows,
            'last_updated': last_updated
        }
        
    finally:
        cur.close()
        conn.close()


# ============================================================================
# TEST
# ============================================================================

if __name__ == "__main__":
    print("Test ODS tasks...")
    # Test nécessite données en STAGING