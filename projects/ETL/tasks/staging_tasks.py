"""
============================================================================
Staging Tasks - Préparation staging + calcul hashdiff
============================================================================
Fichier : tasks/staging_tasks.py

Tasks pour :
- Copier RAW → STAGING
- Calculer hashdiff SHA256
- Détecter changements vs ODS
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
from utils.hashdiff import calculate_hashdiff_sql, detect_changes


@task(name="📋 Copier RAW → STAGING", retries=1)
def copy_raw_to_staging(table_name: str, run_id: str) -> Dict:
    """
    Copier données de raw.raw_{table} vers staging.{table}
    
    Returns:
        Dict avec rows_copied
    """
    logger = get_run_logger()
    
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    try:
        # 1. Vérifier que staging.{table} existe (sinon créer)
        from utils.ddl_generator import create_table_if_not_exists
        
        # Créer table staging si inexistante (même structure que ODS)
        cur.execute(f"""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_schema = 'staging' 
                AND table_name = '{table_name.lower()}'
            )
        """)
        
        staging_exists = cur.fetchone()[0]
        
        if not staging_exists:
            logger.info(f"📋 Création table staging.{table_name.lower()}")
            # Utiliser même DDL que ODS mais dans staging
            cur.execute(f"""
                CREATE TABLE staging.{table_name.lower()} AS 
                TABLE ods.{table_name.lower()} 
                WITH NO DATA
            """)
            conn.commit()
        
        # 2. TRUNCATE staging (données temporaires)
        logger.info(f"🗑️ TRUNCATE staging.{table_name.lower()}")
        cur.execute(f"TRUNCATE TABLE staging.{table_name.lower()}")
        
        # 3. Copier depuis RAW
        logger.info(f"📥 Copie raw → staging pour {table_name}")
        
        # Récupérer colonnes communes entre raw et staging
        cur.execute(f"""
            SELECT column_name 
            FROM information_schema.columns
            WHERE table_schema = 'raw' 
              AND table_name = 'raw_{table_name.lower()}'
              AND column_name NOT IN ('_loaded_at', '_source_file', '_sftp_log_id')
            ORDER BY ordinal_position
        """)
        
        common_cols = [row[0] for row in cur.fetchall()]
        cols_str = ', '.join([f'"{col}"' for col in common_cols])
        
        cur.execute(f"""
            INSERT INTO staging.{table_name.lower()} ({cols_str}, _etl_run_id, _etl_source)
            SELECT {cols_str}, %s::UUID, 'CBM_DATA01'
            FROM raw.raw_{table_name.lower()}
        """, (run_id,))
        
        rows_copied = cur.rowcount
        conn.commit()
        
        logger.info(f"✅ {rows_copied} lignes copiées dans staging")
        
        return {
            'rows_copied': rows_copied,
            'table_name': table_name,
            'run_id': run_id
        }
        
    except Exception as e:
        conn.rollback()
        logger.error(f"❌ Erreur copie raw → staging : {e}")
        raise
        
    finally:
        cur.close()
        conn.close()


@task(name="🔐 Calculer hashdiff", retries=1)
def calculate_staging_hashdiff(table_name: str, run_id: str) -> Dict:
    """
    Calculer hashdiff SHA256 sur colonnes métier
    
    Returns:
        Dict avec rows_updated
    """
    logger = get_run_logger()
    
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    try:
        # Générer SQL hashdiff
        sql = calculate_hashdiff_sql(table_name, 'staging', run_id)
        
        logger.info(f"🔐 Calcul hashdiff pour staging.{table_name}")
        
        cur.execute(sql)
        conn.commit()
        
        rows_updated = cur.rowcount
        logger.info(f"✅ Hashdiff calculé sur {rows_updated} lignes")
        
        return {
            'rows_updated': rows_updated,
            'table_name': table_name,
            'run_id': run_id
        }
        
    except Exception as e:
        conn.rollback()
        logger.error(f"❌ Erreur calcul hashdiff : {e}")
        raise
        
    finally:
        cur.close()
        conn.close()


@task(name="🔍 Détecter changements vs ODS", retries=1)
def detect_staging_changes(table_name: str, run_id: str) -> Dict:
    """
    Détecter nouveaux / modifiés / inchangés vs ODS
    
    Returns:
        Dict avec counts: new, modified, unchanged
    """
    logger = get_run_logger()
    
    try:
        changes = detect_changes(table_name, run_id)
        
        total = sum(changes.values())
        
        logger.info(f"📊 Changements détectés :")
        logger.info(f"  - Nouveaux    : {changes['new']}")
        logger.info(f"  - Modifiés    : {changes['modified']}")
        logger.info(f"  - Inchangés   : {changes['unchanged']}")
        logger.info(f"  - TOTAL       : {total}")
        
        return {
            'new_rows': changes['new'],
            'modified_rows': changes['modified'],
            'unchanged_rows': changes['unchanged'],
            'total_rows': total,
            'table_name': table_name,
            'run_id': run_id
        }
        
    except Exception as e:
        logger.error(f"❌ Erreur détection changements : {e}")
        raise


@task(name="📋 Préparation staging complète")
def prepare_staging_complete(table_name: str, run_id: str) -> Dict:
    """
    Workflow complet staging :
    1. Copier RAW → STAGING
    2. Calculer hashdiff
    3. Détecter changements
    
    Returns:
        Dict avec résultats complets
    """
    logger = get_run_logger()
    
    logger.info(f"📋 Préparation staging pour {table_name}")
    
    start_time = datetime.now()
    
    # 1. Copier RAW → STAGING
    copy_result = copy_raw_to_staging(table_name, run_id)
    
    # 2. Calculer hashdiff
    hashdiff_result = calculate_staging_hashdiff(table_name, run_id)
    
    # 3. Détecter changements
    changes_result = detect_staging_changes(table_name, run_id)
    
    # 4. Durée totale
    duration = (datetime.now() - start_time).total_seconds()
    
    result = {
        'table_name': table_name,
        'run_id': run_id,
        'rows_copied': copy_result['rows_copied'],
        'hashdiff_calculated': hashdiff_result['rows_updated'],
        'new_rows': changes_result['new_rows'],
        'modified_rows': changes_result['modified_rows'],
        'unchanged_rows': changes_result['unchanged_rows'],
        'duration_seconds': duration
    }
    
    logger.info(f"✅ Staging préparé en {duration:.2f}s")
    
    return result


# ============================================================================
# TEST
# ============================================================================

if __name__ == "__main__":
    print("Test staging tasks...")
    # Test nécessite données en RAW