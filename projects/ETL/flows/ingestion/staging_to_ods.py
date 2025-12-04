"""
============================================================================
Flow Prefect : STAGING_ETL → ODS
============================================================================
Responsabilité : Merger les données staging vers ODS
- Lecture depuis staging_etl.stg_{table}
- Merge dans ods.{table}
- Support FULL/INCREMENTAL
============================================================================
"""

from prefect import flow, task
from prefect.logging import get_run_logger
from typing import Optional, List
import sys

sys.path.append(r'E:\Prefect\projects\ETL')
from flows.config.pg_config import config
from tasks.ods_tasks import merge_ods_auto, verify_ods_after_merge


@task(name="[DATA] Lister tables STAGING")
def list_staging_tables():
    """Liste toutes les tables staging_etl.stg_*"""
    import psycopg2
    logger = get_run_logger()
    
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    cur.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'staging_etl' 
          AND table_name LIKE 'stg_%'
        ORDER BY table_name
    """)
 
    tables = [row[0].replace('stg_', '') for row in cur.fetchall()]
    
    cur.close()
    conn.close()
    
    logger.info(f"[DATA] {len(tables)} table(s) STAGING trouvée(s)")
    return tables


@flow(name="[SYNC] STAGING → ODS (Merge)")
def staging_to_ods_flow(
    table_names: Optional[List[str]] = None,
    load_mode: str = "AUTO",  # [OK] Mode AUTO
    run_id: Optional[str] = None
):
    """
    Flow de merge STAGING → ODS
    
    Args:
        table_names: Liste des tables à merger (None = toutes)
        load_mode: 'FULL' ou 'INCREMENTAL'
        run_id: ID du run (auto-généré si None)
    
    Étapes :
    1. Lister tables STAGING (si table_names=None)
    2. Pour chaque table : merge vers ODS
    3. Vérification post-merge
    """
    logger = get_run_logger()
    
    if run_id is None:
        from datetime import datetime
        run_id = f"staging_to_ods_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    # Déterminer tables à traiter
    if table_names is None:
        tables = list_staging_tables()
    else:
        tables = table_names
    
    if not tables:
        logger.info("[INFO] Aucune table à traiter")
        return {"tables_merged": 0}
    
    tables_merged = []
    total_rows_affected = 0
    
    for table in tables:
        try:
            logger.info(f"[TARGET] Merge de {table} ({load_mode})")
            
            result = merge_ods_auto(table, run_id, load_mode)
            verify_ods_after_merge(table, run_id)
            
            rows_affected = result.get('rows_affected', result.get('rows_inserted', 0))
            total_rows_affected += rows_affected
            tables_merged.append(table)
            
            logger.info(f"[OK] {table} : {rows_affected:,} lignes affectées")
            
        except Exception as e:
            logger.error(f"[ERROR] Erreur {table} : {e}")
    
    logger.info(f"[TARGET] TERMINÉ : {len(tables_merged)} table(s), {total_rows_affected:,} lignes affectées")
    
    return {
        "tables_merged": len(tables_merged),
        "total_rows_affected": total_rows_affected,
        "tables": tables_merged,
        "run_id": run_id
    }

if __name__ == "__main__":
    staging_to_ods_flow()