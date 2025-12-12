"""
============================================================================
Flow Prefect : STAGING_ETL -> ODS
============================================================================
Responsabilite : Merger les donnees staging vers ODS
- Lecture depuis staging_etl.stg_{table}
- Merge dans ods.{table}
- Support FULL/INCREMENTAL
============================================================================
"""

from prefect import flow, task
from prefect.logging import get_run_logger
from typing import Optional, List
import sys
import psycopg2

sys.path.append(r'E:\Prefect\projects\ETL')
from flows.config.pg_config import config
from tasks.ods_tasks import merge_ods_auto, verify_ods_after_merge




@task(name="[DATA] List STAGING Tables")
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
    
    logger.info(f"[DATA] {len(tables)} table(s) STAGING trouvee(s)")
    return tables


@task(name="[CHECK] Check Data")
def check_staging_table_has_data(table_name: str) -> bool:
    """
    Verifie si la table STAGING contient des donnees
    
    Returns:
        True si la table a au moins 1 ligne, False sinon
    """
    import psycopg2
    logger = get_run_logger()
    
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    try:
        staging_table = f"staging_etl.stg_{table_name.lower()}"
        
        # Verifier existence table
        cur.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'staging_etl'
                  AND table_name = %s
            )
        """, (f"stg_{table_name.lower()}",))
        
        table_exists = cur.fetchone()[0]
        
        if not table_exists:
            logger.warning(f"[WARN] Table {staging_table} n'existe pas")
            return False
        
        # Compter lignes
        cur.execute(f"SELECT COUNT(*) FROM {staging_table}")
        row_count = cur.fetchone()[0]
        
        if row_count == 0:
            logger.warning(f"[SKIP] Table {staging_table} est vide (0 lignes)")
            return False
        
        logger.info(f"[OK] {staging_table} : {row_count:,} lignes detectees")
        return True
        
    except Exception as e:
        logger.error(f"[ERROR] Erreur verification {table_name}: {e}")
        return False
    finally:
        cur.close()
        conn.close()

@task(name="[ODS] Merge Single Table", retries=1)
def staging_to_ods_single_table(table_name: str, run_id: str, load_mode: str = "AUTO") -> dict:
    """
    Merge UNE SEULE table STAGING → ODS
    Utilisé pour parallélisation avec .map()
    
    Returns:
        {
            'table': str,
            'rows': int,
            'status': 'success' | 'error' | 'skipped',
            'error': str (optionnel)
        }
    """
    logger = get_run_logger()
    
    try:
        # Vérifier si STAGING a des données
        conn = psycopg2.connect(config.get_connection_string())
        cur = conn.cursor()
        
        stg_table = f"staging_etl.stg_{table_name.lower()}"
        cur.execute(f"SELECT COUNT(*) FROM {stg_table}")
        row_count = cur.fetchone()[0]
        
        cur.close()
        conn.close()
        
        if row_count == 0:
            logger.warning(f"[SKIP] {table_name} : Aucune donnée dans STAGING")
            return {
                'table': table_name,
                'rows': 0,
                'status': 'skipped'
            }
        
        logger.info(f"[TARGET] Merge de {table_name} ({load_mode})")
        
        # Merger
        result = merge_ods_auto(table_name, run_id, load_mode)
        
        # Vérifier
        verify_result = verify_ods_after_merge(table_name, run_id)
        
        rows_affected = result.get('rows_affected', 0)
        logger.info(f"[OK] {table_name} : {rows_affected:,} lignes affectées")
        
        return {
            'table': table_name,
            'rows': rows_affected,
            'status': 'success',
            'mode': result.get('mode', load_mode)
        }
        
    except Exception as e:
        logger.error(f"[ERROR] {table_name} : {e}")
        import traceback
        logger.error(traceback.format_exc())
        
        return {
            'table': table_name,
            'rows': 0,
            'status': 'error',
            'error': str(e)
        }


@flow(name="[03] 💾 STAGING to ODS (Merge)")
def staging_to_ods_flow(
    table_names: Optional[List[str]] = None,
    load_mode: str = "AUTO",
    run_id: Optional[str] = None
):
    """
    Flow de merge STAGING to ODS
    
    Args:
        table_names: Liste des tables a merger (None = toutes)
        load_mode: 'FULL' ou 'INCREMENTAL' ou 'AUTO'
        run_id: ID du run (auto-genere si None)
    
    Etapes :
    1. Lister tables STAGING (si table_names=None)
    2. Pour chaque table : merge vers ODS
    3. Verification post-merge
    """
    logger = get_run_logger()
    
    if run_id is None:
        from datetime import datetime
        run_id = f"staging_to_ods_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    # Determiner tables a traiter
    if table_names is None:
        tables = list_staging_tables()
    else:
        tables = table_names
    
    if not tables:
        logger.info("[INFO] Aucune table a traiter")
        return {"tables_merged": 0}
    
    tables_merged = []
    tables_skipped = []
    total_rows_affected = 0
    
    for table in tables:
        try:
            # Verifier si table STAGING a des donnees
            if not check_staging_table_has_data(table):
                logger.warning(f"[SKIP] {table} - Table STAGING vide ou inexistante")
                tables_skipped.append(table)
                continue  # Passer a la table suivante
            
            logger.info(f"[TARGET] Merge de {table} ({load_mode})")
            
            result = merge_ods_auto(table, run_id, load_mode)
            verify_ods_after_merge(table, run_id)
            
            rows_affected = result.get('rows_affected', result.get('rows_inserted', 0))
            total_rows_affected += rows_affected
            tables_merged.append(table)
            
            logger.info(f"[OK] {table} : {rows_affected:,} lignes affectees")
            
        except Exception as e:
            logger.error(f"[ERROR] Erreur {table} : {e}")
            tables_skipped.append(table)
            continue  # Ne pas bloquer le pipeline
    
    # Log recapitulatif
    logger.info("=" * 70)
    logger.info(f"[OK] Traitement termine")
    logger.info(f"   [OK] Mergees : {len(tables_merged)} table(s)")
    logger.info(f"   [SKIP] Skipped : {len(tables_skipped)} table(s)")
    if tables_skipped:
        logger.info(f"   [LIST] Tables skipped : {', '.join(tables_skipped)}")
    logger.info(f"   [DATA] Total lignes : {total_rows_affected:,}")
    logger.info("=" * 70)
    
    return {
        "tables_merged": len(tables_merged),
        "tables_skipped": len(tables_skipped),
        "total_rows_affected": total_rows_affected,
        "tables": tables_merged,
        "skipped_tables": tables_skipped,
        "run_id": run_id
    }


if __name__ == "__main__":
    staging_to_ods_flow()