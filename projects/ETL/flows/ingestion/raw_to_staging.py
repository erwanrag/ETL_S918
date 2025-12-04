"""
============================================================================
Flow Prefect : RAW → STAGING_ETL
============================================================================
Responsabilité :
- Créer STAGING typé depuis metadata
- Charger RAW → STAGING avec nettoyage + hashdiff + UPSERT
============================================================================
"""

from prefect import flow, task
from prefect.logging import get_run_logger
from typing import Optional, List
import sys
import psycopg2

sys.path.append(r"E:\Prefect\projects\ETL")

from flows.config.pg_config import config
from tasks.staging_tasks import create_staging_table, load_raw_to_staging


@task(name="[DATA] Lister tables RAW")
def list_raw_tables():
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()

    cur.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'raw'
          AND table_name LIKE 'raw_%'
        ORDER BY table_name
    """)

    tables = [row[0].replace("raw_", "") for row in cur.fetchall()]

    cur.close()
    conn.close()
    return tables


@task(name="[DATA] Récupérer load_mode depuis sftp_monitoring")
def get_load_mode_for_table(table_name: str) -> str:
    """
    Récupère le load_mode du dernier fichier traité pour une table
    
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


@flow(name="[LIST] RAW → STAGING_ETL (typé + nettoyage + hashdiff + UPSERT)")
def raw_to_staging_flow(
    table_names: Optional[List[str]] = None,
    run_id: Optional[str] = None
):

    logger = get_run_logger()

    if run_id is None:
        from datetime import datetime
        run_id = f"raw_to_staging_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    # Tables RAW à traiter
    tables = table_names if table_names else list_raw_tables()

    logger.info(f"[TARGET] {len(tables)} table(s) à traiter")

    total_rows = 0
    processed = []

    for table in tables:
        # [NEW] Récupérer load_mode depuis sftp_monitoring
        load_mode = get_load_mode_for_table(table)
        logger.info(f"[MODE] {table} → load_mode: {load_mode}")
        
        # Création STAGING (DROP + CREATE si FULL/FULL_RESET)
        logger.info(f"[CONFIG] Création STAGING {table}")
        create_staging_table(table)

        # Chargement avec UPSERT si INCREMENTAL
        logger.info(f"📥 Chargement RAW → STAGING {table}")
        rows = load_raw_to_staging(
            table_name=table,
            run_id=run_id,
            load_mode=load_mode  
        )

        total_rows += (rows or 0)
        processed.append(table)

        if rows is not None:
            logger.info(f"[OK] {table} : {rows:,} lignes ({load_mode})")
        else:
            logger.warning(f"[SKIP] {table} : Aucune donnée chargée")

    return {
        "tables_processed": len(processed),
        "total_rows": total_rows,
        "tables": processed,
        "run_id": run_id
    }

if __name__ == "__main__":
    raw_to_staging_flow()