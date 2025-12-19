"""
============================================================================
Flow Prefect : RAW → STAGING (OPTIMISe)
============================================================================
"""


import sys
import os
from pathlib import Path
from datetime import datetime

# Imports normaux
from prefect import flow, task
from prefect.logging import get_run_logger
from prefect.task_runners import ConcurrentTaskRunner
from typing import Optional, List
import psycopg2

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from shared.config import config
from tasks.staging_tasks import create_staging_table, load_raw_to_staging


@task(name="[DATA] Lister tables RAW")
def list_raw_tables():
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    cur.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'raw' AND table_name LIKE 'raw_%'
        ORDER BY table_name
    """)
    tables = [row[0].replace("raw_", "") for row in cur.fetchall()]
    cur.close()
    conn.close()
    return tables


@task(name="[CHECK] Verifier RAW")
def check_raw_table_has_data(table_name: str) -> bool:
    logger = get_run_logger()
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    try:
        raw_table = f"raw.raw_{table_name.lower()}"
        
        cur.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'raw' AND table_name = %s
            )
        """, (f"raw_{table_name.lower()}",))
        
        if not cur.fetchone()[0]:
            logger.warning(f"[SKIP] Table inexistante")
            return False
        
        cur.execute(f"SELECT COUNT(*) FROM {raw_table}")
        row_count = cur.fetchone()[0]
        
        if row_count == 0:
            logger.warning(f"[SKIP] Table vide")
            return False
        
        logger.info(f"[OK] {row_count:,} lignes")
        return True
        
    finally:
        cur.close()
        conn.close()


@task(name="[DATA] Get Load mode")
def get_load_mode_for_table(table_name: str) -> str:
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    try:
        cur.execute("""
            SELECT load_mode
            FROM sftp_monitoring.sftp_file_log
            WHERE table_name = %s
              AND processing_status IN ('PENDING', 'COMPLETED')
              AND load_mode IS NOT NULL AND load_mode != ''
            ORDER BY detected_at DESC
            LIMIT 1
        """, (table_name,))
        
        result = cur.fetchone()
        return result[0] if result and result[0] else "AUTO"
            
    finally:
        cur.close()
        conn.close()


@task(name="[STAGING] Process Single Table", retries=2)
def raw_to_staging_single_table(table_name: str, run_id: str) -> dict:
    """Process UNE table RAW → STAGING"""
    logger = get_run_logger()
    
    try:
        conn = psycopg2.connect(config.get_connection_string())
        cur = conn.cursor()
        
        raw_table = f"raw.raw_{table_name.lower()}"
        cur.execute(f"SELECT COUNT(*) FROM {raw_table}")
        row_count = cur.fetchone()[0]
        
        cur.close()
        conn.close()
        
        if row_count == 0:
            logger.warning(f"[SKIP] Vide")
            return {'table': table_name, 'rows': 0, 'status': 'skipped'}
        
        load_mode = get_load_mode_for_table(table_name)
        logger.info(f"[MODE] {load_mode}")
        
        create_staging_table(table_name, load_mode)
        rows_affected = load_raw_to_staging(table_name, run_id, load_mode)
        
        if rows_affected == 0:
            logger.warning(f"[WARN] Aucune donnee chargee")
        else:
            logger.info(f"[OK] {rows_affected:,} lignes")
        
        return {
            'table': table_name,
            'rows': rows_affected or 0,
            'status': 'success',
            'load_mode': load_mode
        }
        
    except Exception as e:
        logger.error(f"[ERROR] {e}")
        return {'table': table_name, 'rows': 0, 'status': 'error', 'error': str(e)}


@flow(
    name="[02] ⚙️ RAW → STAGING (Parallel)",
    task_runner=ConcurrentTaskRunner()
)
def raw_to_staging_flow_parallel(
    table_names: Optional[List[str]] = None,
    run_id: Optional[str] = None
):
    """Version parallelisee par taille de table"""
    logger = get_run_logger()

    if run_id is None:
        from datetime import datetime
        run_id = f"raw_to_staging_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    tables = table_names if table_names else list_raw_tables()
    logger.info(f"[TARGET] {len(tables)} tables")
    
    # Recuperer tailles
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    cur.execute("""
        SELECT table_name, MAX(row_count) as max_rows
        FROM sftp_monitoring.sftp_file_log
        WHERE table_name = ANY(%s)
        GROUP BY table_name
    """, (tables,))
    
    table_sizes = {row[0]: row[1] or 0 for row in cur.fetchall()}
    cur.close()
    conn.close()
    
    # Grouper par taille
    small = []   # < 100k
    medium = []  # 100k - 1M
    large = []   # > 1M
    
    for table in tables:
        size = table_sizes.get(table, 0)
        if size < 100000:
            small.append(table)
        elif size < 1000000:
            medium.append(table)
        else:
            large.append(table)
    
    logger.info(f"[PARALLEL] Small: {len(small)}, Medium: {len(medium)}, Large: {len(large)}")
    
    results = []
    
    # SMALL : Parallèle illimite
    if small:
        logger.info(f"[SMALL] {len(small)} tables en parallèle")
        small_results = raw_to_staging_single_table.map(
            table_name=small,
            run_id=[run_id] * len(small)
        )
        results.extend(small_results)
    
    # MEDIUM : Par chunks de 3
    if medium:
        logger.info(f"[MEDIUM] {len(medium)} tables (chunks de 3)")
        for i in range(0, len(medium), 3):
            chunk = medium[i:i+3]
            medium_results = raw_to_staging_single_table.map(
                table_name=chunk,
                run_id=[run_id] * len(chunk)
            )
            results.extend(medium_results)
    
    # LARGE : Sequentiel
    if large:
        logger.info(f"[LARGE] {len(large)} tables sequentiellement")
        for table in large:
            result = raw_to_staging_single_table(table, run_id)
            results.append(result)
    
    # Resoudre futures
    resolved = []
    for r in results:
        if hasattr(r, 'result'):
            try:
                resolved.append(r.result())
            except:
                continue
        else:
            resolved.append(r)
    
    success = [r for r in resolved if r.get('status') == 'success']
    total_rows = sum(r.get('rows', 0) for r in success)
    
    logger.info(f"✅ {len(success)}/{len(tables)} tables OK, {total_rows:,} lignes")
    # ============================================================
    # ANALYZE STAGING (indispensable pour monitoring)
    # ============================================================
    logger.info("[ANALYZE] Toutes les tables staging_etl")

    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    try:
        # Recuperer toutes les tables staging chargees avec succès
        staging_tables = [r['table'] for r in success if r.get('table')]
        
        for table in staging_tables:
            try:
                cur.execute(f"ANALYZE staging_etl.stg_{table};")
                logger.info(f"[ANALYZE] stg_{table}")
            except Exception as e:
                logger.warning(f"[SKIP ANALYZE] stg_{table}: {e}")
        
        conn.commit()
        logger.info(f"[ANALYZE] {len(staging_tables)} tables analyzed")
    finally:
        cur.close()
        conn.close()
    # ============================================================
    # RETURN (CRITIQUE - MANQUAIT !)
    # ============================================================
    return {
        'tables_processed': len(success),
        'total_rows': total_rows,
        'results': resolved
    }

@flow(name="[02] ⚙️ RAW → STAGING (Sequential)")
def raw_to_staging_flow(
    table_names: Optional[List[str]] = None,
    run_id: Optional[str] = None
):
    """Version sequentielle (legacy)"""
    logger = get_run_logger()

    if run_id is None:
        from datetime import datetime
        run_id = f"raw_to_staging_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    tables = table_names if table_names else list_raw_tables()
    logger.info(f"[TARGET] {len(tables)} tables")

    total_rows = 0
    processed = []
    skipped = []

    for table in tables:
        try:
            if not check_raw_table_has_data(table):
                logger.warning(f"[SKIP] {table}")
                skipped.append(table)
                continue
            
            load_mode = get_load_mode_for_table(table)
            logger.info(f"[MODE] {table} -> {load_mode}")
            
            create_staging_table(table, load_mode=load_mode)
            rows = load_raw_to_staging(table, run_id, load_mode)

            total_rows += (rows or 0)
            processed.append(table)

            if rows and rows > 0:
                logger.info(f"[OK] {table} : {rows:,} lignes ({load_mode})")
            else:
                logger.warning(f"[WARN] {table} : Aucune donnee")
                
        except Exception as e:
            logger.error(f"[ERROR] {table}: {e}")
            skipped.append(table)
            continue

    logger.info(f"✅ {len(processed)} OK, {len(skipped)} skipped, {total_rows:,} lignes")

    return {
        "tables_processed": len(processed),
        "tables_skipped": len(skipped),
        "total_rows": total_rows,
        "tables": processed,
        "skipped_tables": skipped,
        "run_id": run_id
    }


if __name__ == "__main__":
    # Utiliser version parallèle par defaut
    raw_to_staging_flow_parallel()