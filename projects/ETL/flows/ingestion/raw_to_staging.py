"""
============================================================================
Flow Prefect : RAW → STAGING_ETL
============================================================================
Responsabilité :
- Créer STAGING typé depuis metadata
- Charger RAW → STAGING avec nettoyage + hashdiff
============================================================================
"""

from prefect import flow, task
from prefect.logging import get_run_logger
from typing import Optional, List
import sys

sys.path.append(r"E:\Prefect\projects\ETL")

from flows.config.pg_config import config
from tasks.staging_tasks import create_staging_table, load_raw_to_staging


@task(name="📊 Lister tables RAW")
def list_raw_tables():
    import psycopg2
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


@flow(name="📋 RAW → STAGING_ETL (typé + nettoyage + hashdiff)")
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

    logger.info(f"🎯 {len(tables)} table(s) à traiter")

    total_rows = 0
    processed = []

    for table in tables:
        logger.info(f"🔧 Création STAGING {table}")
        create_staging_table(table)  # <-- NOUVEAU

        logger.info(f"📥 Chargement RAW → STAGING {table}")
        rows = load_raw_to_staging(table, run_id)  # <-- NOUVEAU

        total_rows += (rows or 0)
        processed.append(table)

        logger.info(f"✅ {table} : {rows:,} lignes")

    return {
        "tables_processed": len(processed),
        "total_rows": total_rows,
        "tables": processed,
        "run_id": run_id
    }


if __name__ == "__main__":
    raw_to_staging_flow()
