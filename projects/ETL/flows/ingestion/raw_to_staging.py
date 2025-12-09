"""
============================================================================
Flow Prefect : RAW -> STAGING_ETL
============================================================================
Responsabilite :
- Creer STAGING type depuis metadata
- Charger RAW -> STAGING avec nettoyage + hashdiff + UPSERT
- Protection contre tables RAW vides
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


@task(name="[CHECK] Verifier si table RAW a des donnees")
def check_raw_table_has_data(table_name: str) -> bool:
    """
    Verifie si la table RAW contient des donnees
    
    Returns:
        True si la table a au moins 1 ligne, False sinon
    """
    logger = get_run_logger()
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    try:
        raw_table = f"raw.raw_{table_name.lower()}"
        
        # Verifier existence table
        cur.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'raw'
                  AND table_name = %s
            )
        """, (f"raw_{table_name.lower()}",))
        
        table_exists = cur.fetchone()[0]
        
        if not table_exists:
            logger.warning(f"[WARN] Table {raw_table} n'existe pas")
            return False
        
        # Compter lignes
        cur.execute(f"SELECT COUNT(*) FROM {raw_table}")
        row_count = cur.fetchone()[0]
        
        if row_count == 0:
            logger.warning(f"[SKIP] Table {raw_table} est vide (0 lignes)")
            return False
        
        logger.info(f"[OK] {raw_table} : {row_count:,} lignes detectees")
        return True
        
    except Exception as e:
        logger.error(f"[ERROR] Erreur verification {table_name}: {e}")
        return False
    finally:
        cur.close()
        conn.close()


@task(name="[DATA] Recuperer load_mode depuis sftp_monitoring")
def get_load_mode_for_table(table_name: str) -> str:
    """
    Recupere le load_mode du dernier fichier traite pour une table
    
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


@flow(name="[LIST] RAW to STAGING_ETL (type + nettoyage + hashdiff + UPSERT)")
def raw_to_staging_flow(
    table_names: Optional[List[str]] = None,
    run_id: Optional[str] = None
):
    logger = get_run_logger()

    if run_id is None:
        from datetime import datetime
        run_id = f"raw_to_staging_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    # Tables RAW a traiter
    tables = table_names if table_names else list_raw_tables()

    logger.info(f"[TARGET] {len(tables)} table(s) a traiter")

    total_rows = 0
    processed = []
    skipped = []

    for table in tables:
        try:
            # Verifier si table RAW a des donnees
            if not check_raw_table_has_data(table):
                logger.warning(f"[SKIP] {table} - Table RAW vide ou inexistante")
                skipped.append(table)
                continue  # Passer a la table suivante
            
            # Recuperer load_mode depuis sftp_monitoring
            load_mode = get_load_mode_for_table(table)
            logger.info(f"[MODE] {table} -> load_mode: {load_mode}")
            
            # Creation STAGING (DROP + CREATE si FULL/FULL_RESET)
            logger.info(f"[CONFIG] Creation STAGING {table}")
            create_staging_table(table, load_mode=load_mode)

            # Chargement avec UPSERT si INCREMENTAL
            logger.info(f"[LOAD] Chargement RAW -> STAGING {table}")
            rows = load_raw_to_staging(
                table_name=table,
                run_id=run_id,
                load_mode=load_mode  
            )

            total_rows += (rows or 0)
            processed.append(table)

            if rows is not None and rows > 0:
                logger.info(f"[OK] {table} : {rows:,} lignes ({load_mode})")
            else:
                logger.warning(f"[WARN] {table} : Aucune donnee chargee")
                
        except Exception as e:
            logger.error(f"[ERROR] Erreur traitement {table}: {e}")
            skipped.append(table)
            continue  # Ne pas bloquer le pipeline

    # Log recapitulatif
    logger.info("=" * 70)
    logger.info(f"[OK] Traitement termine")
    logger.info(f"   [OK] Traitees : {len(processed)} table(s)")
    logger.info(f"   [SKIP] Skipped : {len(skipped)} table(s)")
    if skipped:
        logger.info(f"   [LIST] Tables skipped : {', '.join(skipped)}")
    logger.info(f"   [DATA] Total lignes : {total_rows:,}")
    logger.info("=" * 70)

    return {
        "tables_processed": len(processed),
        "tables_skipped": len(skipped),
        "total_rows": total_rows,
        "tables": processed,
        "skipped_tables": skipped,
        "run_id": run_id
    }

@task(name="[STAGING] Traiter table individuelle", retries=1)
def raw_to_staging_single_table(table_name: str, run_id: str) -> dict:
    """
    Traite UNE SEULE table RAW → STAGING
    """
    logger = get_run_logger()
    
    try:
        # Vérifier si RAW a des données
        conn = psycopg2.connect(config.get_connection_string())
        cur = conn.cursor()
        
        raw_table = f"raw.raw_{table_name.lower()}"
        cur.execute(f"SELECT COUNT(*) FROM {raw_table}")
        row_count = cur.fetchone()[0]
        
        cur.close()
        conn.close()
        
        if row_count == 0:
            logger.warning(f"[SKIP] {table_name} : Aucune donnée dans RAW")
            return {
                'table': table_name,
                'rows': 0,
                'status': 'skipped'
            }
        
        logger.info(f"[OK] {table_name} : {row_count:,} lignes détectées")
        
        # ✅ FIX : Utiliser get_load_mode_for_table au lieu de get_load_mode_from_monitoring
        load_mode = get_load_mode_for_table(table_name)
        logger.info(f"[MODE] {table_name} -> {load_mode}")
        
        # Créer table STAGING (avec skip si existe)
        create_staging_table(table_name, load_mode)
        
        # Charger RAW → STAGING
        rows_affected = load_raw_to_staging(table_name, run_id, load_mode)
        
        if rows_affected == 0:
            logger.warning(f"[WARN] {table_name} : Aucune donnée chargée")
        else:
            logger.info(f"[OK] {table_name} : {rows_affected:,} lignes ({load_mode})")
        
        return {
            'table': table_name,
            'rows': rows_affected or 0,
            'status': 'success',
            'load_mode': load_mode
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


if __name__ == "__main__":
    raw_to_staging_flow()
