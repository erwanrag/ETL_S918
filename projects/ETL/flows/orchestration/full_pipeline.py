
"""
============================================================================
Flow Prefect : Pipeline ETL Complet v6 (avec Services)
============================================================================
Ajout du flow Services (Currency Data) dans le pipeline principal
"""


import sys
import os

# ============================================================
# SETUP PATHS & CONFIGURATION (CRITICAL ORDER)
# ============================================================
from pathlib import Path

# 1. Ajouter la racine du projet au PYTHONPATH
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

# 2. Importer le module de configuration complet
import shared.config as alerting_config

# 3. INJECTION DANS SYS.MODULES (LE HACK)
# Cela doit être fait AVANT d'importer alert_manager pour que
# "from config import ..." fonctionne à l'intérieur de alert_manager
sys.modules["config"] = alerting_config

# 4. Maintenant on peut importer l'objet config spécifique pour ce script
from shared.config import config

# ============================================================
# IMPORTS STANDARD & PREFECT
# ============================================================
from datetime import datetime
from prefect import flow
from prefect.logging import get_run_logger
from typing import Optional, List, Union

# ============================================================
# IMPORTS ETL
# ============================================================
from ETL.flows.ingestion.db_metadata_import import db_metadata_import_flow
from ETL.flows.ingestion.sftp_to_raw import sftp_to_raw_flow
from ETL.flows.ingestion.raw_to_staging import raw_to_staging_flow_parallel
from ETL.flows.ingestion.staging_to_ods import (
    staging_to_ods_single_table, 
    staging_to_ods_flow
)
from ETL.flows.transformations.ods_to_prep import ods_to_prep_flow
from ETL.flows.orchestration.parallel_helpers import (
    group_tables_by_size,
    log_grouping_info
)

# ============================================================
# IMPORT SERVICES
# ============================================================
from Services.flows.currency_rates import load_currency_data_flow

# ============================================================
# IMPORT ALERTING (Après le hack sys.modules)
# ============================================================
from shared.alerting.alert_manager import get_alert_manager, AlertLevel

# Initialisation du gestionnaire d'alertes
alert_mgr = get_alert_manager()


@flow(name="[00] 🚀 Pipeline ETL Proginov", log_prints=True)
def full_etl_pipeline(
    table_names=None,
    run_dbt=False, 
    import_metadata=False,
    enable_parallel=True,
    run_services=False
):
    """
    Pipeline ETL complet Proginov
    
    Args:
        table_names: Liste tables a traiter (None = toutes)
        run_services: Charger donnees devises
        run_dbt: Executer dbt apres ODS
        import_metadata: Importer metadonnees Progress
        enable_parallel: Activer parallelisation
    """
    logger = get_run_logger()
    start_time = datetime.now()
    run_id = f"etl_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    logger.info("=" * 70)
    logger.info("[START] PIPELINE ETL PROGINOV")
    logger.info(f"[RUN] {run_id}")
    logger.info(f"[TABLES] {table_names if table_names else 'ALL'}")
    logger.info(f"[PARALLEL] {'ON' if enable_parallel else 'OFF'}")
    logger.info(f"[SERVICES] {'ON' if run_services else 'OFF'}")
    logger.info(f"[DBT] {'ON' if run_dbt else 'OFF'}")
    logger.info("=" * 70)
    
    results = {
        'run_id': run_id,
        'start_time': start_time.isoformat(),
        'table_filter': table_names,
        'metadata_imported': False,
        'raw_tables': 0,
        'staging_tables': 0,
        'ods_tables': 0,
        'dbt_models': 0,
        'services_executed': False,
        'currency_codes': 0,
        'currency_rates': 0,
        'parallel_enabled': enable_parallel,
        'errors': []
    }
    
    try:
        # ========================================
        # 0. SERVICES (optionnel)
        # ========================================
        if run_services:
            logger.info("=" * 70)
            logger.info("[SERVICES] Currency Data")
            try:
                services_result = load_currency_data_flow()
                results['services_executed'] = True
                results['currency_codes'] = services_result.get('nb_codes', 0)
                results['currency_rates'] = services_result.get('nb_rates', 0)
                logger.info(f"[OK] {results['currency_codes']} codes, {results['currency_rates']} taux")
            except Exception as e:
                logger.error(f"[ERROR] Services : {e}")
                results['errors'].append(f"services: {str(e)}")
        else:
            logger.info("[SKIP] Services")
        
        # ========================================
        # 1. METADATA (optionnel)
        # ========================================
        if import_metadata:
            logger.info("=" * 70)
            logger.info("[METADATA] Progress")
            try:
                db_metadata_import_flow()
                results['metadata_imported'] = True
                logger.info("[OK] Metadata importes")
            except Exception as e:
                logger.warning(f"[SKIP] Metadata : {e}")
                results['errors'].append(f"metadata: {str(e)}")
        
        # ========================================
        # 2. SFTP → RAW
        # ========================================
        logger.info("=" * 70)
        logger.info("[SFTP] RAW")
        
        raw_result = sftp_to_raw_flow(table_filter=table_names)
        results['raw_tables'] = raw_result['tables_loaded']
        results['raw_rows'] = raw_result.get('total_rows', 0)
        
        tables_to_process = raw_result.get('tables', [])
        table_sizes = raw_result.get('table_sizes', {})
        
        # FILTRAGE : Appliquer table_names si fourni
        if table_names:
            logger.info(f"[FILTER] Filtrage sur {len(table_names)} tables")
            tables_to_process = [t for t in tables_to_process if t in table_names]
            logger.info(f"[FILTER] {len(tables_to_process)} tables retenues")
        
        if len(tables_to_process) == 0:
            logger.info("[STOP] Aucune table a traiter")
            results['end_time'] = datetime.now().isoformat()
            results['duration_seconds'] = (datetime.now() - start_time).total_seconds()
            
            if run_services and results['services_executed']:
                alert_mgr.send_alert(
                    level=AlertLevel.INFO,
                    title="Pipeline ETL - No Data",
                    message="Services OK, aucune donnee ETL a traiter",
                    context={
                        "Run ID": run_id,
                        "Services": f"OK {results['currency_codes']} codes",
                        "Filter": str(table_names) if table_names else "None",
                        "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }
                )
            
            return results
        
        logger.info(f"[OK] {len(tables_to_process)} tables, {results['raw_rows']:,} lignes")
        
        # ========================================
        # 3. DEDUPLICATION + GROUPEMENT
        # ========================================
        logger.info("=" * 70)
        logger.info("[OPTIM] Deduplication")
        
        tables_unique = list(set(tables_to_process))
        tables_unique.sort()
        
        logger.info(f"[DEDUPE] {len(tables_to_process)} fichiers → {len(tables_unique)} tables")
        
        if enable_parallel and table_sizes:
            groups = group_tables_by_size(tables_unique, table_sizes)
            log_grouping_info(groups, table_sizes, logger)
        else:
            groups = {'small': [], 'medium': tables_unique, 'large': []}
            logger.info(f"[SEQ] {len(tables_unique)} tables")
        
        # ========================================
        # 4. RAW → STAGING
        # ========================================
        logger.info("=" * 70)
        logger.info("[STAGING] Processing")

        staging_start = datetime.now()

        try:
            if enable_parallel:
                staging_result = raw_to_staging_flow_parallel(
                    table_names=tables_unique,
                    run_id=run_id
                )
            else:
                from flows.ingestion.raw_to_staging import raw_to_staging_flow
                staging_result = raw_to_staging_flow(
                    table_names=tables_unique,
                    run_id=run_id
                )
            
            # Sécurisation contre None
            if staging_result is None:
                logger.error("[STAGING] Flow returned None (crashed)")
                results['staging_tables'] = 0
                results['staging_rows'] = 0
            else:
                results['staging_tables'] = staging_result.get('tables_processed', 0)
                results['staging_rows'] = staging_result.get('total_rows', 0)
                
        except Exception as e:
            logger.error(f"[STAGING] Fatal error: {e}")
            results['staging_tables'] = 0
            results['staging_rows'] = 0
            # Continue le pipeline malgré l'erreur STAGING
        

        # ========================================
        # 5. STAGING → ODS
        # ========================================
        logger.info("=" * 70)
        logger.info("[ODS] Processing")
        
        ods_start = datetime.now()
        
        # TOUJOURS utiliser le subflow (pour coherence UI)
        from flows.ingestion.staging_to_ods import staging_to_ods_flow
        
        ods_result = staging_to_ods_flow(
            table_names=tables_unique,
            load_mode="AUTO",
            run_id=run_id
        )
        
        results['ods_tables'] = ods_result.get('tables_merged', 0)
        results['ods_rows_affected'] = ods_result.get('total_rows_affected', 0)
        
        ods_duration = (datetime.now() - ods_start).total_seconds()
        logger.info(f"[OK] {results['ods_tables']} tables, {ods_duration:.2f}s")

        
        # ========================================
        # 6. ODS → PREP (dbt optionnel)
        # ========================================
        if run_dbt:
            logger.info("=" * 70)
            logger.info("[DBT] Transformations")
            try:
                dbt_result = ods_to_prep_flow(models="prep.*", run_tests=False)
                results['dbt_models'] = dbt_result.get('models_count', 0)
                logger.info(f"[OK] {results['dbt_models']} modeles")
            except Exception as e:
                logger.error(f"[ERROR] dbt : {e}")
                results['errors'].append(f"dbt: {str(e)}")
        
        # ========================================
        # 7. RESUME FINAL
        # ========================================
        end_time = datetime.now()
        results['end_time'] = end_time.isoformat()
        results['duration_seconds'] = (end_time - start_time).total_seconds()
        results['success'] = len(results['errors']) == 0
        
        logger.info("=" * 70)
        logger.info("[DONE] PIPELINE TERMINE")
        logger.info(f"[TIMER] {results['duration_seconds']:.2f}s")
        logger.info(f"  RAW: {results['raw_tables']} tables")
        logger.info(f"  STAGING: {results['staging_tables']} tables")
        logger.info(f"  ODS: {results['ods_tables']} tables")
        
        if run_services:
            logger.info(f"  SERVICES: {results['currency_codes']} codes")
        if run_dbt:
            logger.info(f"  DBT: {results['dbt_models']} modeles")
        if results['errors']:
            logger.warning(f"  ERRORS: {len(results['errors'])}")
        
        logger.info("=" * 70)
        
        # ========================================
        # ALERTING
        # ========================================
        if results['success']:
            context = {
                "Run ID": run_id,
                "Duration": f"{results['duration_seconds']:.2f}s",
                "Tables": f"{results['ods_tables']} tables",
                "Timestamp": end_time.strftime("%Y-%m-%d %H:%M:%S")
            }
            
            if table_names:
                context["Filter"] = f"{len(table_names)} tables"
            
            if run_services:
                context["Services"] = f"{results['currency_codes']} codes"
            
            if run_dbt:
                context["DBT"] = f"{results['dbt_models']} modeles"
            
            alert_mgr.send_alert(
                level=AlertLevel.INFO,
                title="Pipeline ETL Proginov - SUCCESS",
                message="Pipeline execute avec succes",
                context=context
            )
        else:
            context = {
                "Run ID": run_id,
                "Duration": f"{results['duration_seconds']:.2f}s",
                "Errors": ", ".join(results['errors']),
                "Tables": f"{results['ods_tables']} tables",
                "Timestamp": end_time.strftime("%Y-%m-%d %H:%M:%S")
            }
            
            alert_mgr.send_alert(
                level=AlertLevel.ERROR,
                title="Pipeline ETL Proginov - PARTIAL FAILURE",
                message=f"Pipeline termine avec {len(results['errors'])} erreurs",
                context=context
            )
        
        return results
        
    except Exception as e:
        logger.error(f"[CRITICAL] {e}")
        
        end_time = datetime.now()
        results['end_time'] = end_time.isoformat()
        results['duration_seconds'] = (end_time - start_time).total_seconds()
        results['success'] = False
        results['errors'].append(f"CRITICAL: {str(e)}")
        
        alert_mgr.send_alert(
            level=AlertLevel.CRITICAL,
            title="Pipeline ETL Proginov - CRITICAL",
            message="Erreur critique",
            context={
                "Run ID": run_id,
                "Error": str(e),
                "Duration": f"{results['duration_seconds']:.2f}s",
                "Timestamp": end_time.strftime("%Y-%m-%d %H:%M:%S")
            }
        )
        
        raise


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == '--with-services':
            full_etl_pipeline(run_services=True)
        elif sys.argv[1] == '--with-dbt':
            full_etl_pipeline(run_dbt=True)
        elif sys.argv[1] == '--table':
            table = sys.argv[2] if len(sys.argv) > 2 else None
            full_etl_pipeline(table_names=[table] if table else None)
        else:
            full_etl_pipeline()
    else:
        full_etl_pipeline()


