"""
============================================================================
Flow Prefect : Pipeline ETL Complet v5 (OPTIMISÉ)
============================================================================
"""

from datetime import datetime
from prefect import flow
from prefect.logging import get_run_logger
from concurrent.futures import as_completed
import sys

sys.path.append(r'E:\Prefect\projects\ETL')

from flows.ingestion.db_metadata_import import db_metadata_import_flow
from flows.ingestion.sftp_to_raw import sftp_to_raw_flow
from flows.ingestion.raw_to_staging import raw_to_staging_flow_parallel, raw_to_staging_single_table
from flows.ingestion.staging_to_ods import staging_to_ods_flow, staging_to_ods_single_table
from flows.transformations.ods_to_prep import ods_to_prep_flow
from flows.orchestration.parallel_helpers import group_tables_by_size, log_grouping_info


@flow(name="[START] Pipeline ETL v5 (Optimisé)", log_prints=True)
def full_etl_pipeline(
    run_dbt: bool = False, 
    import_metadata: bool = False,
    enable_parallel: bool = True
):
    """Pipeline ETL complet optimisé"""
    logger = get_run_logger()
    start_time = datetime.now()
    run_id = f"full_pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    logger.info("=" * 70)
    logger.info("[START] PIPELINE ETL v5 (OPTIMISÉ)")
    logger.info(f"[RUN] {run_id}")
    logger.info(f"[PARALLEL] {'ACTIF' if enable_parallel else 'INACTIF'}")
    logger.info("=" * 70)
    
    results = {
        'run_id': run_id,
        'start_time': start_time.isoformat(),
        'metadata_imported': False,
        'raw_tables': 0,
        'staging_tables': 0,
        'ods_tables': 0,
        'dbt_models': 0,
        'parallel_enabled': enable_parallel,
        'errors': []
    }
    
    try:
        # ========================================
        # 1. METADATA (optionnel)
        # ========================================
        if import_metadata:
            logger.info("=" * 70)
            logger.info("[BOOKS] Metadata Progress")
            try:
                db_metadata_import_flow()
                results['metadata_imported'] = True
                logger.info("[OK] Metadata importés")
            except Exception as e:
                logger.warning(f"[SKIP] Metadata : {e}")
                results['errors'].append(f"metadata: {str(e)}")
        else:
            logger.info("[SKIP] Metadata")
        
        # ========================================
        # 2. SFTP → RAW
        # ========================================
        logger.info("=" * 70)
        logger.info("[DATA] SFTP → RAW")
        
        raw_result = sftp_to_raw_flow()
        results['raw_tables'] = raw_result['tables_loaded']
        results['raw_rows'] = raw_result.get('total_rows', 0)
        
        tables_to_process = raw_result.get('tables', [])
        table_sizes = raw_result.get('table_sizes', {})
        
        if raw_result['tables_loaded'] == 0:
            logger.info("[STOP] Rien à traiter")
            results['end_time'] = datetime.now().isoformat()
            results['duration_seconds'] = (datetime.now() - start_time).total_seconds()
            return results
        
        logger.info(f"[OK] {results['raw_tables']} tables, {results['raw_rows']:,} lignes")
        
        # ========================================
        # 3. DÉDUPLICATION + GROUPEMENT
        # ========================================
        logger.info("=" * 70)
        logger.info("[OPTIM] Déduplication + Groupement")
        
        tables_unique = list(set(tables_to_process))
        tables_unique.sort()
        
        logger.info(f"[DEDUPE] {len(tables_to_process)} fichiers → {len(tables_unique)} tables")
        
        # Groupement par taille
        if enable_parallel and table_sizes:
            groups = group_tables_by_size(tables_unique, table_sizes)
            log_grouping_info(groups, table_sizes, logger)
        else:
            groups = {'small': [], 'medium': tables_unique, 'large': []}
            logger.info(f"[SEQ] {len(tables_unique)} tables")
        
        # ========================================
        # 4. RAW → STAGING (PARALLÈLE)
        # ========================================
        logger.info("=" * 70)
        logger.info("[PARALLEL] RAW → STAGING")
        
        staging_start = datetime.now()
        
        # Utiliser directement le flow parallèle optimisé
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
        
        results['staging_tables'] = staging_result.get('tables_processed', 0)
        results['staging_rows'] = staging_result.get('total_rows', 0)
        
        staging_duration = (datetime.now() - staging_start).total_seconds()
        logger.info(f"[OK] {results['staging_tables']} tables, {results['staging_rows']:,} lignes")
        logger.info(f"[TIMER] {staging_duration:.2f}s")
        
        # ========================================
        # 5. STAGING → ODS (PARALLÈLE)
        # ========================================
        logger.info("=" * 70)
        logger.info("[PARALLEL] STAGING → ODS")
        
        ods_start = datetime.now()
        ods_results = []
        
        if enable_parallel:
            # SMALL : Parallèle illimité
            if groups['small']:
                logger.info(f"[SMALL] {len(groups['small'])} tables")
                small_ods = staging_to_ods_single_table.map(
                    table_name=groups['small'],
                    run_id=[run_id] * len(groups['small']),
                    load_mode=["AUTO"] * len(groups['small'])
                )
                ods_results.extend(small_ods)
            
            # MEDIUM : Chunks de 3
            if groups['medium']:
                logger.info(f"[MEDIUM] {len(groups['medium'])} tables (x3)")
                for i in range(0, len(groups['medium']), 3):
                    chunk = groups['medium'][i:i+3]
                    medium_ods = staging_to_ods_single_table.map(
                        table_name=chunk,
                        run_id=[run_id] * len(chunk),
                        load_mode=["AUTO"] * len(chunk)
                    )
                    ods_results.extend(medium_ods)
            
            # LARGE : Séquentiel
            if groups['large']:
                logger.info(f"[LARGE] {len(groups['large'])} tables (seq)")
                for table in groups['large']:
                    result = staging_to_ods_single_table(table, run_id, "AUTO")
                    ods_results.append(result)
            
            # Résoudre futures
            resolved = []
            for future in ods_results:
                try:
                    r = future.result() if hasattr(future, 'result') else future
                    resolved.append(r)
                except Exception as e:
                    logger.error(f"[ERROR] Future : {e}")
            
            successful = [r for r in resolved if r and r.get('status') == 'success']
            results['ods_tables'] = len(successful)
            results['ods_rows_affected'] = sum(r.get('rows', 0) for r in successful)
        else:
            # Séquentiel
            ods_result = staging_to_ods_flow(tables_unique, run_id, "AUTO")
            results['ods_tables'] = ods_result['tables_merged']
            results['ods_rows_affected'] = ods_result.get('total_rows_affected', 0)
        
        ods_duration = (datetime.now() - ods_start).total_seconds()
        logger.info(f"[OK] {results['ods_tables']} tables, {results['ods_rows_affected']:,} lignes")
        logger.info(f"[TIMER] {ods_duration:.2f}s")
        
        # ========================================
        # 6. ODS → PREP (dbt optionnel)
        # ========================================
        if run_dbt:
            logger.info("=" * 70)
            logger.info("[DBT] ODS → PREP")
            try:
                dbt_result = ods_to_prep_flow(models="prep.*", run_tests=False)
                results['dbt_models'] = dbt_result.get('models_count', 0)
                logger.info(f"[OK] {results['dbt_models']} modèles")
            except Exception as e:
                logger.error(f"[ERROR] dbt : {e}")
                results['errors'].append(f"dbt: {str(e)}")
        else:
            logger.info("[SKIP] dbt")
        
        # ========================================
        # RÉSUMÉ
        # ========================================
        end_time = datetime.now()
        results['end_time'] = end_time.isoformat()
        results['duration_seconds'] = (end_time - start_time).total_seconds()
        results['success'] = len(results['errors']) == 0
        
        logger.info("=" * 70)
        logger.info("[DONE] PIPELINE TERMINÉ")
        logger.info("=" * 70)
        logger.info(f"[TIMER] Total: {results['duration_seconds']:.2f}s")
        logger.info(f"  RAW: {results['raw_tables']} tables")
        logger.info(f"  STAGING: {results['staging_tables']} tables, {staging_duration:.2f}s")
        logger.info(f"  ODS: {results['ods_tables']} tables, {ods_duration:.2f}s")
        if run_dbt:
            logger.info(f"  DBT: {results['dbt_models']} modèles")
        if results['errors']:
            logger.warning(f"[WARN] {len(results['errors'])} erreurs")
        logger.info("=" * 70)
        
        return results
        
    except Exception as e:
        logger.error(f"[ERROR] CRITIQUE : {e}")
        results['end_time'] = datetime.now().isoformat()
        results['duration_seconds'] = (datetime.now() - start_time).total_seconds()
        results['success'] = False
        results['errors'].append(f"CRITICAL: {str(e)}")
        raise


@flow(name="[DATA] Ingestion seule")
def ingestion_pipeline_only(enable_parallel: bool = True):
    """Pipeline ingestion sans dbt"""
    return full_etl_pipeline(
        run_dbt=False,
        import_metadata=False,
        enable_parallel=enable_parallel
    )


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == '--ingestion-only':
            ingestion_pipeline_only()
        elif sys.argv[1] == '--with-dbt':
            full_etl_pipeline(run_dbt=True)
        elif sys.argv[1] == '--sequential':
            full_etl_pipeline(enable_parallel=False)
        else:
            full_etl_pipeline()
    else:
        full_etl_pipeline()