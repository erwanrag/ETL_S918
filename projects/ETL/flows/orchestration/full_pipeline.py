"""
============================================================================
Flow Prefect : Pipeline ETL Complet v4 (Parallélisation Intelligente)
============================================================================
Propagation + Parallélisation par taille de fichier :
- SFTP → RAW détecte tables + capture row_count
- Groupement par taille (small/medium/large)
- RAW → STAGING en parallèle selon la taille
- STAGING → ODS en parallèle selon la taille
============================================================================
"""

from datetime import datetime
from prefect import flow
from prefect.logging import get_run_logger
import sys

sys.path.append(r'E:\Prefect\projects\ETL')

from flows.ingestion.db_metadata_import import db_metadata_import_flow
from flows.ingestion.sftp_to_raw import sftp_to_raw_flow
from flows.ingestion.raw_to_staging import raw_to_staging_flow, raw_to_staging_single_table
from flows.ingestion.staging_to_ods import staging_to_ods_flow, staging_to_ods_single_table
from flows.transformations.ods_to_prep import ods_to_prep_flow
from flows.orchestration.parallel_helpers import group_tables_by_size, log_grouping_info


@flow(name="[START] Pipeline ETL Complet v4 (Parallélisation)", log_prints=True)
def full_etl_pipeline(
    run_dbt: bool = False, 
    import_metadata: bool = False,
    enable_parallel: bool = True
):
    """
    Pipeline ETL complet avec parallélisation intelligente
    
    Args:
        run_dbt: Exécuter dbt pour ODS → PREP (défaut: False)
        import_metadata: Importer metadata Progress (défaut: False)
        enable_parallel: Activer parallélisation (défaut: True)
    
    Architecture :
    1. SFTP → RAW : Détecte tables + capture row_count
    2. Groupement par taille (small/medium/large)
    3. RAW → STAGING : Parallélisé par groupe
    4. STAGING → ODS : Parallélisé par groupe
    5. ODS → PREP (dbt) : Optionnel
    """
    logger = get_run_logger()
    start_time = datetime.now()
    run_id = f"full_pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    logger.info("=" * 70)
    logger.info("[START] PIPELINE ETL COMPLET - VERSION 4.0 (PARALLELISATION)")
    logger.info(f"[RUN] Run ID: {run_id}")
    logger.info(f"[PARALLEL] Mode : {'ACTIVE' if enable_parallel else 'DESACTIVE'}")
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
        # 1. METADATA PROGRESS (OPTIONNEL)
        # ========================================
        if import_metadata:
            logger.info("=" * 70)
            logger.info("[BOOKS] Phase 1 : Import metadata Progress")
            try:
                db_metadata_import_flow()
                results['metadata_imported'] = True
                logger.info("[OK] Metadata importés")
            except Exception as e:
                logger.warning(f"[WARN] Metadata skip : {e}")
                results['errors'].append(f"metadata: {str(e)}")
        else:
            logger.info("[SKIP] Phase 1 : Metadata ignorée (import_metadata=False)")
        
        # ========================================
        # 2. SFTP → RAW (DÉTECTION + ROW_COUNT)
        # ========================================
        logger.info("=" * 70)
        logger.info("[DATA] Phase 2 : SFTP -> RAW (Ingestion brute)")
        
        raw_result = sftp_to_raw_flow()
        results['raw_tables'] = raw_result['tables_loaded']
        results['raw_rows'] = raw_result.get('total_rows', 0)
        
        tables_to_process = raw_result.get('tables', [])
        table_sizes = raw_result.get('table_sizes', {})
        
        if raw_result['tables_loaded'] == 0:
            logger.info("[INFO] Aucune donnée SFTP à traiter")
            logger.info("[STOP] Arrêt du pipeline (rien à faire)")
            results['end_time'] = datetime.now().isoformat()
            results['duration_seconds'] = (datetime.now() - start_time).total_seconds()
            return results
        
        logger.info(f"[OK] RAW : {results['raw_tables']} table(s) chargée(s)")
        logger.info(f"[LIST] Tables à traiter : {tables_to_process}")
        
        # ========================================
        # 3. DÉDUPLICATION + GROUPEMENT PAR TAILLE
        # ========================================
        logger.info("=" * 70)
        logger.info("[OPTIM] Optimisations : Déduplication + Groupement")
        
        # Déduplication
        tables_raw_list = tables_to_process
        tables_unique = list(set(tables_raw_list))
        tables_unique.sort()
        
        logger.info(f"[DEDUPE] {len(tables_raw_list)} fichiers -> {len(tables_unique)} tables uniques")
        
        if len(tables_raw_list) != len(tables_unique):
            from collections import Counter
            counts = Counter(tables_raw_list)
            duplicates = {t: c for t, c in counts.items() if c > 1}
            logger.info(f"[DEDUPE] Doublons détectés : {duplicates}")
        
        # Groupement par taille
        if enable_parallel and table_sizes:
            groups = group_tables_by_size(tables_unique, table_sizes)
            log_grouping_info(groups, table_sizes, logger)
        else:
            # Mode séquentiel : tout dans "medium"
            groups = {
                'small': [],
                'medium': tables_unique,
                'large': []
            }
            logger.info(f"[SEQUENTIAL] Mode séquentiel : {len(tables_unique)} table(s)")
        
        # ========================================
        # 4. RAW → STAGING (PARALLÉLISÉ)
        # ========================================
        logger.info("=" * 70)
        logger.info("[PARALLEL] Phase 3 : RAW -> STAGING_ETL")
        
        staging_start = datetime.now()
        staging_results = []
        
        if enable_parallel:
            # SMALL : Parallèle illimité
            if groups['small']:
                logger.info(f"[SMALL] {len(groups['small'])} tables en parallèle illimité")
                small_results = raw_to_staging_single_table.map(
                    table_name=groups['small'],
                    run_id=[run_id] * len(groups['small'])
                )
                staging_results.extend(small_results)
            
            # MEDIUM : Parallèle par chunks de 3
            if groups['medium']:
                logger.info(f"[MEDIUM] {len(groups['medium'])} tables (chunks de 3)")
                for i in range(0, len(groups['medium']), 3):
                    chunk = groups['medium'][i:i+3]
                    logger.info(f"   Chunk {i//3 + 1} : {chunk}")
                    medium_results = raw_to_staging_single_table.map(
                        table_name=chunk,
                        run_id=[run_id] * len(chunk)
                    )
                    staging_results.extend(medium_results)
            
            # LARGE : Séquentiel
            if groups['large']:
                logger.info(f"[LARGE] {len(groups['large'])} tables séquentiellement")
                for table in groups['large']:
                    logger.info(f"   Traitement : {table}")
                    result = raw_to_staging_single_table(table_name=table, run_id=run_id)
                    staging_results.append(result)
        else:
            # Mode séquentiel classique
            logger.info(f"[SEQUENTIAL] Traitement de {len(tables_unique)} tables")
            staging_flow_result = raw_to_staging_flow(
                table_names=tables_unique,
                run_id=run_id
            )
            results['staging_tables'] = staging_flow_result['tables_processed']
            results['staging_rows'] = staging_flow_result.get('total_rows', 0)
        
        # Agréger résultats parallèles
        if enable_parallel and staging_results:
            # ✅ FIX : Résoudre les futures avec .result()
            resolved_results = []
            for future in staging_results:
                try:
                    result = future.result() if hasattr(future, 'result') else future
                    resolved_results.append(result)
                except Exception as e:
                    logger.error(f"[ERROR] Erreur résolution future : {e}")
                    continue
            
            successful = [r for r in resolved_results if r and r.get('status') == 'success']
            total_rows = sum(r.get('rows', 0) for r in successful)
            results['staging_tables'] = len(successful)
            results['staging_rows'] = total_rows
        
        staging_duration = (datetime.now() - staging_start).total_seconds()
        logger.info(f"[OK] STAGING : {results['staging_tables']} table(s), {results['staging_rows']:,} lignes")
        logger.info(f"[TIMER] Durée STAGING : {staging_duration:.2f}s")
        
        # ========================================
        # 5. STAGING → ODS (PARALLÉLISÉ)
        # ========================================
        logger.info("=" * 70)
        logger.info("[PARALLEL] Phase 4 : STAGING_ETL -> ODS")
        
        ods_start = datetime.now()
        ods_results = []
        
        if enable_parallel:
            # SMALL : Parallèle illimité
            if groups['small']:
                logger.info(f"[SMALL] {len(groups['small'])} tables en parallèle illimité")
                small_ods = staging_to_ods_single_table.map(
                    table_name=groups['small'],
                    run_id=[run_id] * len(groups['small']),
                    load_mode=["AUTO"] * len(groups['small'])
                )
                ods_results.extend(small_ods)
            
            # MEDIUM : Parallèle par chunks de 3
            if groups['medium']:
                logger.info(f"[MEDIUM] {len(groups['medium'])} tables (chunks de 3)")
                for i in range(0, len(groups['medium']), 3):
                    chunk = groups['medium'][i:i+3]
                    logger.info(f"   Chunk {i//3 + 1} : {chunk}")
                    medium_ods = staging_to_ods_single_table.map(
                        table_name=chunk,
                        run_id=[run_id] * len(chunk),
                        load_mode=["AUTO"] * len(chunk)
                    )
                    ods_results.extend(medium_ods)
            
            # LARGE : Séquentiel
            if groups['large']:
                logger.info(f"[LARGE] {len(groups['large'])} tables séquentiellement")
                for table in groups['large']:
                    logger.info(f"   Merge : {table}")
                    result = staging_to_ods_single_table(
                        table_name=table,
                        run_id=run_id,
                        load_mode="AUTO"
                    )
                    ods_results.append(result)
        else:
            # Mode séquentiel classique
            logger.info(f"[SEQUENTIAL] Merge de {len(tables_unique)} tables")
            ods_flow_result = staging_to_ods_flow(
                table_names=tables_unique,
                run_id=run_id,
                load_mode="AUTO"
            )
            results['ods_tables'] = ods_flow_result['tables_merged']
            results['ods_rows_affected'] = ods_flow_result.get('total_rows_affected', 0)
                
        # Agréger résultats parallèles
        if enable_parallel and ods_results:
            # ✅ FIX : Résoudre les futures avec .result()
            resolved_results = []
            for future in ods_results:
                try:
                    result = future.result() if hasattr(future, 'result') else future
                    resolved_results.append(result)
                except Exception as e:
                    logger.error(f"[ERROR] Erreur résolution future : {e}")
                    continue
            
            successful = [r for r in resolved_results if r and r.get('status') == 'success']
            total_rows = sum(r.get('rows', 0) for r in successful)
            results['ods_tables'] = len(successful)
            results['ods_rows_affected'] = total_rows
        
        ods_duration = (datetime.now() - ods_start).total_seconds()
        logger.info(f"[OK] ODS : {results['ods_tables']} table(s), {results['ods_rows_affected']:,} lignes affectées")
        logger.info(f"[TIMER] Durée ODS : {ods_duration:.2f}s")
        
        # ========================================
        # 6. ODS → PREP (dbt) - OPTIONNEL
        # ========================================
        if run_dbt:
            logger.info("=" * 70)
            logger.info("[SETTINGS] Phase 5 : ODS -> PREP (dbt transformations)")
            
            try:
                dbt_result = ods_to_prep_flow(models="prep.*", run_tests=False)
                results['dbt_models'] = dbt_result.get('models_count', 0)
                results['dbt_tests_passed'] = dbt_result.get('tests_passed', None)
                
                logger.info(f"[OK] dbt : {results['dbt_models']} modèle(s)")
            except Exception as e:
                logger.error(f"[ERROR] Erreur dbt : {e}")
                results['errors'].append(f"dbt: {str(e)}")
        else:
            logger.info("[SKIP] Phase 5 : dbt ignorée (run_dbt=False)")
        
        # ========================================
        # RÉSUMÉ FINAL
        # ========================================
        end_time = datetime.now()
        results['end_time'] = end_time.isoformat()
        results['duration_seconds'] = (end_time - start_time).total_seconds()
        results['success'] = len(results['errors']) == 0
        
        logger.info("=" * 70)
        logger.info("[OK] PIPELINE COMPLET TERMINE")
        logger.info("=" * 70)
        logger.info(f"[TIMER] Durée totale : {results['duration_seconds']:.2f}s")
        logger.info(f"  [TIMER] STAGING : {staging_duration:.2f}s")
        logger.info(f"  [TIMER] ODS : {ods_duration:.2f}s")
        logger.info(f"[DATA] RAW : {results['raw_tables']} table(s), {results['raw_rows']:,} lignes")
        logger.info(f"[LIST] STAGING : {results['staging_tables']} table(s), {results['staging_rows']:,} lignes")
        logger.info(f"[SYNC] ODS : {results['ods_tables']} table(s), {results['ods_rows_affected']:,} lignes affectées")
        
        if run_dbt:
            logger.info(f"[SETTINGS] dbt : {results['dbt_models']} modèle(s)")
        
        if results['errors']:
            logger.warning(f"[WARN] {len(results['errors'])} erreur(s) non bloquante(s)")
        
        logger.info("=" * 70)
        
        return results
        
    except Exception as e:
        logger.error(f"[ERROR] ERREUR CRITIQUE PIPELINE : {e}")
        results['end_time'] = datetime.now().isoformat()
        results['duration_seconds'] = (datetime.now() - start_time).total_seconds()
        results['success'] = False
        results['errors'].append(f"CRITICAL: {str(e)}")
        raise


@flow(name="[DATA] Pipeline Ingestion Python seul")
def ingestion_pipeline_only(enable_parallel: bool = True):
    """
    Pipeline ingestion Python uniquement (sans dbt)
    Avec parallélisation intelligente
    """
    logger = get_run_logger()
    run_id = f"ingestion_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    logger.info("=" * 70)
    logger.info("[DATA] PIPELINE INGESTION : SFTP -> RAW -> STAGING -> ODS")
    logger.info(f"[RUN] Run ID: {run_id}")
    logger.info(f"[PARALLEL] Mode : {'ACTIVE' if enable_parallel else 'DESACTIVE'}")
    logger.info("=" * 70)
    
    return full_etl_pipeline(
        run_dbt=False,
        import_metadata=False,
        enable_parallel=enable_parallel
    )


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == '--ingestion-only':
        ingestion_pipeline_only()
    elif len(sys.argv) > 1 and sys.argv[1] == '--with-dbt':
        full_etl_pipeline(run_dbt=True)
    elif len(sys.argv) > 1 and sys.argv[1] == '--sequential':
        full_etl_pipeline(run_dbt=False, enable_parallel=False)
    else:
        full_etl_pipeline(run_dbt=False, enable_parallel=True)