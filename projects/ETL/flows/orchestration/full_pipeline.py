"""
============================================================================
Flow Prefect : Pipeline ETL Complet (VERSION CORRIGÉE v3)
============================================================================
Propagation des tables traitées à travers le pipeline :
- SFTP → RAW détecte les nouvelles tables
- RAW → STAGING traite UNIQUEMENT ces tables
- STAGING → ODS merge UNIQUEMENT ces tables
============================================================================
"""

from datetime import datetime
from prefect import flow
from prefect.logging import get_run_logger
import sys

sys.path.append(r'E:\Prefect\projects\ETL')

from flows.ingestion.db_metadata_import import db_metadata_import_flow
from flows.ingestion.sftp_to_raw import sftp_to_raw_flow
from flows.ingestion.raw_to_staging import raw_to_staging_flow
from flows.ingestion.staging_to_ods import staging_to_ods_flow
from flows.transformations.ods_to_prep import ods_to_prep_flow


@flow(name="[START] Pipeline ETL Complet v3 (Propagation)", log_prints=True)
def full_etl_pipeline(run_dbt: bool = False, import_metadata: bool = False):
    """
    Pipeline ETL complet avec propagation des tables traitées
    
    Args:
        run_dbt: Exécuter dbt pour ODS → PREP (défaut: False)
        import_metadata: Importer metadata Progress (défaut: False)
    
    Architecture :
    1. SFTP → RAW : Détecte les nouvelles tables (ex: ['produit'])
    2. RAW → STAGING : Traite UNIQUEMENT ces tables
    3. STAGING → ODS : Merge UNIQUEMENT ces tables
    4. ODS → PREP (dbt) : Optionnel
    """
    logger = get_run_logger()
    start_time = datetime.now()
    run_id = f"full_pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    logger.info("=" * 70)
    logger.info("[START] PIPELINE ETL COMPLET - VERSION 3.0 (PROPAGATION)")
    logger.info(f"🆔 Run ID: {run_id}")
    logger.info("=" * 70)
    
    results = {
        'run_id': run_id,
        'start_time': start_time.isoformat(),
        'metadata_imported': False,
        'raw_tables': 0,
        'staging_tables': 0,
        'ods_tables': 0,
        'dbt_models': 0,
        'errors': []
    }
    
    try:
        # ========================================
        # 1. METADATA PROGRESS (OPTIONNEL)
        # ========================================
        if import_metadata:
            logger.info("=" * 70)
            logger.info("📚 Phase 1 : Import metadata Progress")
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
        # 2. SFTP → RAW (DÉTECTION)
        # ========================================
        logger.info("=" * 70)
        logger.info("📥 Phase 2 : SFTP → RAW (Ingestion brute)")
        
        raw_result = sftp_to_raw_flow()
        results['raw_tables'] = raw_result['tables_loaded']
        results['raw_rows'] = raw_result.get('total_rows', 0)
        
        # [OK] RÉCUPÉRER LA LISTE DES TABLES TRAITÉES
        tables_to_process = raw_result.get('tables', [])
        
        if raw_result['tables_loaded'] == 0:
            logger.info("[INFO] Aucune donnée SFTP à traiter")
            logger.info("🛑 Arrêt du pipeline (rien à faire)")
            results['end_time'] = datetime.now().isoformat()
            results['duration_seconds'] = (datetime.now() - start_time).total_seconds()
            return results
        
        logger.info(f"[OK] RAW : {results['raw_tables']} table(s) chargée(s)")
        logger.info(f"[LIST] Tables à traiter : {tables_to_process}")
        
        # ========================================
        # 3. RAW → STAGING (UNIQUEMENT LES NOUVELLES TABLES)
        # ========================================
        logger.info("=" * 70)
        logger.info("[LIST] Phase 3 : RAW → STAGING_ETL (Hashdiff + Enrichissement)")
        logger.info(f"[TARGET] Traitement de {len(tables_to_process)} table(s) : {tables_to_process}")
        
        # [OK] PASSER LA LISTE DES TABLES À TRAITER
        staging_result = raw_to_staging_flow(
            table_names=tables_to_process,  # ← NOUVELLE LOGIQUE
            run_id=run_id
        )
        results['staging_tables'] = staging_result['tables_processed']
        results['staging_rows'] = staging_result.get('total_rows', 0)
        
        logger.info(f"[OK] STAGING : {results['staging_tables']} table(s), {results['staging_rows']:,} lignes")
        
        # ========================================
        # 4. STAGING → ODS (UNIQUEMENT LES NOUVELLES TABLES)
        # ========================================
        logger.info("=" * 70)
        logger.info("[SYNC] Phase 4 : STAGING_ETL → ODS (Merge intelligent)")
        logger.info(f"[TARGET] Merge de {len(tables_to_process)} table(s) : {tables_to_process}")
        
        # [OK] PASSER LA LISTE DES TABLES À MERGER
        ods_result = staging_to_ods_flow(
            table_names=tables_to_process,  # ← NOUVELLE LOGIQUE
            run_id=run_id,
            load_mode="AUTO"
        )
        results['ods_tables'] = ods_result['tables_merged']
        results['ods_rows_affected'] = ods_result.get('total_rows_affected', 0)
        
        logger.info(f"[OK] ODS : {results['ods_tables']} table(s), {results['ods_rows_affected']:,} lignes affectées")
        
        # ========================================
        # 5. ODS → PREP (dbt) - OPTIONNEL
        # ========================================
        if run_dbt:
            logger.info("=" * 70)
            logger.info("[SETTINGS] Phase 5 : ODS → PREP (dbt transformations)")
            
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
        logger.info("[OK] PIPELINE COMPLET TERMINÉ")
        logger.info("=" * 70)
        logger.info(f"[TIMER]  Durée totale : {results['duration_seconds']:.2f}s")
        logger.info(f"📥 RAW : {results['raw_tables']} table(s), {results['raw_rows']:,} lignes")
        logger.info(f"[LIST] STAGING : {results['staging_tables']} table(s)")
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


@flow(name="📥 Pipeline Ingestion Python seul")
def ingestion_pipeline_only():
    """
    Pipeline ingestion Python uniquement (sans dbt)
    Avec propagation des tables détectées
    """
    logger = get_run_logger()
    run_id = f"ingestion_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    logger.info("=" * 70)
    logger.info("📥 PIPELINE INGESTION : SFTP → RAW → STAGING → ODS")
    logger.info(f"🆔 Run ID: {run_id}")
    logger.info("=" * 70)
    
    # 1. SFTP → RAW (détection)
    logger.info("📥 Phase 1 : SFTP → RAW")
    raw_result = sftp_to_raw_flow()
    
    if raw_result['tables_loaded'] == 0:
        logger.info("[INFO] Aucune donnée à traiter")
        return
    
    # [OK] Récupérer liste des tables chargées
    tables_to_process = raw_result.get('tables', [])
    logger.info(f"[LIST] Tables détectées : {tables_to_process}")
    
    # 2. RAW → STAGING (uniquement tables chargées)
    logger.info("=" * 70)
    logger.info("[LIST] Phase 2 : RAW → STAGING_ETL")
    staging_result = raw_to_staging_flow(
        table_names=tables_to_process,
        run_id=run_id
    )
    
    # 3. STAGING → ODS (uniquement tables chargées)
    logger.info("=" * 70)
    logger.info("[SYNC] Phase 3 : STAGING_ETL → ODS")
    ods_result = staging_to_ods_flow(
        table_names=tables_to_process,
        run_id=run_id
    )
    
    logger.info("=" * 70)
    logger.info("[OK] PIPELINE INGESTION TERMINÉ")
    logger.info(f"[DATA] {len(tables_to_process)} table(s) traitée(s)")
    logger.info("=" * 70)


if __name__ == "__main__":
    import sys
    
    # python full_pipeline.py --ingestion-only
    if len(sys.argv) > 1 and sys.argv[1] == '--ingestion-only':
        ingestion_pipeline_only()
    # python full_pipeline.py --with-dbt
    elif len(sys.argv) > 1 and sys.argv[1] == '--with-dbt':
        full_etl_pipeline(run_dbt=True)
    # python full_pipeline.py (défaut: sans dbt)
    else:
        full_etl_pipeline(run_dbt=False)