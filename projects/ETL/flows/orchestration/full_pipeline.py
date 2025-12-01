"""
============================================================================
Flow Prefect : Pipeline ETL Complet
============================================================================
Orchestration de bout en bout :
1. Metadata Progress → PostgreSQL
2. SFTP → RAW
3. RAW → STAGING_ETL
4. STAGING_ETL → ODS
5. ODS → PREP (dbt)
============================================================================
"""

from datetime import datetime
from prefect import flow
from prefect.logging import get_run_logger
import sys

sys.path.append(r'E:\Prefect\projects\ETL')

# Import des flows
from flows.ingestion.db_metadata_import import db_metadata_import_flow
from flows.ingestion.sftp_to_raw import sftp_to_raw_flow
from flows.ingestion.raw_to_staging import raw_to_staging_flow
from flows.ingestion.staging_to_ods import staging_to_ods_flow
from flows.transformations.ods_to_prep import ods_to_prep_flow


@flow(name="🚀 Pipeline ETL Complet", log_prints=True)
def full_etl_pipeline(run_dbt: bool = True):
    """
    Pipeline ETL complet de bout en bout
    
    Args:
        run_dbt: Exécuter dbt pour ODS → PREP
    
    Architecture :
    1. Metadata Progress → PostgreSQL
    2. SFTP → RAW (brut)
    3. RAW → STAGING_ETL (+ hashdiff)
    4. STAGING_ETL → ODS (merge)
    5. ODS → PREP (dbt)
    """
    logger = get_run_logger()
    start_time = datetime.now()
    
    logger.info("=" * 70)
    logger.info("🚀 PIPELINE ETL COMPLET")
    logger.info("=" * 70)
    
    run_id = f"full_pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    try:
        # 1. Metadata Progress
        logger.info("📚 Phase 1 : Import metadata Progress")
        try:
            db_metadata_import_flow()
        except Exception as e:
            logger.warning(f"⚠️ Pas de nouveaux metadata : {e}")
        
        # 2. SFTP → RAW
        logger.info("=" * 70)
        logger.info("📥 Phase 2 : SFTP → RAW")
        raw_result = sftp_to_raw_flow()
        
        if raw_result['tables_loaded'] == 0:
            logger.info("ℹ️ Aucune donnée à traiter, arrêt du pipeline")
            return
        
        # 3. RAW → STAGING
        logger.info("=" * 70)
        logger.info("📋 Phase 3 : RAW → STAGING_ETL")
        staging_result = raw_to_staging_flow(run_id=run_id)
        
        # 4. STAGING → ODS
        logger.info("=" * 70)
        logger.info("🔄 Phase 4 : STAGING → ODS")
        ods_result = staging_to_ods_flow(run_id=run_id)
        
        # 5. ODS → PREP (dbt)
        if run_dbt:
            logger.info("=" * 70)
            logger.info("⚙️ Phase 5 : ODS → PREP (dbt)")
            dbt_result = ods_to_prep_flow(models="prep.*", run_tests=True)
        else:
            logger.info("⏭️ Phase 5 : dbt ignoré")
            dbt_result = None
        
        # RÉSUMÉ
        total_duration = (datetime.now() - start_time).total_seconds()
        
        logger.info("=" * 70)
        logger.info("✅ PIPELINE COMPLET TERMINÉ")
        logger.info(f"⏱️  Durée totale : {total_duration:.2f}s")
        logger.info(f"📊 Tables chargées : {raw_result['tables_loaded']}")
        logger.info(f"📋 Tables staging : {staging_result['tables_processed']}")
        logger.info(f"🔄 Tables ODS : {ods_result['tables_merged']}")
        if dbt_result:
            logger.info(f"⚙️ Modèles dbt : {dbt_result['models_count']}")
        logger.info("=" * 70)
        
        return {
            'success': True,
            'duration_seconds': total_duration,
            'raw_tables': raw_result['tables_loaded'],
            'staging_tables': staging_result['tables_processed'],
            'ods_tables': ods_result['tables_merged'],
            'dbt_models': dbt_result['models_count'] if dbt_result else 0
        }
        
    except Exception as e:
        logger.error(f"❌ Erreur pipeline : {e}")
        raise


@flow(name="📥 Pipeline Ingestion seul (sans dbt)")
def ingestion_pipeline_only():
    """Pipeline ingestion Python uniquement (sans dbt)"""
    logger = get_run_logger()
    
    run_id = f"ingestion_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    logger.info("📥 Pipeline Ingestion : SFTP → RAW → STAGING → ODS")
    
    try:
        db_metadata_import_flow()
    except:
        pass
    
    sftp_to_raw_flow()
    raw_to_staging_flow(run_id=run_id)
    staging_to_ods_flow(run_id=run_id)
    
    logger.info("✅ Pipeline Ingestion terminé")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == '--no-dbt':
        ingestion_pipeline_only()
    else:
        full_etl_pipeline()