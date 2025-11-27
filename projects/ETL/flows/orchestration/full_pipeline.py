"""
============================================================================
Flow Prefect : Pipeline Complet ETL + dbt
============================================================================
Orchestration complète :
1. Import metadata Progress
2. SFTP → RAW → STAGING_ETL → ODS (Python)
3. ODS → PREP → MARTS (dbt)
============================================================================
"""

import subprocess
from pathlib import Path
from datetime import datetime
from prefect import flow, task
from prefect.logging import get_run_logger
import sys

sys.path.append(r'E:\Prefect\projects\ETL')
from flows.config.pg_config import config

# Import des flows existants
from flows.ingestion.db_metadata_import import db_metadata_import_flow
from flows.ingestion.sftp_to_ods_flow import sftp_to_ods_complete_flow


@task(name="🔨 Exécuter dbt models", retries=1)
def run_dbt_models(models: str = "prep.*"):
    """
    Exécuter dbt pour transformer ODS → PREP
    
    Args:
        models: Sélecteur dbt (défaut: "prep.*")
    """
    logger = get_run_logger()
    
    dbt_project_dir = Path(config.dbt_project_dir)
    
    cmd = ["dbt", "run", "--models", models, "--project-dir", str(dbt_project_dir)]
    
    logger.info(f"🔨 Commande dbt : {' '.join(cmd)}")
    
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=str(dbt_project_dir),
            timeout=1800  # 30 minutes max
        )
        
        if result.stdout:
            logger.info(f"📄 dbt output:\n{result.stdout}")
        
        if result.stderr:
            logger.warning(f"⚠️ dbt warnings:\n{result.stderr}")
        
        if result.returncode != 0:
            raise Exception(f"dbt run failed with code {result.returncode}")
        
        # Compter les modèles créés
        models_count = result.stdout.count('OK created') if result.stdout else 0
        logger.info(f"✅ {models_count} modèle(s) dbt créé(s)")
        
        return {'success': True, 'models_count': models_count}
        
    except subprocess.TimeoutExpired:
        logger.error("❌ dbt timeout (> 30 min)")
        raise
    except Exception as e:
        logger.error(f"❌ Erreur dbt : {e}")
        raise


@flow(name="🚀 Pipeline ETL Complet : Phase 1 + Phase 2 + dbt", log_prints=True)
def full_etl_pipeline():
    """
    Pipeline complet de bout en bout
    
    Architecture :
    1. Import metadata Progress → PostgreSQL
    2. SFTP → RAW → STAGING_ETL → ODS (Python/Prefect)
    3. ODS → PREP (dbt)
    4. PREP → MARTS (dbt - futur)
    """
    logger = get_run_logger()
    
    start_time = datetime.now()
    
    logger.info("=" * 70)
    logger.info("🚀 PIPELINE ETL COMPLET : SFTP → RAW → ODS → PREP")
    logger.info("=" * 70)
    
    try:
        # ===================================================================
        # PHASE 0 : Import metadata Progress (si nouveaux fichiers)
        # ===================================================================
        logger.info("📚 Phase 0 : Import metadata Progress")
        try:
            db_metadata_import_flow()
        except Exception as e:
            logger.warning(f"⚠️ Pas de nouveaux metadata : {e}")
        
        # ===================================================================
        # PHASE 1 : Ingestion Python (SFTP → RAW → STAGING_ETL → ODS)
        # ===================================================================
        logger.info("=" * 70)
        logger.info("📥 Phase 1 : Ingestion Python (SFTP → ODS)")
        logger.info("=" * 70)
        
        sftp_to_ods_complete_flow()
        
        # ===================================================================
        # PHASE 2 : Transformations dbt (ODS → PREP)
        # ===================================================================
        logger.info("=" * 70)
        logger.info("⚙️ Phase 2 : Transformations dbt (ODS → PREP)")
        logger.info("=" * 70)
        
        dbt_result = run_dbt_models(models="prep.*")
        
        # ===================================================================
        # RÉSUMÉ
        # ===================================================================
        total_duration = (datetime.now() - start_time).total_seconds()
        
        logger.info("=" * 70)
        logger.info("✅ PIPELINE COMPLET TERMINÉ")
        logger.info(f"⏱️  Durée totale : {total_duration:.2f}s")
        logger.info(f"📊 Modèles dbt créés : {dbt_result['models_count']}")
        logger.info("=" * 70)
        
        return {
            'success': True,
            'duration_seconds': total_duration,
            'dbt_models': dbt_result['models_count']
        }
        
    except Exception as e:
        logger.error(f"❌ Erreur pipeline : {e}")
        raise


@flow(name="🔄 Pipeline Python seul (sans dbt)", log_prints=True)
def python_pipeline_only():
    """Pipeline Python uniquement : SFTP → RAW → STAGING_ETL → ODS"""
    logger = get_run_logger()
    
    logger.info("🚀 Pipeline Python : SFTP → ODS")
    
    # Import metadata
    try:
        db_metadata_import_flow()
    except:
        pass
    
    # Ingestion
    sftp_to_ods_complete_flow()
    
    logger.info("✅ Pipeline Python terminé")


@flow(name="⚙️ dbt seul (ODS → PREP)", log_prints=True)
def dbt_only():
    """Exécuter uniquement dbt (suppose que ODS est déjà rempli)"""
    logger = get_run_logger()
    
    logger.info("⚙️ Transformation dbt : ODS → PREP")
    
    result = run_dbt_models(models="prep.*")
    
    logger.info(f"✅ dbt terminé : {result['models_count']} modèles")
    
    return result


# ============================================================================
# EXÉCUTION
# ============================================================================

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        mode = sys.argv[1]
        
        if mode == '--python-only':
            python_pipeline_only()
        elif mode == '--dbt-only':
            dbt_only()
        else:
            full_etl_pipeline()
    else:
        # Par défaut : pipeline complet
        full_etl_pipeline()