"""
============================================================================
Flow Prefect : ODS → PREP (via dbt)
============================================================================
Responsabilité : Exécuter les modèles dbt prep.*
- Lit depuis ods.*
- Crée tables prep.*
- Tests dbt (optionnel)
============================================================================
"""

import subprocess
from pathlib import Path
from prefect import flow, task
from prefect.logging import get_run_logger
import sys

sys.path.append(r'E:\Prefect\projects\ETL')
from flows.config.pg_config import config


@task(name="🔨 Exécuter dbt run")
def run_dbt_models(models: str = "prep.*"):
    """
    Exécuter dbt pour transformer ODS → PREP
    
    Args:
        models: Sélecteur dbt (défaut: "prep.*")
    
    Returns:
        dict: {'success': bool, 'models_count': int, 'stdout': str}
    """
    logger = get_run_logger()
    
    dbt_project_dir = Path(config.dbt_project_dir)
    
    if not dbt_project_dir.exists():
        raise FileNotFoundError(f"Projet dbt introuvable : {dbt_project_dir}")
    
    cmd = ["dbt", "run", "--models", models, "--project-dir", str(dbt_project_dir)]
    
    logger.info(f"🔨 Commande : {' '.join(cmd)}")
    
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=str(dbt_project_dir),
            timeout=1800  # 30 min max
        )
        
        if result.stdout:
            logger.info(f"[FILE] dbt output:\n{result.stdout}")
        
        if result.stderr:
            logger.warning(f"[WARN] dbt warnings:\n{result.stderr}")
        
        if result.returncode != 0:
            raise Exception(f"dbt run failed (code {result.returncode})")
        
        # Compter modèles créés
        models_count = result.stdout.count('OK created') if result.stdout else 0
        logger.info(f"[OK] {models_count} modèle(s) dbt créé(s)")
        
        return {
            'success': True, 
            'models_count': models_count,
            'stdout': result.stdout
        }
        
    except subprocess.TimeoutExpired:
        logger.error("[ERROR] dbt timeout (> 30 min)")
        raise
    except Exception as e:
        logger.error(f"[ERROR] Erreur dbt : {e}")
        raise


@task(name="🧪 Exécuter dbt test")
def run_dbt_tests(models: str = "prep.*"):
    """
    Exécuter les tests dbt sur les modèles PREP
    
    Args:
        models: Sélecteur dbt (défaut: "prep.*")
    
    Returns:
        dict: {'success': bool, 'stdout': str}
    """
    logger = get_run_logger()
    
    dbt_project_dir = Path(config.dbt_project_dir)
    
    cmd = ["dbt", "test", "--models", models, "--project-dir", str(dbt_project_dir)]
    
    logger.info(f"🧪 Commande : {' '.join(cmd)}")
    
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=str(dbt_project_dir),
            timeout=900  # 15 min max
        )
        
        if result.stdout:
            logger.info(f"[FILE] dbt test output:\n{result.stdout}")
        
        tests_passed = result.returncode == 0
        
        if tests_passed:
            logger.info("[OK] Tous les tests dbt passés")
        else:
            logger.warning("[WARN] Certains tests dbt échoués")
        
        return {
            'success': tests_passed,
            'stdout': result.stdout
        }
        
    except Exception as e:
        logger.warning(f"[WARN] Erreur dbt test : {e}")
        return {'success': False, 'error': str(e)}


@flow(name="[SETTINGS] ODS → PREP (dbt transformations)")
def ods_to_prep_flow(
    models: str = "prep.*",
    run_tests: bool = False
):
    """
    Flow de transformation dbt : ODS → PREP
    
    Args:
        models: Sélecteur dbt (défaut: "prep.*")
        run_tests: Exécuter les tests dbt (défaut: False)
    
    Étapes :
    1. dbt run --models prep.*
    2. dbt test --models prep.* (si run_tests=True)
    
    Returns:
        dict: Statistiques d'exécution
    """
    logger = get_run_logger()
    
    logger.info(f"[SETTINGS] Transformation dbt : {models}")
    
    # 1. dbt run
    run_result = run_dbt_models(models=models)
    models_count = run_result['models_count']
    
    # 2. dbt test (optionnel)
    tests_passed = None
    if run_tests:
        test_result = run_dbt_tests(models=models)
        tests_passed = test_result['success']
    
    logger.info(f"[OK] dbt terminé : {models_count} modèle(s)")
    
    return {
        "models_count": models_count,
        "tests_passed": tests_passed
    }


if __name__ == "__main__":
    # Test du flow
    ods_to_prep_flow(run_tests=False)