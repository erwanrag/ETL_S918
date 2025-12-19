"""
============================================================================
Flow Prefect : ODS → PREP (via dbt)
============================================================================
Responsabilité : Exécuter les modèles dbt prep.*
- Lit depuis ods.*
- Crée tables prep.*
- Tests dbt (optionnel)
- Alerting intégré
============================================================================
"""

import subprocess
from pathlib import Path
from datetime import datetime
from prefect import flow, task
from prefect.logging import get_run_logger
import sys

# Ajouter le chemin racine du projet
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))
from shared.config import config, paths_config

# Import alerting avec le bon chemin
from shared.alerting.alert_manager import get_alert_manager, AlertLevel


@task(name="🔨 Exécuter dbt run", retries=1, retry_delay_seconds=30)
def run_dbt_models(models: str = "prep.*", full_refresh: bool = False):
    """
    Exécuter dbt pour transformer ODS → PREP
    
    Args:
        models: Sélecteur dbt (défaut: "prep.*")
        full_refresh: Forcer full refresh (défaut: False)
    """
    logger = get_run_logger()
    
    dbt_project_dir = paths_config.dbt_project_dir
    
    if not dbt_project_dir.exists():
        raise FileNotFoundError(f"Projet dbt introuvable : {dbt_project_dir}")
    
    cmd = ["dbt", "run", "--models", models, "--project-dir", str(dbt_project_dir)]
    
    if full_refresh:
        cmd.append("--full-refresh")
        logger.info("[MODE] Full refresh activé")
    
    logger.info(f"[CMD] {' '.join(cmd)}")
    
    start_time = datetime.now()
    
    try:
        # Forcer l'encodage UTF-8 pour subprocess
        import os
        env = os.environ.copy()
        env['PYTHONIOENCODING'] = 'utf-8'
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=str(dbt_project_dir),
            timeout=1800,  # 30 min max
            encoding='utf-8',
            errors='replace',  # Remplacer caractères invalides
            env=env
        )
        
        duration = (datetime.now() - start_time).total_seconds()
        
        if result.stdout:
            logger.info(f"[OUTPUT]\n{result.stdout}")
        
        if result.stderr:
            logger.warning(f"[STDERR]\n{result.stderr}")
        
        if result.returncode != 0:
            raise Exception(f"dbt run failed (code {result.returncode})")
        
        # Compter modèles créés
        models_count = result.stdout.count('OK created') if result.stdout else 0
        logger.info(f"[OK] {models_count} modèle(s) dbt créé(s) en {duration:.2f}s")
        
        return {
            'success': True,
            'models_count': models_count,
            'duration_seconds': duration,
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
        dict: {
            'success': bool,
            'tests_passed': int,
            'tests_failed': int,
            'stdout': str
        }
    """
    logger = get_run_logger()
    
    dbt_project_dir = Path(config.dbt_project_dir)
    
    cmd = ["dbt", "test", "--models", models, "--project-dir", str(dbt_project_dir)]
    
    logger.info(f"[CMD] {' '.join(cmd)}")
    
    try:
        # Forcer l'encodage UTF-8
        import os
        env = os.environ.copy()
        env['PYTHONIOENCODING'] = 'utf-8'
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=str(dbt_project_dir),
            timeout=900,  # 15 min max
            encoding='utf-8',
            errors='replace',
            env=env
        )
        
        if result.stdout:
            logger.info(f"[OUTPUT]\n{result.stdout}")
        
        # Parser résultats tests
        tests_passed = result.stdout.count('PASS') if result.stdout else 0
        tests_failed = result.stdout.count('FAIL') if result.stdout else 0
        
        tests_success = result.returncode == 0
        
        if tests_success:
            logger.info(f"[OK] Tous les tests dbt passés ({tests_passed} tests)")
        else:
            logger.warning(f"[WARN] {tests_failed} test(s) dbt échoué(s)")
        
        return {
            'success': tests_success,
            'tests_passed': tests_passed,
            'tests_failed': tests_failed,
            'stdout': result.stdout
        }
        
    except Exception as e:
        logger.warning(f"[WARN] Erreur dbt test : {e}")
        return {
            'success': False,
            'tests_passed': 0,
            'tests_failed': 0,
            'error': str(e)
        }


@flow(name="[04] 🔨 ODS → PREP (dbt transformations)")
def ods_to_prep_flow(
    models: str = "prep.*",
    run_tests: bool = False,
    send_alerts: bool = True, 
    full_refresh: bool = False 
):
    """
    Flow de transformation dbt : ODS → PREP
    
    Args:
        models: Sélecteur dbt (défaut: "prep.*")
        run_tests: Exécuter les tests dbt (défaut: False)
        send_alerts: Envoyer alertes Teams/Email (défaut: True)
    
    Étapes :
    1. dbt run --models prep.*
    2. dbt test --models prep.* (si run_tests=True)
    3. Alerting (si send_alerts=True)
    
    Returns:
        dict: Statistiques d'exécution
    """
    logger = get_run_logger()
    
    start_time = datetime.now()
    run_id = f"ods_to_prep_{start_time.strftime('%Y%m%d_%H%M%S')}"
    
    logger.info("=" * 70)
    logger.info("[START] ODS → PREP Transformation")
    logger.info(f"[RUN] {run_id}")
    logger.info(f"[MODELS] {models}")
    logger.info(f"[TESTS] {'ON' if run_tests else 'OFF'}")
    logger.info("=" * 70)
    
    results = {
        'run_id': run_id,
        'start_time': start_time.isoformat(),
        'models': models,
        'models_count': 0,
        'tests_passed': None,
        'tests_failed': None,
        'success': False,
        'errors': []
    }
    
    try:
        # 1. dbt run
        logger.info("[DBT] Exécution modèles...")
        run_result = run_dbt_models(models=models, full_refresh=full_refresh)
        results['models_count'] = run_result['models_count']
        results['dbt_duration'] = run_result['duration_seconds']
        
        # 2. dbt test (optionnel)
        if run_tests:
            logger.info("[DBT] Exécution tests...")
            test_result = run_dbt_tests(models=models)
            results['tests_passed'] = test_result['tests_passed']
            results['tests_failed'] = test_result['tests_failed']
            results['tests_success'] = test_result['success']
        
        # Résumé
        end_time = datetime.now()
        results['end_time'] = end_time.isoformat()
        results['duration_seconds'] = (end_time - start_time).total_seconds()
        results['success'] = True
        
        logger.info("=" * 70)
        logger.info("[DONE] Transformation terminée")
        logger.info(f"[TIMER] {results['duration_seconds']:.2f}s")
        logger.info(f"[MODELS] {results['models_count']} modèle(s)")
        if run_tests:
            logger.info(f"[TESTS] {results['tests_passed']} passed, {results['tests_failed']} failed")
        logger.info("=" * 70)
        
        # Alerting
        if send_alerts:
            alert_mgr = get_alert_manager()
            
            if results['success'] and (not run_tests or results.get('tests_success', True)):
                alert_mgr.send_alert(
                    level=AlertLevel.INFO,
                    title="✅ ODS → PREP Transformation - SUCCESS",
                    message="Modèles dbt prep créés avec succès",
                    context={
                        "Run ID": run_id,
                        "Modèles": results['models_count'],
                        "Duration": f"{results['duration_seconds']:.2f}s",
                        "Tests": f"{results['tests_passed']} passed" if run_tests else "Skipped",
                        "Timestamp": end_time.strftime("%Y-%m-%d %H:%M:%S")
                    }
                )
            elif results.get('tests_failed', 0) > 0:
                alert_mgr.send_alert(
                    level=AlertLevel.WARNING,
                    title="⚠️ ODS → PREP Transformation - TESTS FAILED",
                    message=f"{results['tests_failed']} test(s) dbt échoué(s)",
                    context={
                        "Run ID": run_id,
                        "Modèles": results['models_count'],
                        "Tests Passed": results['tests_passed'],
                        "Tests Failed": results['tests_failed'],
                        "Timestamp": end_time.strftime("%Y-%m-%d %H:%M:%S")
                    }
                )
        
        return results
        
    except Exception as e:
        logger.error(f"[ERROR] Échec transformation : {e}")
        
        end_time = datetime.now()
        results['end_time'] = end_time.isoformat()
        results['duration_seconds'] = (end_time - start_time).total_seconds()
        results['success'] = False
        results['errors'].append(str(e))
        
        # Alerting erreur critique
        if send_alerts:
            alert_mgr = get_alert_manager()
            alert_mgr.send_alert(
                level=AlertLevel.CRITICAL,
                title="🚨 ODS → PREP Transformation - CRITICAL",
                message="Erreur critique lors de la transformation dbt",
                context={
                    "Run ID": run_id,
                    "Error": str(e),
                    "Duration": f"{results['duration_seconds']:.2f}s",
                    "Timestamp": end_time.strftime("%Y-%m-%d %H:%M:%S")
                }
            )
        
        raise


if __name__ == "__main__":
    # Test du flow
    ods_to_prep_flow(run_tests=False)