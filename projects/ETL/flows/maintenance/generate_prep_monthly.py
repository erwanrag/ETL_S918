"""
============================================================================
Flow Prefect : Génération Automatique Modèles PREP
============================================================================
Responsabilité : Régénérer modèles dbt prep mensuellement
- Appelle scripts/generators/generate_prep_models.py
- Compile modèles dbt pour validation
- Alerting intégré Teams/Email
============================================================================
"""

import subprocess
import sys
from pathlib import Path
from datetime import datetime
from prefect import flow, task
from prefect.logging import get_run_logger

sys.path.append(r'E:\Prefect\projects\ETL')
from flows.config.pg_config import config

# Import alerting
from projects.shared.alerting.alert_manager import get_alert_manager, AlertLevel


@task(name="📊 Générer Modèles PREP", retries=2, retry_delay_seconds=60)
def generate_prep_models():
    """
    Exécuter le script Python de génération modèles prep
    
    Returns:
        dict: {
            'success': bool,
            'tables_generated': int,
            'columns_kept': int,
            'columns_excluded': int,
            'stdout': str
        }
    """
    logger = get_run_logger()
    
    script_path = Path(r"E:\Prefect\projects\ETL\scripts\generators\generate_prep_models.py")
    
    if not script_path.exists():
        raise FileNotFoundError(f"Script introuvable : {script_path}")
    
    logger.info("[RUN] Lancement génération modèles prep...")
    
    try:
        # Exécuter script
        result = subprocess.run(
            ["python", str(script_path)],
            capture_output=True,
            text=True,
            cwd=r"E:\Prefect\projects\ETL",
            timeout=600  # 10 min max
        )
        
        if result.stdout:
            logger.info(f"[OUTPUT]\n{result.stdout}")
        
        if result.stderr:
            logger.warning(f"[STDERR]\n{result.stderr}")
        
        if result.returncode != 0:
            raise Exception(f"Script échoué avec code {result.returncode}")
        
        # Parser output pour extraire stats
        stats = parse_output(result.stdout)
        
        logger.info(f"[OK] {stats['tables_generated']} modèles générés")
        logger.info(f"[STATS] {stats['columns_kept']}/{stats['columns_total']} colonnes gardées")
        
        return {
            'success': True,
            'tables_generated': stats['tables_generated'],
            'columns_kept': stats['columns_kept'],
            'columns_excluded': stats['columns_excluded'],
            'reduction_pct': stats['reduction_pct'],
            'stdout': result.stdout
        }
        
    except subprocess.TimeoutExpired:
        logger.error("[ERROR] Timeout génération prep (> 10 min)")
        raise
    except Exception as e:
        logger.error(f"[ERROR] Échec génération : {e}")
        raise


def parse_output(stdout: str) -> dict:
    """
    Parser la sortie du script pour extraire statistiques
    
    Returns:
        dict: {'tables_generated': int, 'columns_kept': int, ...}
    """
    stats = {
        'tables_generated': 0,
        'columns_total': 0,
        'columns_kept': 0,
        'columns_excluded': 0,
        'reduction_pct': 0.0
    }
    
    try:
        lines = stdout.split('\n')
        for line in lines:
            if 'Tables traitées' in line:
                stats['tables_generated'] = int(line.split(':')[1].strip())
            elif 'Colonnes ODS' in line:
                stats['columns_total'] = int(line.split(':')[1].strip())
            elif 'Colonnes PREP' in line:
                stats['columns_kept'] = int(line.split(':')[1].strip())
            elif 'Réduction' in line:
                pct_str = line.split(':')[1].strip().replace('%', '')
                stats['reduction_pct'] = float(pct_str)
        
        stats['columns_excluded'] = stats['columns_total'] - stats['columns_kept']
    except:
        pass  # Parser best-effort
    
    return stats


@task(name="🔨 Compiler Modèles dbt PREP")
def compile_dbt_prep():
    """
    Compiler les modèles dbt prep pour vérifier syntaxe
    
    Returns:
        dict: {'success': bool, 'models_compiled': int}
    """
    logger = get_run_logger()
    
    dbt_project_dir = Path(config.dbt_project_dir)
    
    logger.info("[DBT] Compilation modèles prep...")
    
    try:
        result = subprocess.run(
            ["dbt", "compile", "--models", "prep.*", "--project-dir", str(dbt_project_dir)],
            capture_output=True,
            text=True,
            cwd=str(dbt_project_dir),
            timeout=300  # 5 min max
        )
        
        if result.stdout:
            logger.info(f"[OUTPUT]\n{result.stdout}")
        
        if result.returncode != 0:
            logger.warning("[WARN] Compilation échouée (vérifier logs dbt)")
            return {'success': False, 'models_compiled': 0}
        
        # Compter modèles compilés
        models_count = result.stdout.count('OK compiled')
        
        logger.info(f"[OK] {models_count} modèles prep compilés")
        
        return {'success': True, 'models_compiled': models_count}
        
    except Exception as e:
        logger.error(f"[ERROR] Compilation dbt : {e}")
        return {'success': False, 'error': str(e)}


@task(name="📧 Notifier Génération PREP")
def notify_generation(stats: dict, compile_result: dict):
    """
    Envoyer notification après génération
    
    Args:
        stats: Statistiques génération
        compile_result: Résultat compilation dbt
    """
    logger = get_run_logger()
    
    alert_mgr = get_alert_manager()
    
    if stats['success'] and compile_result['success']:
        alert_mgr.send_alert(
            level=AlertLevel.INFO,
            title="✅ Génération Modèles PREP - SUCCESS",
            message="Modèles prep régénérés avec succès",
            context={
                "Tables": stats['tables_generated'],
                "Colonnes gardées": f"{stats['columns_kept']:,}",
                "Colonnes exclues": f"{stats['columns_excluded']:,}",
                "Réduction": f"{stats['reduction_pct']:.1f}%",
                "Modèles compilés": compile_result['models_compiled'],
                "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
        )
        logger.info("[ALERT] Notification SUCCESS envoyée")
    else:
        alert_mgr.send_alert(
            level=AlertLevel.WARNING,
            title="⚠️ Génération Modèles PREP - PARTIAL",
            message="Génération terminée avec avertissements",
            context={
                "Tables": stats.get('tables_generated', 0),
                "Compilation dbt": "OK" if compile_result['success'] else "ÉCHEC",
                "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
        )
        logger.warning("[ALERT] Notification WARNING envoyée")


@flow(name="[MONTHLY] 📊 Génération Modèles PREP")
def monthly_prep_generation_flow():
    """
    Flow mensuel : Régénérer modèles prep + compiler
    
    Fréquence : 1er dimanche du mois à 5h00
    
    Étapes :
    1. Générer modèles prep (analyse ODS)
    2. Compiler dbt pour vérifier syntaxe
    3. Notifier résultat
    
    Returns:
        dict: Statistiques exécution
    """
    logger = get_run_logger()
    
    logger.info("=" * 70)
    logger.info("[START] Génération mensuelle modèles PREP")
    logger.info(f"[DATE] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 70)
    
    # 1. Générer modèles
    stats = generate_prep_models()
    
    # 2. Compiler dbt
    compile_result = compile_dbt_prep()
    
    # 3. Notifier
    notify_generation(stats, compile_result)
    
    logger.info("=" * 70)
    logger.info("[DONE] Génération terminée")
    logger.info("=" * 70)
    
    return {
        'generation': stats,
        'compilation': compile_result,
        'timestamp': datetime.now().isoformat()
    }


if __name__ == "__main__":
    # Test du flow
    monthly_prep_generation_flow()