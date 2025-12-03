"""
============================================================================
Flow Prefect : Exécuter dbt (RAW → ODS)
============================================================================
Fichier : E:\Prefect\postgresql\transformations\dbt_runner.py
Objectif : Lancer dbt pour transformer raw → staging → ods
============================================================================
"""

import subprocess
from pathlib import Path
from datetime import datetime
from prefect import flow, task
from prefect.logging import get_run_logger
import psycopg2
from psycopg2.extras import Json
import sys

# Ajouter le chemin du config
sys.path.append(r'E:\Prefect\projects/ETL')
from flows.config.pg_config import config

# ============================================================================
# TASKS
# ============================================================================

@task(name="🔨 Exécuter dbt run", retries=1)
def run_dbt_models(models: str = None):
    """
    Exécuter les modèles dbt
    
    Args:
        models: Modèles spécifiques à exécuter (ex: "staging.*" ou None pour tous)
    """
    logger = get_run_logger()
    
    dbt_project_dir = Path(config.dbt_project_dir)
    
    if not dbt_project_dir.exists():
        raise FileNotFoundError(f"Projet dbt introuvable : {dbt_project_dir}")
    
    # Construire la commande dbt
    cmd = ["dbt", "run", "--project-dir", str(dbt_project_dir)]
    
    if models:
        cmd.extend(["--models", models])
    
    logger.info(f"🔨 Commande dbt : {' '.join(cmd)}")
    
    try:
        # Exécuter dbt
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=str(dbt_project_dir),
            timeout=3600  # 1 heure max
        )
        
        # Logger la sortie
        if result.stdout:
            logger.info(f"[FILE] Output dbt:\n{result.stdout}")
        
        if result.stderr:
            logger.warning(f"[WARN] Warnings dbt:\n{result.stderr}")
        
        if result.returncode != 0:
            raise Exception(f"dbt run failed with return code {result.returncode}")
        
        logger.info("[OK] dbt run terminé avec succès")
        
        return {
            'success': True,
            'stdout': result.stdout,
            'stderr': result.stderr
        }
        
    except subprocess.TimeoutExpired:
        logger.error("[ERROR] dbt run timeout (> 1 heure)")
        raise
    except Exception as e:
        logger.error(f"[ERROR] Erreur dbt run : {e}")
        raise


@task(name="🧪 Exécuter dbt test")
def run_dbt_tests(models: str = None):
    """
    Exécuter les tests dbt
    
    Args:
        models: Modèles à tester (ex: "staging.*" ou None pour tous)
    """
    logger = get_run_logger()
    
    dbt_project_dir = Path(config.dbt_project_dir)
    
    # Construire la commande
    cmd = ["dbt", "test", "--project-dir", str(dbt_project_dir)]
    
    if models:
        cmd.extend(["--models", models])
    
    logger.info(f"🧪 Commande dbt test : {' '.join(cmd)}")
    
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=str(dbt_project_dir),
            timeout=1800  # 30 minutes max
        )
        
        if result.stdout:
            logger.info(f"[FILE] Output dbt test:\n{result.stdout}")
        
        if result.stderr:
            logger.warning(f"[WARN] Warnings dbt test:\n{result.stderr}")
        
        # Les tests peuvent échouer sans que ce soit une erreur critique
        if result.returncode != 0:
            logger.warning(f"[WARN] Certains tests dbt ont échoué (code {result.returncode})")
        else:
            logger.info("[OK] Tous les tests dbt sont passés")
        
        return {
            'success': result.returncode == 0,
            'stdout': result.stdout,
            'stderr': result.stderr
        }
        
    except Exception as e:
        logger.error(f"[ERROR] Erreur dbt test : {e}")
        # On ne raise pas pour ne pas bloquer le flow
        return {'success': False, 'error': str(e)}


@task(name="[DATA] Générer documentation dbt")
def generate_dbt_docs():
    """Générer la documentation dbt"""
    logger = get_run_logger()
    
    dbt_project_dir = Path(config.dbt_project_dir)
    
    try:
        # dbt docs generate
        result = subprocess.run(
            ["dbt", "docs", "generate", "--project-dir", str(dbt_project_dir)],
            capture_output=True,
            text=True,
            cwd=str(dbt_project_dir),
            timeout=300
        )
        
        if result.returncode == 0:
            logger.info("[OK] Documentation dbt générée")
            logger.info(f"📚 Pour voir : dbt docs serve (dans {dbt_project_dir})")
        else:
            logger.warning(f"[WARN] Erreur génération docs : {result.stderr}")
        
        return result.returncode == 0
        
    except Exception as e:
        logger.warning(f"[WARN] Impossible de générer la doc : {e}")
        return False


@task(name="[NOTE] Logger transformation dans ETL logs")
def log_dbt_run(status: str, models_run: int = 0, tests_passed: bool = None, error_message: str = None):
    """Logger l'exécution dbt dans etl_logs.etl_run_log"""
    logger = get_run_logger()
    
    metadata = {
        'dbt_project': config.dbt_project_dir,
        'models_run': models_run,
        'tests_passed': tests_passed
    }
    
    try:
        conn = psycopg2.connect(config.get_connection_string())
        cur = conn.cursor()
        
        cur.execute("""
            INSERT INTO etl_logs.etl_run_log 
            (flow_name, task_name, run_status, rows_processed, error_message, metadata, completed_at)
            VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        """, ('dbt_transformation_flow', 'dbt_run', status, models_run, error_message, Json(metadata)))
        
        conn.commit()
        cur.close()
        conn.close()
        
        logger.info(f"[NOTE] Exécution dbt loggée : {status}")
        
    except Exception as e:
        logger.warning(f"[WARN] Erreur log ETL : {e}")


@task(name="[STATS] Mettre à jour métadonnées tables")
def update_table_metadata():
    """Mettre à jour les métadonnées des tables dans etl_logs.etl_table_metadata"""
    logger = get_run_logger()
    
    try:
        conn = psycopg2.connect(config.get_connection_string())
        cur = conn.cursor()
        
        # Mettre à jour les row counts pour les tables ODS
        cur.execute("""
            WITH table_stats AS (
                SELECT 
                    schemaname,
                    tablename,
                    n_live_tup as row_count
                FROM pg_stat_user_tables
                WHERE schemaname IN ('raw', 'staging', 'ods', 'marts')
            )
            INSERT INTO etl_logs.etl_table_metadata 
                (schema_name, table_name, row_count, last_load_timestamp, updated_at)
            SELECT 
                schemaname,
                tablename,
                row_count,
                CURRENT_TIMESTAMP,
                CURRENT_TIMESTAMP
            FROM table_stats
            ON CONFLICT (schema_name, table_name) 
            DO UPDATE SET
                row_count = EXCLUDED.row_count,
                last_load_timestamp = EXCLUDED.last_load_timestamp,
                updated_at = CURRENT_TIMESTAMP
        """)
        
        conn.commit()
        rows_updated = cur.rowcount
        
        logger.info(f"[DATA] {rows_updated} table(s) metadata mise(s) à jour")
        
        cur.close()
        conn.close()
        
        return rows_updated
        
    except Exception as e:
        logger.warning(f"[WARN] Erreur mise à jour metadata : {e}")
        if conn:
            conn.rollback()
            conn.close()
        return 0


# ============================================================================
# FLOW PRINCIPAL
# ============================================================================

@flow(name="[SETTINGS] Transformation dbt (RAW → ODS)", log_prints=True)
def dbt_transformation_flow(models: str = None, run_tests: bool = True, generate_docs: bool = False):
    """
    Flow principal de transformation dbt
    
    Args:
        models: Modèles spécifiques à exécuter (None = tous)
        run_tests: Exécuter les tests dbt après le run
        generate_docs: Générer la documentation dbt
    
    Étapes:
    1. dbt run (transformations SQL)
    2. dbt test (validation qualité)
    3. Mise à jour métadonnées
    4. Génération doc (optionnel)
    """
    logger = get_run_logger()
    
    logger.info("=" * 60)
    logger.info("[START] Démarrage transformation dbt")
    logger.info("=" * 60)
    
    error_occurred = False
    
    try:
        # 1. Exécuter dbt run
        logger.info("🔨 Étape 1/4 : dbt run")
        run_result = run_dbt_models(models=models)
        
        # Compter les modèles exécutés (parsing du output dbt)
        models_count = run_result['stdout'].count('OK created') if run_result['stdout'] else 0
        logger.info(f"[OK] {models_count} modèle(s) dbt exécuté(s)")
        
        # 2. Exécuter dbt test (si demandé)
        tests_passed = None
        if run_tests:
            logger.info("🧪 Étape 2/4 : dbt test")
            test_result = run_dbt_tests(models=models)
            tests_passed = test_result['success']
            
            if tests_passed:
                logger.info("[OK] Tous les tests passés")
            else:
                logger.warning("[WARN] Certains tests ont échoué")
        else:
            logger.info("[SKIP] Étape 2/4 : Tests ignorés")
        
        # 3. Mettre à jour les métadonnées
        logger.info("[DATA] Étape 3/4 : Mise à jour métadonnées")
        tables_updated = update_table_metadata()
        
        # 4. Générer la documentation (si demandé)
        if generate_docs:
            logger.info("📚 Étape 4/4 : Génération documentation")
            generate_dbt_docs()
        else:
            logger.info("[SKIP] Étape 4/4 : Documentation ignorée")
        
        # Logger l'exécution
        status = "SUCCESS" if not error_occurred else "FAILED"
        log_dbt_run(status, models_count, tests_passed)
        
        logger.info("=" * 60)
        logger.info(f"[OK] Transformation terminée : {models_count} modèle(s), {tables_updated} table(s) metadata")
        logger.info("=" * 60)
        
        return {
            'models_run': models_count,
            'tests_passed': tests_passed,
            'tables_metadata_updated': tables_updated
        }
        
    except Exception as e:
        logger.error(f"[ERROR] Erreur flow dbt : {e}")
        log_dbt_run("FAILED", 0, None, str(e))
        raise


# ============================================================================
# FLOWS COMBINÉS
# ============================================================================

@flow(name="[SYNC] Pipeline Complet : SFTP → RAW → ODS", log_prints=True)
def full_etl_pipeline():
    """
    Pipeline ETL complet
    
    1. Ingestion SFTP → RAW
    2. Transformation dbt RAW → ODS
    """
    logger = get_run_logger()
    
    logger.info("=" * 60)
    logger.info("[START] PIPELINE ETL COMPLET")
    logger.info("=" * 60)
    
    # Import du flow d'ingestion
    from ingestion.sftp_to_raw import sftp_to_raw_flow
    
    # 1. Ingestion
    logger.info("📥 Phase 1 : Ingestion SFTP → RAW")
    sftp_to_raw_flow()
    
    # 2. Transformation
    logger.info("[SETTINGS] Phase 2 : Transformation dbt RAW → ODS")
    dbt_transformation_flow(run_tests=True, generate_docs=False)
    
    logger.info("=" * 60)
    logger.info("[OK] PIPELINE COMPLET TERMINÉ")
    logger.info("=" * 60)


# ============================================================================
# EXÉCUTION
# ============================================================================

if __name__ == "__main__":
    # Test du flow
    dbt_transformation_flow(run_tests=True, generate_docs=True)