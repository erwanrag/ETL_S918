"""
============================================================================
Flow Prefect : STAGING_ETL -> ODS_DBT (via dbt SCD2)
============================================================================
⚠️  FLOW DE TEST - Ne touche PAS à la production (ods schema)

Responsabilité : Transformation STAGING → ODS_DBT avec SCD2 complet
- Génération modèles dbt dynamiques
- Exécution dbt run --models ods.*
- Support FULL/INCREMENTAL/FULL_RESET via var load_mode
- Schéma cible : ods_dbt (pas ods)
============================================================================
"""

import subprocess
from pathlib import Path
from datetime import datetime
from prefect import flow, task
from prefect.logging import get_run_logger
import sys
import psycopg2

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from shared.config import config
from tasks.ods_generator_dbt import ODSGeneratorDBT


@task(name="[GEN-DBT] Générer modèles dbt ODS")
def generate_ods_dbt_models(table_names=None):
    """
    Générer modèles dbt ODS depuis metadata
    
    Génère :
    - models/ods/ods_*.sql
    - models/ods/sources.yml
    - models/ods/schema.yml
    """
    logger = get_run_logger()
    
    generator = ODSGeneratorDBT()
    
    if table_names:
        logger.info(f"[GEN] Génération de {len(table_names)} modèles")
        for table in table_names:
            try:
                generator.generate_sql_model(table)
            except Exception as e:
                logger.error(f"[ERROR] {table}: {e}")
        
        # Régénérer sources.yml et schema.yml avec toutes les tables
        all_tables = generator.get_all_active_tables()
        generator.generate_sources_yml(all_tables)
        generator.generate_schema_yml(all_tables)
    else:
        logger.info("[GEN] Génération de tous les modèles ODS")
        generator.generate_all()
    
    logger.info("[OK] Modèles dbt générés")


@task(name="[DBT] Compiler modèles ods.*")
def compile_dbt_ods():
    """Compiler modèles dbt pour vérification syntaxe"""
    logger = get_run_logger()
    
    dbt_project_dir = Path(__file__).parent.parent.parent / 'dbt' / 'etl_db'
    
    cmd = [
        "dbt", "compile",
        "--models", "ods.*",
        "--project-dir", str(dbt_project_dir)
    ]
    
    logger.info(f"[CMD] {' '.join(cmd)}")
    
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        cwd=str(dbt_project_dir),
        timeout=300
    )
    
    if result.returncode != 0:
        logger.error(f"[ERROR] Compilation failed\n{result.stderr}")
        raise Exception("dbt compile failed")
    
    logger.info("[OK] Compilation réussie")
    
    return True


@task(name="[DBT] Exécuter dbt run ods.*")
def run_dbt_ods(load_mode: str = "INCREMENTAL", models: str = "ods.*"):
    """
    Exécuter dbt pour STAGING → ODS_DBT
    
    Args:
        load_mode: FULL | INCREMENTAL | FULL_RESET
        models: Sélecteur dbt (défaut: ods.*)
    """
    logger = get_run_logger()
    
    dbt_project_dir = Path(__file__).parent.parent.parent / 'dbt' / 'etl_db'
    
    cmd = [
        "dbt", "run",
        "--models", models,
        "--project-dir", str(dbt_project_dir),
        "--vars", f'{{"load_mode": "{load_mode}"}}'
    ]
    
    logger.info(f"[CMD] {' '.join(cmd)}")
    logger.info(f"[MODE] {load_mode}")
    logger.info(f"[TARGET] Schéma ods_dbt")
    
    start_time = datetime.now()
    
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        cwd=str(dbt_project_dir),
        timeout=1800,
        encoding='utf-8',
        errors='replace'
    )
    
    duration = (datetime.now() - start_time).total_seconds()
    
    if result.stdout:
        logger.info(f"[OUTPUT]\n{result.stdout}")
    
    if result.returncode != 0:
        logger.error(f"[ERROR]\n{result.stderr}")
        raise Exception(f"dbt run failed (code {result.returncode})")
    
    # Compter modèles créés
    models_count = result.stdout.count('OK created') if result.stdout else 0
    models_count += result.stdout.count('OK updated') if result.stdout else 0
    
    logger.info(f"[OK] {models_count} table(s) ODS_DBT traitées en {duration:.2f}s")
    
    return {
        'models_count': models_count,
        'load_mode': load_mode,
        'duration_seconds': duration
    }


@task(name="[CHECK] Vérifier ODS_DBT")
def verify_ods_dbt(table_names=None):
    """Vérifier volumétrie et intégrité ODS_DBT"""
    logger = get_run_logger()
    
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    try:
        # Récupérer tables ODS_DBT
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables
            WHERE table_schema = 'ods_dbt'
              AND table_name LIKE 'ods_%'
            ORDER BY table_name
        """)
        
        tables = [row[0] for row in cur.fetchall()]
        
        if not tables:
            logger.warning("[WARN] Aucune table dans ods_dbt")
            return {'tables_count': 0}
        
        logger.info(f"[CHECK] {len(tables)} tables trouvées dans ods_dbt")
        
        total_rows = 0
        total_current = 0
        total_deleted = 0
        
        for table in tables:
            try:
                cur.execute(f"""
                    SELECT 
                        COUNT(*) AS total,
                        COUNT(*) FILTER (WHERE _etl_is_current = TRUE) AS current,
                        COUNT(*) FILTER (WHERE _etl_is_deleted = TRUE) AS deleted
                    FROM ods_dbt.{table}
                """)
                
                total, current, deleted = cur.fetchone()
                total_rows += total
                total_current += current
                total_deleted += deleted
                
                logger.info(f"  {table}: {total:,} rows ({current:,} current, {deleted:,} deleted)")
            
            except Exception as e:
                logger.warning(f"  [SKIP] {table}: {e}")
        
        logger.info("=" * 70)
        logger.info(f"[TOTAL] {total_rows:,} rows")
        logger.info(f"  Current : {total_current:,}")
        logger.info(f"  Deleted : {total_deleted:,}")
        logger.info("=" * 70)
        
        return {
            'tables_count': len(tables),
            'total_rows': total_rows,
            'current_rows': total_current,
            'deleted_rows': total_deleted
        }
    
    finally:
        cur.close()
        conn.close()


@flow(name="[03-DBT] 💾 STAGING to ODS_DBT (dbt SCD2 - TEST)")
def staging_to_ods_dbt_flow(
    table_names=None,
    load_mode: str = "AUTO",
    run_id=None,
    compile_only: bool = False
):
    """
    Flow de transformation STAGING → ODS_DBT avec SCD2
    
    ⚠️  FLOW DE TEST - Schéma cible : ods_dbt (pas ods)
    
    Args:
        table_names: Liste tables à traiter (None = toutes)
        load_mode: 'FULL' | 'INCREMENTAL' | 'AUTO'
        run_id: ID du run
        compile_only: Si True, compile sans exécuter
    
    Étapes:
    1. Générer modèles dbt ODS
    2. Compiler (vérification syntaxe)
    3. Exécuter dbt run --models ods.*
    4. Validation volumétrie
    """
    logger = get_run_logger()
    
    if run_id is None:
        run_id = f"staging_to_ods_dbt_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    logger.info("=" * 70)
    logger.info("[START] STAGING → ODS_DBT (dbt SCD2 - TEST)")
    logger.info(f"[RUN] {run_id}")
    logger.info(f"[MODE] {load_mode}")
    logger.info(f"[TARGET] Schéma ods_dbt (TEST)")
    logger.info("=" * 70)
    
    results = {
        'run_id': run_id,
        'start_time': datetime.now().isoformat(),
        'load_mode': load_mode,
        'target_schema': 'ods_dbt',
        'tables_generated': 0,
        'tables_processed': 0,
        'total_rows': 0
    }
    
    try:
        # 1. Générer modèles dbt
        logger.info("[STEP 1/4] Génération modèles dbt...")
        generate_ods_dbt_models(table_names)
        results['tables_generated'] = len(table_names) if table_names else 'all'
        
        # 2. Compiler
        logger.info("[STEP 2/4] Compilation dbt...")
        compile_dbt_ods()
        
        if compile_only:
            logger.info("[MODE] Compile-only : fin du flow")
            return results
        
        # 3. Exécuter dbt run
        logger.info("[STEP 3/4] Exécution dbt run...")
        run_result = run_dbt_ods(load_mode)
        results['tables_processed'] = run_result['models_count']
        results['duration_dbt'] = run_result['duration_seconds']
        
        # 4. Vérification
        logger.info("[STEP 4/4] Vérification ODS_DBT...")
        verify_result = verify_ods_dbt(table_names)
        results['total_rows'] = verify_result['total_rows']
        results['current_rows'] = verify_result['current_rows']
        results['deleted_rows'] = verify_result['deleted_rows']
        
    except Exception as e:
        logger.error(f"[ERROR] {e}")
        results['error'] = str(e)
        raise
    
    finally:
        results['end_time'] = datetime.now().isoformat()
    
    logger.info("=" * 70)
    logger.info("[OK] STAGING → ODS_DBT terminé")
    logger.info(f"  Tables : {results['tables_processed']}")
    logger.info(f"  Rows   : {results.get('total_rows', 0):,}")
    logger.info(f"  Current: {results.get('current_rows', 0):,}")
    logger.info(f"  Deleted: {results.get('deleted_rows', 0):,}")
    logger.info("=" * 70)
    
    return results


if __name__ == "__main__":
    # Test local
    staging_to_ods_dbt_flow(
        table_names=['histolig'],
        load_mode='INCREMENTAL'
    )