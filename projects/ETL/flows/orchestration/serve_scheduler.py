"""
Scheduler Prefect 3.x avec Health Checks
"""

import sys
from pathlib import Path
from datetime import timedelta
import psycopg2
import shutil

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

import logging.config
import yaml

LOG_FILE = ROOT / "prefect_logging.yml"
if LOG_FILE.exists():
    with open(LOG_FILE, 'r') as f:
        log_config = yaml.safe_load(f)
        logging.config.dictConfig(log_config)

from prefect import serve, task
from prefect.client.schemas.schedules import CronSchedule, IntervalSchedule

from flows.orchestration.full_pipeline import full_etl_pipeline
from shared.config import config, paths_config


@task(name="🏥 Health Check")
def pipeline_health_check():
    """Health check PostgreSQL + SFTP + Disk"""
    checks = {}
    
    # 1. PostgreSQL
    try:
        conn = psycopg2.connect(config.get_connection_string())
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.close()
        conn.close()
        checks['postgres'] = True
    except:
        checks['postgres'] = False
    
    # 2. SFTP mount
    sftp_path = Path(config.sftp_parquet_dir)
    checks['sftp'] = sftp_path.exists()
    
    # 3. Disk space (minimum 10 GB)
    try:
        stats = shutil.disk_usage(paths_config.etl_root or "/data/prefect")
        free_gb = stats.free / (1024**3)
        checks['disk_space'] = free_gb > 10
    except:
        checks['disk_space'] = False
    
    return all(checks.values()), checks


DEPLOYMENT_CONFIGS = {
    "production": {
        "schedule": CronSchedule(cron="0 * * * *", timezone="Europe/Paris"),
        "description": "Production : toutes les heures",
        "name": "etl-production-hourly",
        "tags": ["production", "hourly"]
    },
    "frequent": {
        "schedule": IntervalSchedule(interval=timedelta(hours=4)),
        "description": "Fréquent : toutes les 4 heures",
        "name": "etl-frequent-4h",
        "tags": ["frequent", "4h"]
    },
    "test": {
        "schedule": IntervalSchedule(interval=timedelta(hours=1)),
        "description": "Test : toutes les heures",
        "name": "etl-test-1h",
        "tags": ["test", "hourly"]
    },
    "every-15min": {
        "schedule": IntervalSchedule(interval=timedelta(minutes=15)),
        "description": "Test rapide : toutes les 15 minutes",
        "name": "etl-test-15min",
        "tags": ["test", "rapid"]
    },
    "manual-only": {
        "schedule": None,
        "description": "Pas de schedule - Manuel uniquement",
        "name": "etl-manual",
        "tags": ["manual"]
    }
}


def start_scheduler(config_name: str = "production"):
    """Démarrer scheduler avec health check"""
    
    if config_name not in DEPLOYMENT_CONFIGS:
        print(f"\n[ERROR] Config '{config_name}' inconnue")
        list_configs()
        return
    
    cfg = DEPLOYMENT_CONFIGS[config_name]
    
    print("\n" + "=" * 70)
    print(f"[START] SCHEDULER ETL - {config_name}")
    print("=" * 70)
    
    # Health check initial
    print("\n🏥 Health Check...")
    try:
        is_healthy, checks = pipeline_health_check()
        if not is_healthy:
            print(f"[ERROR] Health check échoué : {checks}")
            print("[ABORT] Impossible de démarrer")
            return
        print(f"[OK] Health check passé : {checks}")
    except Exception as e:
        print(f"[WARN] Health check error : {e}")
    
    print(f"\n{cfg['description']}")
    print(f"Nom : {cfg['name']}")
    print(f"Tags : {', '.join(cfg['tags'])}")
    
    if cfg['schedule']:
        if isinstance(cfg['schedule'], CronSchedule):
            print(f"CRON : {cfg['schedule'].cron} ({cfg['schedule'].timezone})")
        else:
            interval = cfg['schedule'].interval
            if interval.total_seconds() < 3600:
                minutes = int(interval.total_seconds() / 60)
                print(f"Interval : {minutes} min")
            else:
                hours = int(interval.total_seconds() / 3600)
                print(f"Interval : {hours}h")
    else:
        print("Mode : Manuel uniquement")
    
    print(f"\n⚠️  LAISSER CETTE FENÊTRE OUVERTE")
    print(f"🌐 UI : http://127.0.0.1:4200")
    print("=" * 70 + "\n")
    
    default_params = {
        "import_metadata": False,
        "run_dbt": False,
        "enable_parallel": True
    }
    
    deployments = [
        full_etl_pipeline.to_deployment(
            name=cfg['name'],
            tags=cfg['tags'],
            description=cfg['description'],
            parameters=default_params,
            schedule=cfg['schedule']
        )
    ]

    try:
        print("🚀 Serveur démarré\n")
        serve(*deployments)
    except KeyboardInterrupt:
        print("\n[STOP] Arrêt (Ctrl+C)\n")
    except Exception as e:
        print(f"\n[ERROR] {e}\n")
        raise


def list_configs():
    """Lister configs"""
    print("\n[LIST] CONFIGURATIONS")
    print("=" * 70)
    
    for name, cfg in DEPLOYMENT_CONFIGS.items():
        print(f"\n🔹 {name}")
        print(f"   {cfg['description']}")
        
        if cfg['schedule']:
            if isinstance(cfg['schedule'], CronSchedule):
                print(f"   CRON: {cfg['schedule'].cron}")
            else:
                interval = cfg['schedule'].interval
                if interval.total_seconds() < 3600:
                    minutes = int(interval.total_seconds() / 60)
                    print(f"   Interval: {minutes} min")
                else:
                    hours = int(interval.total_seconds() / 3600)
                    print(f"   Interval: {hours}h")
        else:
            print(f"   Mode: Manuel")
        
        print(f"   Tags: {', '.join(cfg['tags'])}")
    
    print("\n" + "=" * 70)
    print("\nUSAGE:")
    print("  python serve_scheduler.py --config production")
    print("  python serve_scheduler.py --list")
    print("=" * 70 + "\n")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Scheduler ETL Prefect 3.x")
    parser.add_argument("--config", choices=list(DEPLOYMENT_CONFIGS.keys()), default="production")
    parser.add_argument("--list", action="store_true")
    
    args = parser.parse_args()
    
    if args.list:
        list_configs()
    else:
        start_scheduler(args.config)