"""
============================================================================
Scheduler Automatique - Prefect 3.x (flow.serve) - LOCAL
============================================================================
Fichier : flows/orchestration/serve_scheduler.py

Exécute le pipeline ETL automatiquement selon un calendrier défini.

USAGE:
    python serve_scheduler.py [--config production|frequent|test|hourly]

Cette commande démarre un serveur LOCAL qui reste actif et exécute le flow
selon le schedule configuré.

IMPORTANT: Laisser cette fenêtre PowerShell OUVERTE pour que le 
           scheduler continue de fonctionner !
============================================================================
"""

import sys
from pathlib import Path

# Ajouter le chemin du projet AVANT les imports locaux
ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

# Logging doit aussi pointer au bon fichier YAML
import logging.config
import yaml

LOG_FILE = ROOT / "prefect_logging.yml"
with open(LOG_FILE, 'r') as f:
    log_config = yaml.safe_load(f)
    logging.config.dictConfig(log_config)

# Imports Prefect après setup sys.path
from prefect import serve
from prefect.client.schemas.schedules import CronSchedule, IntervalSchedule

# Import des flows après correction du path
from flows.orchestration.full_pipeline import full_etl_pipeline
from flows.ingestion.manual_table_import import manual_table_import_flow


# ============================================================================
# CONFIGURATIONS DE SCHEDULE
# ============================================================================

DEPLOYMENT_CONFIGS = {
    "production": {
        "schedule": CronSchedule(cron="0 * * * *", timezone="Europe/Paris"),  # ← Changé
        "description": "Production : toutes les heures",  # ← Changé
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
    "hourly": {
        "schedule": CronSchedule(cron="0 * * * *", timezone="Europe/Paris"),
        "description": "Horaire : toutes les heures",
        "name": "etl-hourly",
        "tags": ["production", "hourly"]
    },
    "every-15min": {
        "schedule": IntervalSchedule(interval=timedelta(minutes=15)),
        "description": "Test rapide : toutes les 15 minutes",
        "name": "etl-test-15min",
        "tags": ["test", "rapid"]
    },
    "manual-only": {
        "schedule": None,
        "description": "Pas de schedule - Démarrage manuel uniquement",
        "name": "etl-manual",
        "tags": ["manual"]
    }
}


def start_scheduler(config_name: str = "production"):
    """
    Démarrer le scheduler avec la configuration choisie
    
    Args:
        config_name: "production", "frequent", "test", "hourly", "every-15min", "manual-only"
    """
    if config_name not in DEPLOYMENT_CONFIGS:
        print(f"\n[ERROR] Configuration '{config_name}' inconnue")
        print(f"\n[LIST] Configurations disponibles :")
        list_configs()
        return
    
    config = DEPLOYMENT_CONFIGS[config_name]
    
    print("\n" + "=" * 70)
    print(f"[START] DÉMARRAGE SCHEDULER ETL - Configuration '{config_name}'")
    print("=" * 70)
    print(f"\n[NOTE] Description : {config['description']}")
    print(f"[TAG]  Nom : {config['name']}")
    print(f"[TAG]  Tags : {', '.join(config['tags'])}")
    
    if config['schedule']:
        if isinstance(config['schedule'], CronSchedule):
            print(f"📅 Schedule CRON : {config['schedule'].cron}")
            print(f"🌍 Timezone : {config['schedule'].timezone}")
        else:
            print(f"⏱️  Schedule Interval : {config['schedule'].interval}")
    else:
        print(f"🔧 Mode manuel : Pas de schedule automatique")
    
    print(f"\n⚠️  IMPORTANT : Laisser cette fenêtre PowerShell OUVERTE !")
    print(f"               Le scheduler s'arrête si vous fermez cette fenêtre.\n")
    print(f"🌐 UI Web : http://127.0.0.1:4200")
    print("=" * 70 + "\n")
    
    # Paramètres par défaut du flow principal
    default_params = {
        "import_metadata": False,  # Metadata déjà importée
        "run_dbt": False,          # dbt désactivé par défaut
    }
    
  # Créer les deployments
    deployments = [
        # Pipeline complet
        full_etl_pipeline.to_deployment(
            name=config['name'],
            tags=config['tags'],
            description=config['description'],
            parameters=default_params,
            schedule=config['schedule']
        )
    ]

    # Démarrer le serveur
    try:
        print("🚀 Serveur Prefect démarré !\n")
        print("📋 Deployment disponible :")
        print(f"   - {config['name']} (pipeline complet)\n")
        print("💡 Pour importer une table manuellement :")
        print("   python flows/ingestion/manual_table_import.py\n")
        
        serve(*deployments)
            
    except KeyboardInterrupt:
        print("\n\n[STOP] Scheduler arrêté par l'utilisateur (Ctrl+C)")
        print("[OK] Arrêt propre du scheduler\n")
    except Exception as e:
        print(f"\n\n[ERROR] Erreur : {e}\n")
        raise


def list_configs():
    """Lister toutes les configurations disponibles"""
    print("\n[LIST] CONFIGURATIONS DISPONIBLES")
    print("=" * 70)
    
    for name, cfg in DEPLOYMENT_CONFIGS.items():
        print(f"\n🔹 {name}")
        print(f"   {cfg['description']}")
        
        if cfg['schedule']:
            if isinstance(cfg['schedule'], CronSchedule):
                print(f"   📅 CRON: {cfg['schedule'].cron} ({cfg['schedule'].timezone})")
            else:
                interval = cfg['schedule'].interval
                if interval.total_seconds() < 3600:
                    minutes = int(interval.total_seconds() / 60)
                    print(f"   ⏱️  Interval: {minutes} minutes")
                else:
                    hours = int(interval.total_seconds() / 3600)
                    print(f"   ⏱️  Interval: {hours} heures")
        else:
            print(f"   🔧 Mode: Manuel uniquement")
        
        print(f"   🏷️  Tags: {', '.join(cfg['tags'])}")
    
    print("\n" + "=" * 70)
    print("\n💡 USAGE:")
    print("   python serve_scheduler.py --config production")
    print("   python serve_scheduler.py --config hourly")
    print("   python serve_scheduler.py --config manual-only")
    print("=" * 70 + "\n")


# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Démarrer le scheduler ETL Prefect 3.x (LOCAL)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples:
  python serve_scheduler.py --config production    # Tous les jours à 3h
  python serve_scheduler.py --config hourly        # Toutes les heures
  python serve_scheduler.py --config manual-only   # Pas de schedule auto
  python serve_scheduler.py --list                 # Lister les configs
        """
    )
    
    parser.add_argument(
        "--config",
        choices=list(DEPLOYMENT_CONFIGS.keys()),
        default="production",
        help="Configuration à utiliser"
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="Lister les configurations disponibles"
    )
    
    args = parser.parse_args()
    
    if args.list:
        list_configs()
    else:
        start_scheduler(args.config)