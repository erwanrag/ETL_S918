"""
============================================================================
Scheduler Automatique - Prefect 3.x (flow.serve)
============================================================================
Fichier : flows/orchestration/serve_scheduler.py

Exécute le pipeline ETL automatiquement selon un calendrier défini.

USAGE:
    python serve_scheduler.py [--config production|frequent|test|hourly]

Cette commande démarre un serveur qui reste actif et exécute le flow
selon le schedule configuré.

IMPORTANT: Laisser cette fenêtre PowerShell OUVERTE pour que le 
           scheduler continue de fonctionner !
============================================================================
"""

from prefect import serve
from prefect.client.schemas.schedules import CronSchedule, IntervalSchedule
from datetime import timedelta
import sys
from pathlib import Path

# Ajouter le chemin du projet
sys.path.append(str(Path(__file__).parent.parent.parent))

# Importer le flow principal
from flows.orchestration.full_pipeline import full_etl_pipeline


# ============================================================================
# CONFIGURATIONS DE SCHEDULE
# ============================================================================

DEPLOYMENT_CONFIGS = {
    "production": {
        "schedule": CronSchedule(cron="0 2 * * *", timezone="Europe/Paris"),
        "description": "Production : tous les jours à 2h du matin",
        "name": "etl-production-daily",
        "tags": ["production", "daily"]
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
    }
}


def start_scheduler(config_name: str = "production"):
    """
    Démarrer le scheduler avec la configuration choisie
    
    Args:
        config_name: "production", "frequent", "test", "hourly", "every-15min"
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
    
    if isinstance(config['schedule'], CronSchedule):
        print(f"📅 Schedule CRON : {config['schedule'].cron}")
        print(f"🌍 Timezone : {config['schedule'].timezone}")
    else:
        print(f"[TIME] Schedule Interval : {config['schedule'].interval}")
    
    print(f"\n[WARN]  IMPORTANT : Laisser cette fenêtre PowerShell OUVERTE !")
    print(f"               Le scheduler s'arrête si vous fermez cette fenêtre.\n")
    print(f"[WEB] UI Web : http://127.0.0.1:4200")
    print("=" * 70 + "\n")
    
    # Paramètres par défaut du flow
    default_params = {
        "import_metadata": False,  # Metadata déjà importée
        "run_dbt": False,          # dbt désactivé par défaut
    }
    
    # Créer le deployment et démarrer
    try:
        serve(
            full_etl_pipeline.to_deployment(
                name=config['name'],
                tags=config['tags'],
                description=config['description'],
                parameters=default_params,
                schedule=config['schedule']
            )
        )
    except KeyboardInterrupt:
        print("\n\n[STOP]  Scheduler arrêté par l'utilisateur (Ctrl+C)")
        print("[OK] Arrêt propre du scheduler\n")
    except Exception as e:
        print(f"\n\n[ERROR] Erreur : {e}\n")
        raise


def list_configs():
    """Lister toutes les configurations disponibles"""
    print("\n[LIST] CONFIGURATIONS DISPONIBLES")
    print("=" * 70)
    
    for name, cfg in DEPLOYMENT_CONFIGS.items():
        print(f"\n[-] {name}")
        print(f"   {cfg['description']}")
        
        if isinstance(cfg['schedule'], CronSchedule):
            print(f"   📅 CRON: {cfg['schedule'].cron} ({cfg['schedule'].timezone})")
        else:
            interval = cfg['schedule'].interval
            if interval.total_seconds() < 3600:
                minutes = int(interval.total_seconds() / 60)
                print(f"   [TIME] Interval: {minutes} minutes")
            else:
                hours = int(interval.total_seconds() / 3600)
                print(f"   [TIME] Interval: {hours} heures")
        
        print(f"   [TAG]  Tags: {', '.join(cfg['tags'])}")
    
    print("\n" + "=" * 70)
    print("\n[TIP] USAGE:")
    print("   python serve_scheduler.py --config production")
    print("   python serve_scheduler.py --config test")
    print("=" * 70 + "\n")


# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Démarrer le scheduler ETL Prefect 3.x",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples:
  python serve_scheduler.py --config production    # Tous les jours à 2h
  python serve_scheduler.py --config test          # Toutes les heures
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