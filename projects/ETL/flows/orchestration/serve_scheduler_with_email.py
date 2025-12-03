"""
[START] Scheduler ETL avec alertes email intégrées

Version améliorée du scheduler Prefect 3.x avec support natif des alertes email
en cas d'échec du pipeline.

Usage:
    # 1. Configurer les alertes (une seule fois)
    python serve_scheduler.py --setup-email
    
    # 2. Démarrer avec alertes
    python serve_scheduler.py --config production --enable-email
    
    # 3. Tester les alertes
    python serve_scheduler.py --test-email

Auteur: ETL Team
Date: 2025-12-03
"""

import sys
import os
import argparse
from datetime import datetime, timedelta
from pathlib import Path
import json
from typing import Optional, Dict, List

# [FIX] Ajouter le répertoire parent au path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from prefect import flow, get_run_logger
from prefect.client.schemas.schedules import CronSchedule, IntervalSchedule
from prefect.deployments import run_deployment

# [IMPORT] Flow principal
try:
    from flows.full_pipeline import full_etl_pipeline
except ImportError:
    from full_pipeline import full_etl_pipeline

# [IMPORT] Email alerts
try:
    from email_alerts import send_failure_alert, test_email_configuration, EmailConfig
except ImportError:
    # Fallback si email_alerts.py pas dans le même dossier
    email_alerts_available = False
else:
    email_alerts_available = True


# [CONFIG] Configurations prédéfinies
SCHEDULE_CONFIGS = {
    "production": {
        "name": "etl-production",
        "description": "Production : tous les jours à 2h du matin",
        "schedule": CronSchedule(cron="0 2 * * *", timezone="Europe/Paris"),
        "tags": ["production", "daily"]
    },
    "frequent": {
        "name": "etl-frequent",
        "description": "Fréquent : toutes les 4 heures",
        "schedule": IntervalSchedule(interval=timedelta(hours=4)),
        "tags": ["frequent", "auto"]
    },
    "test": {
        "name": "etl-test",
        "description": "Test : toutes les heures",
        "schedule": IntervalSchedule(interval=timedelta(hours=1)),
        "tags": ["test", "hourly"]
    },
    "hourly": {
        "name": "etl-hourly",
        "description": "Horaire : toutes les heures à H:00",
        "schedule": CronSchedule(cron="0 * * * *", timezone="Europe/Paris"),
        "tags": ["hourly", "auto"]
    },
    "every-15min": {
        "name": "etl-test-15min",
        "description": "Test rapide : toutes les 15 minutes",
        "schedule": IntervalSchedule(interval=timedelta(minutes=15)),
        "tags": ["test", "rapid"]
    }
}


class EmailAlertConfig:
    """[CONFIG] Configuration des alertes email"""
    
    CONFIG_FILE = Path(__file__).parent / "email_config.json"
    
    @classmethod
    def save(cls, config: Dict) -> bool:
        """[SAVE] Sauvegarder la configuration email"""
        try:
            with open(cls.CONFIG_FILE, 'w') as f:
                json.dump(config, f, indent=2)
            print(f"[OK] Configuration sauvegardée: {cls.CONFIG_FILE}")
            return True
        except Exception as e:
            print(f"[ERROR] Échec sauvegarde: {e}")
            return False
    
    @classmethod
    def load(cls) -> Optional[Dict]:
        """[LOAD] Charger la configuration email"""
        try:
            if cls.CONFIG_FILE.exists():
                with open(cls.CONFIG_FILE, 'r') as f:
                    config = json.load(f)
                print(f"[OK] Configuration chargée: {cls.CONFIG_FILE}")
                return config
            else:
                print(f"[WARN] Aucune configuration trouvée: {cls.CONFIG_FILE}")
                return None
        except Exception as e:
            print(f"[ERROR] Échec chargement: {e}")
            return None
    
    @classmethod
    def delete(cls) -> bool:
        """[DROP] Supprimer la configuration"""
        try:
            if cls.CONFIG_FILE.exists():
                cls.CONFIG_FILE.unlink()
                print(f"[OK] Configuration supprimée")
                return True
            return False
        except Exception as e:
            print(f"[ERROR] Échec suppression: {e}")
            return False


def setup_email_alerts():
    """[CONFIG] Configuration interactive des alertes email"""
    
    if not email_alerts_available:
        print("[ERROR] Module email_alerts.py non trouvé")
        print("[INFO] Copiez email_alerts.py dans le même dossier que serve_scheduler.py")
        return
    
    print("""
    ======================================================================
    [CONFIG] CONFIGURATION DES ALERTES EMAIL
    ======================================================================
    
    Cette configuration sera sauvegardée et réutilisée automatiquement
    à chaque démarrage du scheduler avec --enable-email.
    
    ======================================================================
    """)
    
    # [INPUT] Type de serveur SMTP
    print("\n[CONFIG] Types de serveurs SMTP disponibles:")
    for i, preset in enumerate(EmailConfig.PRESETS.keys(), 1):
        print(f"  {i}. {preset}")
    
    smtp_type = input("\n[CONFIG] Choisir le type (gmail/outlook/office365) [gmail]: ").strip() or "gmail"
    
    if smtp_type not in EmailConfig.PRESETS:
        print(f"[ERROR] Type inconnu: {smtp_type}")
        return
    
    # [INPUT] Identifiants
    smtp_username = input("\n[CONFIG] Email expéditeur (SMTP username): ").strip()
    
    print("\n[WARN] Pour Gmail:")
    print("       1. Activez l'authentification à 2 facteurs")
    print("       2. Générez un 'mot de passe d'application'")
    print("       3. Utilisez ce mot de passe (pas votre mot de passe Gmail)")
    
    smtp_password = input("\n[CONFIG] Mot de passe SMTP: ").strip()
    
    # [INPUT] Destinataires
    recipients_input = input("\n[MAIL] Destinataire(s) (séparés par des virgules): ").strip()
    recipients = [r.strip() for r in recipients_input.split(",")]
    
    # [BUILD] Configuration
    smtp_config = EmailConfig.PRESETS[smtp_type].copy()
    smtp_config["username"] = smtp_username
    smtp_config["password"] = smtp_password
    
    # [TEST] Tester la configuration
    print("\n[TEST] Test de la configuration...")
    
    if test_email_configuration(recipients, smtp_config):
        # [SAVE] Sauvegarder
        config = {
            "recipients": recipients,
            "smtp_config": smtp_config
        }
        
        if EmailAlertConfig.save(config):
            print("""
            ======================================================================
            [OK] CONFIGURATION TERMINÉE
            ======================================================================
            
            Les alertes email sont maintenant configurées !
            
            Pour démarrer le scheduler avec alertes:
                python serve_scheduler.py --config production --enable-email
            
            ======================================================================
            """)
        else:
            print("[ERROR] Échec de la sauvegarde de la configuration")
    else:
        print("""
        ======================================================================
        [ERROR] ÉCHEC DU TEST
        ======================================================================
        
        Vérifiez:
        - Identifiants SMTP corrects
        - Pour Gmail: mot de passe d'application généré
        - Connexion internet active
        - Pare-feu autorisant le port SMTP
        
        ======================================================================
        """)


def test_email_alerts():
    """[TEST] Tester les alertes email avec la configuration sauvegardée"""
    
    if not email_alerts_available:
        print("[ERROR] Module email_alerts.py non trouvé")
        return
    
    # [LOAD] Charger configuration
    config = EmailAlertConfig.load()
    
    if not config:
        print("[ERROR] Aucune configuration trouvée")
        print("[INFO] Lancez d'abord: python serve_scheduler.py --setup-email")
        return
    
    print("\n[TEST] Envoi d'un email de test...")
    
    # [TEST] Envoyer email de test
    success = test_email_configuration(
        recipients=config["recipients"],
        smtp_config=config["smtp_config"]
    )
    
    if success:
        print("\n[OK] Email de test envoyé avec succès !")
        print(f"[MAIL] Vérifiez la boîte mail: {', '.join(config['recipients'])}")
    else:
        print("\n[ERROR] Échec de l'envoi de l'email de test")


@flow(
    name="[START] Pipeline ETL Complet v3 (Propagation)",
    description="Pipeline ETL complet avec gestion automatique FULL/INCREMENTAL et alertes email",
    log_prints=True
)
def full_etl_pipeline_with_email_alerts(
    import_metadata: bool = False,
    run_dbt: bool = False,
    email_config: Optional[Dict] = None
):
    """
    [START] Wrapper du pipeline ETL avec gestion des alertes email
    
    Args:
        import_metadata: Importer les métadonnées Progress
        run_dbt: Exécuter dbt après ODS
        email_config: Configuration email pour les alertes
    """
    logger = get_run_logger()
    
    try:
        # [START] Lancer le pipeline principal
        result = full_etl_pipeline(
            import_metadata=import_metadata,
            run_dbt=run_dbt
        )
        
        return result
        
    except Exception as e:
        # [ERROR] Capturer l'erreur
        logger.error(f"[ERROR] Échec du pipeline: {e}")
        
        # [MAIL] Envoyer alerte si configuré
        if email_config and email_alerts_available:
            try:
                from prefect.context import get_run_context
                
                context = get_run_context()
                flow_run = context.flow_run
                
                send_failure_alert(
                    flow_name=flow_run.flow_name,
                    flow_run_id=str(flow_run.id),
                    flow_run_name=flow_run.name,
                    error_message=str(e),
                    recipients=email_config["recipients"],
                    smtp_config=email_config["smtp_config"],
                    timestamp=datetime.now(),
                    ui_url=f"http://127.0.0.1:4200/runs/flow-run/{flow_run.id}"
                )
                
                logger.info("[OK] Alerte email envoyée")
                
            except Exception as email_error:
                logger.error(f"[ERROR] Échec envoi alerte email: {email_error}")
        
        # [ERROR] Re-lever l'exception
        raise


def main():
    """[MAIN] Point d'entrée principal"""
    
    parser = argparse.ArgumentParser(
        description="[START] Scheduler Prefect ETL avec alertes email",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples:
    # Lister les configurations disponibles
    python serve_scheduler.py --list
    
    # Configurer les alertes email (une seule fois)
    python serve_scheduler.py --setup-email
    
    # Tester les alertes
    python serve_scheduler.py --test-email
    
    # Démarrer en production avec alertes
    python serve_scheduler.py --config production --enable-email
    
    # Démarrer en test sans alertes
    python serve_scheduler.py --config test
        """
    )
    
    parser.add_argument(
        "--config",
        choices=list(SCHEDULE_CONFIGS.keys()),
        help="Configuration de schedule à utiliser"
    )
    
    parser.add_argument(
        "--list",
        action="store_true",
        help="Lister les configurations disponibles"
    )
    
    parser.add_argument(
        "--setup-email",
        action="store_true",
        help="Configurer les alertes email (interactif)"
    )
    
    parser.add_argument(
        "--test-email",
        action="store_true",
        help="Tester la configuration email"
    )
    
    parser.add_argument(
        "--enable-email",
        action="store_true",
        help="Activer les alertes email (nécessite --setup-email d'abord)"
    )
    
    args = parser.parse_args()
    
    # [LIST] Lister les configurations
    if args.list:
        print("\n" + "=" * 70)
        print("[LIST] CONFIGURATIONS DISPONIBLES")
        print("=" * 70 + "\n")
        
        for name, config in SCHEDULE_CONFIGS.items():
            print(f"[CONFIG] {name}")
            print(f"  [NOTE] {config['description']}")
            print(f"  [TAG] {', '.join(config['tags'])}")
            
            if isinstance(config['schedule'], CronSchedule):
                print(f"  [TIME] CRON: {config['schedule'].cron}")
            else:
                print(f"  [TIME] Interval: {config['schedule'].interval}")
            
            print()
        
        return
    
    # [SETUP] Configuration email
    if args.setup_email:
        setup_email_alerts()
        return
    
    # [TEST] Test email
    if args.test_email:
        test_email_alerts()
        return
    
    # [START] Démarrage scheduler
    if not args.config:
        parser.print_help()
        return
    
    config = SCHEDULE_CONFIGS[args.config]
    
    # [LOAD] Configuration email si demandée
    email_config = None
    if args.enable_email:
        if not email_alerts_available:
            print("[ERROR] Module email_alerts.py non disponible")
            print("[INFO] Les alertes email seront désactivées")
        else:
            email_config = EmailAlertConfig.load()
            if not email_config:
                print("[WARN] Aucune configuration email trouvée")
                print("[INFO] Lancez: python serve_scheduler.py --setup-email")
                print("[INFO] Les alertes email seront désactivées")
            else:
                print(f"[OK] Alertes email activées pour: {', '.join(email_config['recipients'])}")
    
    # [DATA] Paramètres par défaut du flow
    default_params = {
        "import_metadata": False,
        "run_dbt": False
    }
    
    if email_config:
        default_params["email_config"] = email_config
    
    # [INFO] Afficher configuration
    print("\n" + "=" * 70)
    print(f"[START] DÉMARRAGE SCHEDULER ETL - Configuration '{args.config}'")
    print("=" * 70 + "\n")
    print(f"[NOTE] Description : {config['description']}")
    print(f"[TAG]  Nom : {config['name']}")
    print(f"[TAG]  Tags : {', '.join(config['tags'])}")
    
    if isinstance(config['schedule'], CronSchedule):
        print(f"[TIME] Schedule CRON : {config['schedule'].cron}")
    else:
        print(f"[TIME] Schedule Interval : {config['schedule'].interval}")
    
    if email_config:
        print(f"[MAIL] Alertes email : ACTIVÉES")
        print(f"[MAIL] Destinataires : {', '.join(email_config['recipients'])}")
    else:
        print(f"[MAIL] Alertes email : DÉSACTIVÉES")
    
    print(f"\n[WARN]  IMPORTANT : Laisser cette fenêtre PowerShell OUVERTE !")
    print(f"               Le scheduler s'arrête si vous fermez cette fenêtre.")
    print(f"\n[WEB] UI Web : http://127.0.0.1:4200")
    print("=" * 70 + "\n")
    
    # [SERVE] Démarrer le serveur
    flow_to_serve = full_etl_pipeline_with_email_alerts if email_config else full_etl_pipeline
    
    flow_to_serve.serve(
        name=config["name"],
        tags=config["tags"],
        schedule=config["schedule"],
        parameters=default_params
    )


if __name__ == "__main__":
    main()