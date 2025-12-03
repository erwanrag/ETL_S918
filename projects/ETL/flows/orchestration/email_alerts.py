"""
[MAIL] Configuration des alertes email pour échecs ETL

Ce module configure les notifications email via SMTP pour alerter en cas d'échec
du pipeline ETL. Compatible avec Gmail, Outlook, serveurs SMTP personnalisés.

Usage:
    from email_alerts import setup_email_notifications
    
    # Dans votre flow
    setup_email_notifications(
        flow_name="Pipeline ETL",
        recipients=["admin@example.com"],
        smtp_config={
            "host": "smtp.gmail.com",
            "port": 587,
            "username": "votre.email@gmail.com",
            "password": "votre_mot_de_passe_app"
        }
    )

Configuration Gmail:
    1. Activer l'authentification à 2 facteurs
    2. Générer un "mot de passe d'application"
    3. Utiliser ce mot de passe dans smtp_config
"""

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from typing import List, Dict, Optional
from prefect import flow, task
from prefect.blocks.system import Secret
from prefect.events import Event
import logging

logger = logging.getLogger(__name__)


class EmailConfig:
    """Configuration email centralisée"""
    
    # [CONFIG] Configurations SMTP prédéfinies
    PRESETS = {
        "gmail": {
            "host": "smtp.gmail.com",
            "port": 587,
            "use_tls": True
        },
        "outlook": {
            "host": "smtp-mail.outlook.com",
            "port": 587,
            "use_tls": True
        },
        "office365": {
            "host": "smtp.office365.com",
            "port": 587,
            "use_tls": True
        }
    }


def send_email(
    subject: str,
    body: str,
    recipients: List[str],
    smtp_host: str,
    smtp_port: int,
    smtp_username: str,
    smtp_password: str,
    use_tls: bool = True,
    sender_name: str = "Prefect ETL Monitor"
) -> bool:
    """
    [SEND] Envoyer un email via SMTP
    
    Args:
        subject: Sujet de l'email
        body: Corps de l'email (HTML supporté)
        recipients: Liste des destinataires
        smtp_host: Serveur SMTP
        smtp_port: Port SMTP
        smtp_username: Nom d'utilisateur SMTP
        smtp_password: Mot de passe SMTP
        use_tls: Utiliser TLS (défaut: True)
        sender_name: Nom affiché de l'expéditeur
        
    Returns:
        True si envoi réussi, False sinon
    """
    try:
        # [BUILD] Construire le message
        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = f"{sender_name} <{smtp_username}>"
        msg['To'] = ", ".join(recipients)
        
        # [HTML] Ajouter le corps HTML
        html_part = MIMEText(body, 'html', 'utf-8')
        msg.attach(html_part)
        
        # [CONNECT] Connexion SMTP
        logger.info(f"[CONNECT] Connexion à {smtp_host}:{smtp_port}")
        
        if use_tls:
            server = smtplib.SMTP(smtp_host, smtp_port)
            server.starttls()
        else:
            server = smtplib.SMTP_SSL(smtp_host, smtp_port)
        
        # [AUTH] Authentification
        server.login(smtp_username, smtp_password)
        
        # [SEND] Envoi
        server.send_message(msg)
        server.quit()
        
        logger.info(f"[OK] Email envoyé à {len(recipients)} destinataire(s)")
        return True
        
    except Exception as e:
        logger.error(f"[ERROR] Échec envoi email: {e}")
        return False


def create_failure_email_html(
    flow_name: str,
    flow_run_id: str,
    flow_run_name: str,
    error_message: str,
    timestamp: datetime,
    duration: Optional[float] = None,
    ui_url: Optional[str] = None
) -> str:
    """
    [HTML] Créer le template HTML pour email d'échec
    
    Args:
        flow_name: Nom du flow
        flow_run_id: ID de l'exécution
        flow_run_name: Nom de l'exécution
        error_message: Message d'erreur
        timestamp: Timestamp de l'échec
        duration: Durée avant échec (secondes)
        ui_url: URL de l'interface Prefect
        
    Returns:
        HTML formaté
    """
    duration_str = f"{duration:.2f}s" if duration else "N/A"
    
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8">
        <style>
            body {{
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Arial, sans-serif;
                line-height: 1.6;
                color: #333;
                max-width: 600px;
                margin: 0 auto;
                padding: 20px;
            }}
            .header {{
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white;
                padding: 30px;
                border-radius: 8px 8px 0 0;
                text-align: center;
            }}
            .header h1 {{
                margin: 0;
                font-size: 24px;
            }}
            .status {{
                background: #fee;
                color: #c33;
                padding: 15px;
                text-align: center;
                font-weight: bold;
                font-size: 18px;
                border-left: 4px solid #c33;
            }}
            .content {{
                background: #f9f9f9;
                padding: 20px;
                border-radius: 0 0 8px 8px;
            }}
            .info-box {{
                background: white;
                padding: 15px;
                margin: 10px 0;
                border-radius: 4px;
                border-left: 3px solid #667eea;
            }}
            .info-label {{
                font-weight: bold;
                color: #667eea;
                display: inline-block;
                min-width: 120px;
            }}
            .error-box {{
                background: #fff5f5;
                border: 1px solid #feb2b2;
                padding: 15px;
                margin: 15px 0;
                border-radius: 4px;
                font-family: 'Courier New', monospace;
                font-size: 13px;
                white-space: pre-wrap;
                word-wrap: break-word;
            }}
            .button {{
                display: inline-block;
                padding: 12px 24px;
                background: #667eea;
                color: white;
                text-decoration: none;
                border-radius: 4px;
                margin-top: 15px;
            }}
            .footer {{
                text-align: center;
                margin-top: 20px;
                padding-top: 20px;
                border-top: 1px solid #ddd;
                color: #666;
                font-size: 12px;
            }}
        </style>
    </head>
    <body>
        <div class="header">
            <h1>[ERROR] Échec Pipeline ETL</h1>
        </div>
        
        <div class="status">
            ÉCHEC DÉTECTÉ
        </div>
        
        <div class="content">
            <div class="info-box">
                <span class="info-label">[TARGET] Flow:</span> {flow_name}<br>
                <span class="info-label">[KEY] Run ID:</span> {flow_run_id}<br>
                <span class="info-label">[TAG] Run Name:</span> {flow_run_name}<br>
                <span class="info-label">[TIME] Timestamp:</span> {timestamp.strftime('%Y-%m-%d %H:%M:%S')}<br>
                <span class="info-label">[TIMER] Durée:</span> {duration_str}
            </div>
            
            <h3>[ERROR] Message d'erreur:</h3>
            <div class="error-box">{error_message}</div>
            
            {f'<a href="{ui_url}" class="button">[WEB] Voir dans Prefect UI</a>' if ui_url else ''}
            
            <div class="footer">
                <p>Cette alerte a été générée automatiquement par Prefect ETL Monitor</p>
                <p>Serveur: {smtp_host} | Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            </div>
        </div>
    </body>
    </html>
    """
    
    return html


@task(name="[MAIL] Envoyer alerte échec")
def send_failure_alert(
    flow_name: str,
    flow_run_id: str,
    flow_run_name: str,
    error_message: str,
    recipients: List[str],
    smtp_config: Dict[str, any],
    timestamp: Optional[datetime] = None,
    duration: Optional[float] = None,
    ui_url: Optional[str] = None
) -> bool:
    """
    [ALERT] Envoyer une alerte email en cas d'échec
    
    Args:
        flow_name: Nom du flow
        flow_run_id: ID de l'exécution
        flow_run_name: Nom de l'exécution
        error_message: Message d'erreur
        recipients: Liste des destinataires
        smtp_config: Configuration SMTP
        timestamp: Timestamp de l'échec
        duration: Durée avant échec
        ui_url: URL de l'interface Prefect
        
    Returns:
        True si envoi réussi
    """
    if timestamp is None:
        timestamp = datetime.now()
    
    # [HTML] Générer le contenu HTML
    html_body = create_failure_email_html(
        flow_name=flow_name,
        flow_run_id=flow_run_id,
        flow_run_name=flow_run_name,
        error_message=error_message,
        timestamp=timestamp,
        duration=duration,
        ui_url=ui_url
    )
    
    # [SUBJECT] Sujet de l'email
    subject = f"[ERROR] Échec ETL: {flow_name}"
    
    # [SEND] Envoi
    return send_email(
        subject=subject,
        body=html_body,
        recipients=recipients,
        smtp_host=smtp_config["host"],
        smtp_port=smtp_config["port"],
        smtp_username=smtp_config["username"],
        smtp_password=smtp_config["password"],
        use_tls=smtp_config.get("use_tls", True)
    )


def create_email_notification_block(
    block_name: str = "etl-email-alerts",
    recipients: List[str] = None,
    smtp_preset: str = "gmail",
    smtp_username: str = None,
    smtp_password: str = None,
    custom_smtp: Dict[str, any] = None
):
    """
    [CONFIG] Créer un bloc de notification email Prefect
    
    Cette fonction crée la configuration qui sera utilisée par le scheduler
    pour envoyer automatiquement des alertes en cas d'échec.
    
    Args:
        block_name: Nom du bloc Prefect
        recipients: Liste des destinataires
        smtp_preset: Preset SMTP ("gmail", "outlook", "office365")
        smtp_username: Nom d'utilisateur SMTP
        smtp_password: Mot de passe SMTP
        custom_smtp: Configuration SMTP personnalisée
        
    Returns:
        Configuration complète
    """
    if recipients is None:
        recipients = []
    
    # [CONFIG] Configuration SMTP
    if custom_smtp:
        smtp_config = custom_smtp
    elif smtp_preset in EmailConfig.PRESETS:
        smtp_config = EmailConfig.PRESETS[smtp_preset].copy()
    else:
        raise ValueError(f"Preset SMTP inconnu: {smtp_preset}")
    
    # [AUTH] Ajouter authentification
    smtp_config["username"] = smtp_username
    smtp_config["password"] = smtp_password
    
    config = {
        "block_name": block_name,
        "recipients": recipients,
        "smtp_config": smtp_config
    }
    
    logger.info(f"[OK] Configuration email créée: {block_name}")
    logger.info(f"[MAIL] Destinataires: {', '.join(recipients)}")
    logger.info(f"[CONNECT] SMTP: {smtp_config['host']}:{smtp_config['port']}")
    
    return config


# [TEST] Fonction de test
def test_email_configuration(
    recipients: List[str],
    smtp_config: Dict[str, any]
) -> bool:
    """
    [TEST] Tester la configuration email
    
    Args:
        recipients: Liste des destinataires
        smtp_config: Configuration SMTP
        
    Returns:
        True si test réussi
    """
    logger.info("[TEST] Test de configuration email...")
    
    test_html = f"""
    <html>
    <body>
        <h2>[OK] Test de configuration email</h2>
        <p>Ce message confirme que votre configuration email fonctionne correctement.</p>
        <p><strong>[TIME] Timestamp:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        <p><strong>[CONNECT] Serveur SMTP:</strong> {smtp_config['host']}:{smtp_config['port']}</p>
        <p><strong>[MAIL] Destinataires:</strong> {', '.join(recipients)}</p>
    </body>
    </html>
    """
    
    success = send_email(
        subject="[TEST] Configuration email Prefect ETL",
        body=test_html,
        recipients=recipients,
        smtp_host=smtp_config["host"],
        smtp_port=smtp_config["port"],
        smtp_username=smtp_config["username"],
        smtp_password=smtp_config["password"],
        use_tls=smtp_config.get("use_tls", True)
    )
    
    if success:
        logger.info("[OK] Test réussi - Vérifiez votre boîte mail")
    else:
        logger.error("[ERROR] Test échoué")
    
    return success


if __name__ == "__main__":
    """
    [TEST] Script de test de configuration email
    
    Usage:
        python email_alerts.py
    """
    print("""
    ======================================================================
    [TEST] TEST DE CONFIGURATION EMAIL
    ======================================================================
    
    Ce script teste votre configuration email SMTP.
    Vous devez fournir les informations suivantes:
    
    1. Adresse email expéditeur (SMTP username)
    2. Mot de passe SMTP (ou mot de passe d'application pour Gmail)
    3. Adresse(s) email destinataire(s)
    4. Type de serveur (gmail/outlook/office365)
    
    ======================================================================
    """)
    
    # [INPUT] Saisie configuration
    smtp_username = input("[CONFIG] Email expéditeur: ").strip()
    smtp_password = input("[CONFIG] Mot de passe SMTP: ").strip()
    recipients_input = input("[MAIL] Destinataire(s) (séparés par des virgules): ").strip()
    smtp_type = input("[CONFIG] Type serveur (gmail/outlook/office365) [gmail]: ").strip() or "gmail"
    
    recipients = [r.strip() for r in recipients_input.split(",")]
    
    # [BUILD] Configuration
    if smtp_type in EmailConfig.PRESETS:
        smtp_config = EmailConfig.PRESETS[smtp_type].copy()
        smtp_config["username"] = smtp_username
        smtp_config["password"] = smtp_password
    else:
        print(f"[ERROR] Type de serveur inconnu: {smtp_type}")
        exit(1)
    
    # [TEST] Lancer test
    print("\n[START] Envoi email de test...\n")
    success = test_email_configuration(recipients, smtp_config)
    
    if success:
        print("""
        ======================================================================
        [OK] CONFIGURATION VALIDÉE
        ======================================================================
        
        Votre configuration email fonctionne correctement !
        Vous pouvez maintenant l'utiliser dans le scheduler.
        
        Prochaines étapes:
        1. Notez votre configuration
        2. Modifiez serve_scheduler.py pour ajouter les alertes
        3. Relancez le scheduler
        
        ======================================================================
        """)
    else:
        print("""
        ======================================================================
        [ERROR] ÉCHEC DE CONFIGURATION
        ======================================================================
        
        Causes possibles:
        - Identifiants incorrects
        - Serveur SMTP inaccessible
        - Pare-feu bloquant le port SMTP
        - Pour Gmail: mot de passe d'application requis
        
        ======================================================================
        """)