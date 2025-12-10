"""
Configuration centralisée alerting ETL + Services
"""
import os

# =============================================================================
# EMAIL SMTP (Proginov interne)
# =============================================================================
EMAIL_CONFIG = {
    "host": "mail.proginov.fr",
    "port": 25,
    "use_tls": False,  # Port 25 = pas de TLS
    "username": None,   # Pas d'authentification
    "password": None,
    "sender": "cbm_etl@cbmcompany.com",
    "sender_name": "Admin S918_ETL",
    "recipients": [
        "e.ragueneau@cbmcompany.com",  # ← À PERSONNALISER
        # "team@cbmcompany.com"
    ]
}

# =============================================================================
# MICROSOFT TEAMS (Power Automate webhook)
# =============================================================================
TEAMS_WEBHOOK_URL = os.getenv(
    "TEAMS_WEBHOOK_URL",
    "https://defaultddd00466a8b744b3be72579de2c17c.8a.environment.api.powerplatform.com:443/powerautomate/automations/direct/workflows/519df0eb53a74d8c90d57fed05c31f7b/triggers/manual/paths/invoke/?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=w6V5Z2SuIGZFWe1oB0SCN6EANQ1lMAAjlBf-3aB1Yr0"
)

# =============================================================================
# ROUTAGE ALERTES
# =============================================================================
ALERT_ROUTING = {
    "info": ["teams"],              # Succès → Teams uniquement
    "warning": ["teams"],           # Warning → Teams
    "error": ["teams", "email"],    # Erreur → Teams + Email
    "critical": ["teams", "email"]  # Critique → Teams + Email
}