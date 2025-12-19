"""
Gestionnaire unifié d'alertes multi-canaux
"""
from enum import Enum
from typing import Optional, Dict
import logging
import sys
from pathlib import Path

logger = logging.getLogger(__name__)

# Ajouter le projet au path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))


class AlertLevel(Enum):
    """Niveaux d'alerte"""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class AlertManager:
    """Routage intelligent des alertes vers Email et/ou Teams"""
    
    def __init__(self, 
                 email_config: Optional[Dict] = None,
                 teams_webhook: Optional[str] = None,
                 routing: Optional[Dict] = None):
        """
        Args:
            email_config: Config SMTP (ou None pour désactiver)
            teams_webhook: URL webhook Teams (ou None pour désactiver)
            routing: Règles de routage par niveau
        """
        from shared.config import ALERT_ROUTING
        
        self.email_enabled = email_config is not None
        self.teams_enabled = teams_webhook is not None
        self.routing = routing or ALERT_ROUTING
        
        if self.email_enabled:
            from shared.alerting.email_alerts import EmailAlerter
            self.email = EmailAlerter(email_config)
        
        if self.teams_enabled:
            from shared.alerting.teams_alerts import TeamsAlerter
            self.teams = TeamsAlerter(teams_webhook)
    
    def send_alert(self, 
                level: AlertLevel,
                title: str, 
                message: str,
                context: Optional[Dict] = None,
                html: bool = False):
        """Envoyer alerte selon niveau et routage"""
        
        channels = self.routing.get(level.value, [])
        logger.info(f"[ALERT] Level={level.value}, Channels={channels}")
        
        # Mapping couleurs Teams
        color_map = {
            AlertLevel.INFO: "info",
            AlertLevel.WARNING: "warning",
            AlertLevel.ERROR: "attention",
            AlertLevel.CRITICAL: "attention"
        }
        
        # Envoyer vers Teams
        if "teams" in channels and self.teams_enabled:
            logger.info(f"[TEAMS] Envoi vers Teams...")
            try:
                self.teams.send(
                    title=title,
                    message=message,
                    color=color_map[level],
                    facts=context
                )
                logger.info(f"[TEAMS] OK")
            except Exception as e:
                logger.error(f"[TEAMS] ERREUR: {e}")
        
        # Envoyer vers Email
        if "email" in channels and self.email_enabled:
            logger.info(f"[EMAIL] Envoi vers Email...")
            try:
                # Si HTML fourni, utiliser directement
                if html:
                    email_body = message
                else:
                    # Sinon, créer HTML automatiquement
                    from shared.alerting.email_alerts import create_error_email_html
                    email_body = create_error_email_html(
                        flow_name=context.get("flow", "Pipeline") if context else "Pipeline",
                        error_message=message,
                        context=context
                    )
                
                self.email.send(
                    title=title,
                    message=email_body,
                    html=True,
                    priority="high" if level in (AlertLevel.ERROR, AlertLevel.CRITICAL) else "normal"
                )
                logger.info(f"[EMAIL] OK")
            except Exception as e:
                logger.error(f"[EMAIL] ERREUR: {e}")


def get_alert_manager() -> AlertManager:
    """Factory pour obtenir AlertManager configuré"""
    from shared.config import EMAIL_CONFIG, TEAMS_WEBHOOK_URL
    
    return AlertManager(
        email_config=EMAIL_CONFIG,
        teams_webhook=TEAMS_WEBHOOK_URL
    )