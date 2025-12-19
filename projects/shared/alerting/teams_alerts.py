"""
Alerting Microsoft Teams via Power Automate webhook
"""
import requests
import json
from datetime import datetime
from typing import Optional, Dict
import logging

logger = logging.getLogger(__name__)


class TeamsAlerter:
    """Envoi notifications Teams via Power Automate"""
    
    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url
    
    def send(self, 
            title: str, 
            message: str, 
            color: str = "0078D4",
            facts: Optional[Dict] = None) -> bool:
        """Envoyer notification Teams"""
        
        # Mapping couleurs
        colors = {
            "good": "28a745",
            "warning": "ffc107",
            "attention": "dc3545",
            "info": "0078D4"
        }
        color_hex = colors.get(color, color)
        
        # Payload simplifié pour Power Automate
        payload = {
            "title": title,
            "text": message,
            "color": color_hex
        }
        
        # Ajouter faits comme texte formaté
        if facts:
            facts_text = "\n\n**Détails:**\n"
            for k, v in facts.items():
                facts_text += f"- **{k}:** {v}\n"
            payload["text"] = message + facts_text
        
        try:
            response = requests.post(
                self.webhook_url,
                headers={"Content-Type": "application/json"},
                data=json.dumps(payload),
                timeout=10
            )
            
            # 200 ou 202 = OK pour Power Automate
            if response.status_code in (200, 202):
                logger.info(f"✅ Teams notification sent: {title}")
                return True
            else:
                logger.error(f"❌ Teams error {response.status_code}: {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Teams exception: {e}")
            return False


# Test
if __name__ == "__main__":
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent.parent.parent))
    
    from shared.config import TEAMS_WEBHOOK_URL
    
    alerter = TeamsAlerter(TEAMS_WEBHOOK_URL)
    
    alerter.send(
        title="✅ Test Pipeline ETL - SUCCESS",
        message="Ceci est un test de notification Teams",
        color="good",
        facts={
            "Server": "S918_ETL",
            "Duration": "45s",
            "Tables": "127"
        }
    )