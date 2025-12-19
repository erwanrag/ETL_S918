"""
Alerting Email via SMTP Proginov interne
"""
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from typing import List, Optional
import logging
import socket

logger = logging.getLogger(__name__)


class EmailAlerter:
    """Envoi emails via SMTP interne Proginov"""
    
    def __init__(self, config: dict):
        self.host = config["host"]
        self.port = config["port"]
        self.use_tls = config.get("use_tls", False)
        self.username = config.get("username")
        self.password = config.get("password")
        self.sender = config["sender"]
        self.sender_name = config.get("sender_name", "ETL Monitor")
        self.recipients = config["recipients"]
    
    def send(self, 
             title: str, 
             message: str,
             html: bool = True,
             priority: str = "normal") -> bool:
        """
        Envoyer email d'alerte
        
        Args:
            title: Sujet de l'email
            message: Corps du message (HTML si html=True)
            html: Si True, message interprété comme HTML
            priority: normal | high
        """
        try:
            # Construire message
            msg = MIMEMultipart('alternative')
            msg['Subject'] = title
            msg['From'] = f"{self.sender_name} <{self.sender}>"
            msg['To'] = ", ".join(self.recipients)
            
            if priority == "high":
                msg['X-Priority'] = '1'
                msg['Importance'] = 'high'
            
            # Corps du message
            if html:
                body = MIMEText(message, 'html', 'utf-8')
            else:
                body = MIMEText(message, 'plain', 'utf-8')
            
            msg.attach(body)
            
            # Connexion SMTP (sans auth pour Proginov)
            server = smtplib.SMTP(self.host, self.port)
            
            if self.use_tls:
                server.starttls()
            
            # Authentification uniquement si credentials fournis
            if self.username and self.password:
                server.login(self.username, self.password)
            
            # Envoi
            server.send_message(msg)
            server.quit()
            
            logger.info(f"✅ Email envoyé: {title}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Erreur email: {e}")
            return False


def create_error_email_html(
    flow_name: str,
    error_message: str,
    context: dict = None
) -> str:
    """Template HTML pour email d'erreur (ultra-compatible)"""
    
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    hostname = socket.gethostname()
    
    context_rows = ""
    if context:
        for key, value in context.items():
            context_rows += f"<tr><td><b>{key}:</b></td><td>{value}</td></tr>"
    
    html = f"""
    <html>
    <body>
        <table width="600" cellpadding="0" cellspacing="0" border="0" style="margin: 0 auto; font-family: Arial, sans-serif;">
            <!-- Header -->
            <tr>
                <td bgcolor="#dc3545" style="padding: 30px; text-align: center;">
                    <font color="#ffffff" size="5" style="font-weight: bold;">
                        ❌ Échec Pipeline ETL
                    </font>
                </td>
            </tr>
            
            <!-- Body -->
            <tr>
                <td bgcolor="#f9f9f9" style="padding: 20px;">
                    <p><b>🎯 Flow:</b> {flow_name}</p>
                    <p><b>🕐 Timestamp:</b> {timestamp}</p>
                    
                    <h3>⚠️ Erreur:</h3>
                    <table width="100%" bgcolor="#fff5f5" style="border-left: 4px solid #dc3545; padding: 15px;">
                        <tr>
                            <td style="font-family: Courier New, monospace; white-space: pre-wrap;">
{error_message}
                            </td>
                        </tr>
                    </table>
                    
                    <h3>📋 Contexte:</h3>
                    <table>
                        {context_rows}
                    </table>
                    
                    <hr style="border: 0; border-top: 1px solid #ddd; margin-top: 20px;">
                    
                    <p align="center" style="color: #666; font-size: 12px;">
                        Alerte générée automatiquement par S918_ETL<br>
                        Serveur: {hostname} | {timestamp}
                    </p>
                </td>
            </tr>
        </table>
    </body>
    </html>
    """
    
    return html


def create_success_email_html(
    flow_name: str,
    message: str,
    stats: dict = None
) -> str:
    """Template HTML pour email de succès"""
    
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    hostname = socket.gethostname()
    
    stats_rows = ""
    if stats:
        for key, value in stats.items():
            stats_rows += f"<tr><td><b>{key}:</b></td><td>{value}</td></tr>"
    
    html = f"""
    <html>
    <body>
        <table width="600" cellpadding="0" cellspacing="0" border="0" style="margin: 0 auto; font-family: Arial, sans-serif;">
            <tr>
                <td bgcolor="#28a745" style="padding: 30px; text-align: center;">
                    <font color="#ffffff" size="5" style="font-weight: bold;">
                        ✅ Succès Pipeline ETL
                    </font>
                </td>
            </tr>
            
            <tr>
                <td bgcolor="#f9f9f9" style="padding: 20px;">
                    <p><b>🎯 Flow:</b> {flow_name}</p>
                    <p><b>🕐 Timestamp:</b> {timestamp}</p>
                    <p>{message}</p>
                    
                    <h3>📊 Statistiques:</h3>
                    <table>
                        {stats_rows}
                    </table>
                    
                    <hr style="border: 0; border-top: 1px solid #ddd; margin-top: 20px;">
                    
                    <p align="center" style="color: #666; font-size: 12px;">
                        Rapport généré automatiquement par S918_ETL<br>
                        Serveur: {hostname} | {timestamp}
                    </p>
                </td>
            </tr>
        </table>
    </body>
    </html>
    """
    
    return html


# Test
if __name__ == "__main__":
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent.parent.parent))
    
    from shared.config import EMAIL_CONFIG
    
    alerter = EmailAlerter(EMAIL_CONFIG)
    
    html = create_error_email_html(
        flow_name="Test Pipeline",
        error_message="Ceci est un test d'email",
        context={
            "Server": "S918_ETL",
            "Environment": "Test"
        }
    )

    alerter.send(
        title="[TEST] Alerte Email S918_ETL",
        message=html,
        html=True
    )