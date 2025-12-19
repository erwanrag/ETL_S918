"""
Configuration globale pour tous les projets Prefect
Utilise exclusivement les variables d'environnement (.env)
"""
import os
import re
from pathlib import Path
from dataclasses import dataclass
from typing import Optional

def load_env_file():
    """Charge les variables depuis .env avec expansion ${VAR}"""
    env_file = Path("/data/prefect/.env")
    
    if env_file.exists():
        env_vars = {}
        
        # Premier passage : charger toutes les variables
        with open(env_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    env_vars[key.strip()] = value.strip()
        
        # Deuxième passage : expansion des ${VAR}
        for key, value in env_vars.items():
            while '${' in value:
                match = re.search(r'\$\{([^}]+)\}', value)
                if match:
                    var_name = match.group(1)
                    var_value = env_vars.get(var_name, '')
                    value = value.replace(f'${{{var_name}}}', var_value)
                else:
                    break
            
            os.environ[key] = value

load_env_file()

@dataclass
class PostgreSQLConfig:
    """Configuration PostgreSQL via variables d'environnement"""
    
    host: str = os.getenv("ETL_PG_HOST", "localhost")
    port: int = int(os.getenv("ETL_PG_PORT", "5432"))
    database: str = os.getenv("ETL_PG_DATABASE", "etl_db")
    user: str = os.getenv("ETL_PG_USER", "postgres")
    password: str = os.getenv("ETL_PG_PASSWORD", "")
    
    def __post_init__(self):
        if not self.password:
            raise ValueError("ETL_PG_PASSWORD non définie dans .env")
    
    # Schémas
    schema_raw: str = "raw"
    schema_staging: str = "staging_etl"
    schema_ods: str = "ods"
    schema_prep: str = "prep"
    schema_marts: str = "marts"
    schema_metadata: str = "metadata"
    schema_logs: str = "etl_logs"
    schema_sftp: str = "sftp_monitoring"
    
    def get_connection_string(self, schema: Optional[str] = None) -> str:
        """Retourne la connection string psycopg2"""
        conn_str = (
            f"host={self.host} port={self.port} "
            f"dbname={self.database} user={self.user} password={self.password}"
        )
        if schema:
            conn_str += f" options='-c search_path={schema}'"
        return conn_str
    
    def get_sqlalchemy_url(self, schema: Optional[str] = None) -> str:
        """Retourne l'URL SQLAlchemy"""
        url = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
        if schema:
            url += f"?options=-csearch_path%3D{schema}"
        return url

@dataclass
class SFTPConfig:
    """Configuration SFTP"""
    sftp_root_dir: Path = Path(os.getenv("ETL_SFTP_ROOT", "/data/sftp_cbmdata01"))
    
    @property
    def sftp_db_metadata_dir(self) -> Path:
        return self.sftp_root_dir / "Incoming" / "db_metadata"
    
    @property
    def sftp_parquet_dir(self) -> Path:
        return self.sftp_root_dir / "Incoming" / "data" / "parquet"
    
    @property
    def sftp_metadata_dir(self) -> Path:
        return self.sftp_root_dir / "Incoming" / "data" / "metadata"
    
    @property
    def sftp_status_dir(self) -> Path:
        return self.sftp_root_dir / "Incoming" / "data" / "status"
    
    @property
    def sftp_processed_dir(self) -> Path:
        return self.sftp_root_dir / "Processed"
    
    @property
    def sftp_logs_dir(self) -> Path:
        return self.sftp_root_dir / "Logs"

@dataclass
class PathsConfig:
    """Configuration des chemins"""
    log_dir: Path = Path(os.getenv("ETL_LOG_DIR", "/data/prefect/projects/ETL/logs"))
    dbt_project_dir: Path = Path(os.getenv("ETL_DBT_PROJECT", "/data/prefect/projects/ETL/dbt/etl_db"))

@dataclass
class PrefectConfig:
    """Configuration Prefect"""
    server_ip: str = os.getenv("ETL_SERVER_IP", "172.30.27.14")
    api_port: int = int(os.getenv("PREFECT_SERVER_API_PORT", "4200"))
    
    @property
    def api_url(self) -> str:
        return f"http://{self.server_ip}:{self.api_port}/api"
    
    @property
    def ui_url(self) -> str:
        return f"http://{self.server_ip}:{self.api_port}"

# =============================================================================
# ALERTING CONFIGURATION
# =============================================================================

# Email SMTP (Proginov interne)
EMAIL_CONFIG = {
    "host": os.getenv("SMTP_SERVER", "mail.proginov.fr"),
    "port": int(os.getenv("SMTP_PORT", "25")),
    "use_tls": False,
    "username": None,
    "password": None,
    "sender": os.getenv("ALERT_FROM_EMAIL", "cbm_etl@cbmcompany.com"),
    "sender_name": os.getenv("ALERT_SENDER_NAME", "Admin S918_ETL"),
    "recipients": os.getenv("ALERT_TO_EMAILS", "e.ragueneau@cbmcompany.com").split(",")
}

# Microsoft Teams (Power Automate webhook)
TEAMS_WEBHOOK_URL = os.getenv(
    "TEAMS_WEBHOOK_URL",
    ""
)

# Routage alertes par niveau
ALERT_ROUTING = {
    "info": ["teams"],
    "warning": ["teams"],
    "error": ["teams", "email"],
    "critical": ["teams", "email"]
}

# Instances globales
config = PostgreSQLConfig()
sftp_config = SFTPConfig()
paths_config = PathsConfig()
prefect_config = PrefectConfig()