"""
============================================================================
Configuration PostgreSQL pour Prefect Flows
============================================================================
Fichier : E:\Prefect\projects\ETL\flows\config\pg_config.py

[WARN] SECURITÉ : Utilise fichier .env pour les credentials
[WARN] Ce fichier peut être versionné (pas de secrets en dur)
============================================================================
"""

import os
from pathlib import Path
from dataclasses import dataclass
from typing import Optional

# Charger le fichier .env automatiquement
def load_env_file():
    """Charge les variables depuis .env si le fichier existe"""
    env_file = Path(__file__).parent.parent.parent / ".env"
    
    if env_file.exists():
        with open(env_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                # Ignorer commentaires et lignes vides
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key.strip()] = value.strip()

# Charger .env au démarrage
load_env_file()

@dataclass
class PostgreSQLConfig:
    """Configuration de connexion PostgreSQL via variables d'environnement"""
    
    # Connexion PostgreSQL - Variables d'environnement
    host: str = os.getenv("ETL_PG_HOST", "localhost")
    port: int = int(os.getenv("ETL_PG_PORT", "5432"))
    database: str = os.getenv("ETL_PG_DATABASE", "etl_db")
    user: str = os.getenv("ETL_PG_USER", "postgres")
    password: str = os.getenv("ETL_PG_PASSWORD", "")  # [WARN] Doit être défini en env
    
    # Validation au démarrage
    def __post_init__(self):
        if not self.password:
            raise ValueError(
                "[ERROR] Variable d'environnement ETL_PG_PASSWORD non définie!\n"
                "Exécuter en tant qu'ADMIN : \n"
                "  cd E:\\Prefect\\projects\\ETL\\scripts\n"
                "  .\\setup_env_vars.ps1\n"
                "Puis redémarrer PowerShell"
            )
    
    # Schémas
    schema_raw: str = "raw"
    schema_staging: str = "staging"
    schema_ods: str = "ods"
    schema_marts: str = "marts"
    schema_logs: str = "etl_logs"
    schema_sftp: str = "sftp_monitoring"
    
    # -------------------------------------------------------------------------
    # SFTP MIRRORING LOCAL (S918)
    # -------------------------------------------------------------------------
    #
    # [WARN] IMPORTANT :
    # - CBM_DATA01 envoie → /Incoming/data/parquet, /metadata, /status
    # - Un service (WinSCP, robocopy, script, scheduled task…) synchronise
    #   tout vers E:\SFTP_Mirroring sur le serveur S918.
    # - Prefect lit EXCLUSIVEMENT ce miroir local.
    # -------------------------------------------------------------------------

    sftp_root_dir: str = os.getenv("ETL_SFTP_ROOT", r"E:\SFTP_Mirroring")

    # Incoming / db_metadata (Vient de CBM_DATA01 tous les matins)
    @property
    def sftp_db_metadata_dir(self) -> str:
        return os.path.join(self.sftp_root_dir, "Incoming", "db_metadata")
    
    # Incoming / Data Business
    @property
    def sftp_parquet_dir(self) -> str:
        return os.path.join(self.sftp_root_dir, "Incoming", "data", "parquet")
    
    @property
    def sftp_metadata_dir(self) -> str:
        return os.path.join(self.sftp_root_dir, "Incoming", "data", "metadata")
    
    @property
    def sftp_status_dir(self) -> str:
        return os.path.join(self.sftp_root_dir, "Incoming", "data", "status")
    
    # Processed : archives créées par Prefect
    @property
    def sftp_processed_dir(self) -> str:
        return os.path.join(self.sftp_root_dir, "Processed")
    
    # Logs SFTP (générés par le mirroring)
    @property
    def sftp_logs_dir(self) -> str:
        return os.path.join(self.sftp_root_dir, "Logs")
    
    # Prefect logs
    log_dir: str = os.getenv("ETL_LOG_DIR", r"E:\Prefect\projects\ETL\logs")
    
    # dbt project
    dbt_project_dir: str = os.getenv(
        "ETL_DBT_PROJECT", 
        r"E:\Prefect\projects\ETL\dbt\etl_db"
    )
    
    # -------------------------------------------------------------------------

    def get_connection_string(self, schema: Optional[str] = None) -> str:
        """Retourne la connection string psycopg2."""
        conn_str = (
            f"host={self.host} port={self.port} "
            f"dbname={self.database} user={self.user} password={self.password}"
        )
        if schema:
            conn_str += f" options='-c search_path={schema}'"
        return conn_str
    
    def get_sqlalchemy_url(self, schema: Optional[str] = None) -> str:
        """Retourne l'URL SQLAlchemy."""
        url = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
        if schema:
            url += f"?options=-csearch_path%3D{schema}"
        return url


# Instance globale
config = PostgreSQLConfig()