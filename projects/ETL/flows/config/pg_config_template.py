"""
============================================================================
Configuration PostgreSQL Template (à copier vers pg_config.py)
============================================================================
⚠️ NE PAS MODIFIER CE FICHIER DIRECTEMENT
⚠️ Copier vers pg_config.py et remplir avec vos credentials
============================================================================
"""

import os
from dataclasses import dataclass
from typing import Optional

@dataclass
class PostgreSQLConfig:
    """Configuration de connexion PostgreSQL"""
    
    # Connexion PostgreSQL - À PERSONNALISER
    host: str = os.getenv("PG_HOST", "localhost")
    port: int = int(os.getenv("PG_PORT", "5432"))
    database: str = os.getenv("PG_DATABASE", "etl_db")
    user: str = os.getenv("PG_USER", "postgres")
    password: str = os.getenv("PG_PASSWORD", "CHANGE_ME")  # ⚠️ À CHANGER
    
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
    sftp_root_dir: str = r"E:\SFTP_Mirroring"
    sftp_db_metadata_dir: str = r"E:\SFTP_Mirroring\Incoming\db_metadata"
    sftp_parquet_dir: str = r"E:\SFTP_Mirroring\Incoming\data\parquet"
    sftp_metadata_dir: str = r"E:\SFTP_Mirroring\Incoming\data\metadata"
    sftp_status_dir: str = r"E:\SFTP_Mirroring\Incoming\data\status"
    sftp_processed_dir = r"E:\SFTP_Mirroring\Processed"
    sftp_logs_dir: str = r"E:\SFTP_Mirroring\Logs"
    
    # Logs Prefect
    log_dir: str = r"E:\Prefect\projects\ETL\logs"
    
    # dbt project - ✅ CORRIGÉ
    dbt_project_dir: str = r"E:\Prefect\projects\ETL\dbt\cbm_analytics"
    
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