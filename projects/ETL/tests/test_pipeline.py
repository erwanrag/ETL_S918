"""
Tests unitaires pipeline ETL
"""

import pytest
import psycopg2
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from shared.config import config, sftp_config


def test_db_connection():
    """Test connexion PostgreSQL"""
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    cur.execute("SELECT 1")
    assert cur.fetchone()[0] == 1
    cur.close()
    conn.close()


def test_sftp_directory_exists():
    """Test existence répertoire SFTP"""
    sftp_path = sftp_config.sftp_parquet_dir
    assert sftp_path.exists(), f"SFTP dir manquant : {sftp_path}"


def test_schemas_exist():
    """Test existence schémas PostgreSQL"""
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    schemas = ['raw', 'staging_etl', 'ods', 'metadata']
    
    for schema in schemas:
        cur.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.schemata 
                WHERE schema_name = %s
            )
        """, (schema,))
        assert cur.fetchone()[0], f"Schéma {schema} manquant"
    
    cur.close()
    conn.close()


def test_metadata_loaded():
    """Test metadata Progress chargées"""
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    cur.execute("SELECT COUNT(*) FROM metadata.proginovcolumns")
    count = cur.fetchone()[0]
    assert count > 0, "Metadata vide"
    
    cur.close()
    conn.close()


def test_ods_has_data():
    """Test ODS contient données"""
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    # Tester quelques tables clés
    tables = ['fournis', 'client']
    
    for table in tables:
        cur.execute(f"SELECT COUNT(*) FROM ods.{table}")
        count = cur.fetchone()[0]
        assert count > 0, f"Table ods.{table} vide"
    
    cur.close()
    conn.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])