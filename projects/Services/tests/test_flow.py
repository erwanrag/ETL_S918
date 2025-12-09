"""
============================================================================
Tests Unitaires - Services Module
============================================================================
"""

import pytest
import psycopg2
import sys
from pathlib import Path
from datetime import date

# Ajouter Services au path
SERVICES_PATH = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(SERVICES_PATH))

from config.pg_config import config

# =============================================================================
# FIXTURES
# =============================================================================

@pytest.fixture(scope="session")
def db_connection():
    """Connexion PostgreSQL pour tests"""
    conn = psycopg2.connect(config.get_connection_string())
    yield conn
    conn.close()

# =============================================================================
# TESTS CONFIGURATION
# =============================================================================

def test_config_exists():
    """Test : config existe et est valide"""
    assert config is not None
    assert config.host is not None
    assert config.database is not None
    assert config.user is not None


def test_connection_string():
    """Test : connection string valide"""
    conn_str = config.get_connection_string()
    assert "host=" in conn_str
    assert "dbname=" in conn_str
    assert "user=" in conn_str


# =============================================================================
# TESTS POSTGRESQL
# =============================================================================

def test_db_connection(db_connection):
    """Test : connexion PostgreSQL fonctionne"""
    cur = db_connection.cursor()
    cur.execute("SELECT 1")
    result = cur.fetchone()
    cur.close()
    assert result[0] == 1


def test_schema_reference_exists(db_connection):
    """Test : schéma reference existe"""
    cur = db_connection.cursor()
    cur.execute("""
        SELECT EXISTS (
            SELECT 1 FROM information_schema.schemata 
            WHERE schema_name = 'reference'
        )
    """)
    exists = cur.fetchone()[0]
    cur.close()
    assert exists, "Schéma 'reference' n'existe pas. Exécuter sql/create_tables.sql"


def test_table_currencies_exists(db_connection):
    """Test : table currencies existe"""
    cur = db_connection.cursor()
    cur.execute("""
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables 
            WHERE table_schema = 'reference' 
            AND table_name = 'currencies'
        )
    """)
    exists = cur.fetchone()[0]
    cur.close()
    assert exists, "Table 'reference.currencies' n'existe pas"


def test_table_currency_rates_exists(db_connection):
    """Test : table currency_rates existe"""
    cur = db_connection.cursor()
    cur.execute("""
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables 
            WHERE table_schema = 'reference' 
            AND table_name = 'currency_rates'
        )
    """)
    exists = cur.fetchone()[0]
    cur.close()
    assert exists, "Table 'reference.currency_rates' n'existe pas"


def test_table_time_dimension_exists(db_connection):
    """Test : table time_dimension existe"""
    cur = db_connection.cursor()
    cur.execute("""
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables 
            WHERE table_schema = 'reference' 
            AND table_name = 'time_dimension'
        )
    """)
    exists = cur.fetchone()[0]
    cur.close()
    assert exists, "Table 'reference.time_dimension' n'existe pas"


# =============================================================================
# TESTS FONCTIONNELS (si données présentes)
# =============================================================================

def test_currencies_has_data(db_connection):
    """Test : currencies contient des données (si chargées)"""
    cur = db_connection.cursor()
    cur.execute("SELECT COUNT(*) FROM reference.currencies")
    count = cur.fetchone()[0]
    cur.close()
    
    # Test passe si table vide OU avec données (>=150 codes attendus)
    if count > 0:
        assert count >= 150, f"Seulement {count} devises (attendu >=150)"


def test_time_dimension_has_data(db_connection):
    """Test : time_dimension contient des données (si chargées)"""
    cur = db_connection.cursor()
    cur.execute("SELECT COUNT(*) FROM reference.time_dimension")
    count = cur.fetchone()[0]
    cur.close()
    
    # Test passe si table vide OU avec données (7,671 jours attendus)
    if count > 0:
        assert count >= 7000, f"Seulement {count} jours (attendu >=7000)"


def test_time_dimension_date_range(db_connection):
    """Test : time_dimension couvre période 2015-2035 (si chargée)"""
    cur = db_connection.cursor()
    cur.execute("""
        SELECT MIN(date_id), MAX(date_id) 
        FROM reference.time_dimension
    """)
    result = cur.fetchone()
    cur.close()
    
    if result[0] is not None:
        min_date = result[0]
        max_date = result[1]
        
        assert min_date <= date(2015, 1, 1)
        assert max_date >= date(2035, 12, 31)


# =============================================================================
# TESTS MODULES
# =============================================================================

def test_import_flows():
    """Test : imports flows fonctionnent"""
    from flows import (
        load_currency_codes_flow,
        load_exchange_rates_flow,
        build_time_dimension_flow
    )
    
    assert callable(load_currency_codes_flow)
    assert callable(load_exchange_rates_flow)
    assert callable(build_time_dimension_flow)


# =============================================================================
# EXECUTION
# =============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v"])