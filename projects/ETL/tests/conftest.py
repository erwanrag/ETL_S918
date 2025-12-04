# tests/conftest.py
import sys
from pathlib import Path
import pytest

# Ajouter le projet au path
sys.path.insert(0, str(Path(__file__).parent.parent))

@pytest.fixture(scope="session")
def pg_config():
    """Config PostgreSQL pour tests"""
    from flows.config.pg_config import config
    return config

@pytest.fixture(scope="session")
def test_table():
    """Table test standard"""
    return "client"