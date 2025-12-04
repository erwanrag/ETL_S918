# tests/unit/test_extent_handler.py
import pytest
from utils.extent_handler import (
    has_extent_columns,
    get_extent_columns_with_metadata,
    generate_column_comments
)

def test_has_extent_columns():
    """Test détection colonnes extent"""
    # Remplacer 'client' par une vraie table avec extent si besoin
    result = has_extent_columns('client')
    assert isinstance(result, bool)

def test_get_extent_metadata(test_table):
    """Test récupération metadata extent"""
    result = get_extent_columns_with_metadata(test_table)
    assert isinstance(result, dict)
    
    for col, meta in result.items():
        assert 'extent' in meta
        assert 'label' in meta
        assert meta['extent'] > 0

def test_generate_comments():
    """Test génération commentaires SQL - skip si pas de vraies données"""
    pytest.skip("Nécessite vraie table avec extent dans metadata.proginovcolumns")