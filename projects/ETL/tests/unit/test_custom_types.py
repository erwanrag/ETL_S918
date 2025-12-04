import pytest
from utils.custom_types import get_pg_type, build_table_columns_sql

def test_get_pg_type_basic():
    assert get_pg_type("integer", "int", None, None, 0) == "INTEGER"
    assert get_pg_type("logical", "bit", None, None, 0) == "BOOLEAN"

def test_extent_always_text():
    result = get_pg_type("integer", "int", None, None, 3)
    assert result == "TEXT"

def test_build_columns_sql():
    cols_meta = {
        'id': {
            'ColumnName': 'id',
            'ProgressType': 'integer',
            'DataType': 'int',
            'Width': None,
            'Scale': None,
            'Extent': 0
        }
    }
    result = build_table_columns_sql(cols_meta)
    assert '"id"' in result
    assert 'INTEGER' in result