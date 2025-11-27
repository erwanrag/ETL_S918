"""
============================================================================
Metadata Helper - Récupération métadonnées Progress
============================================================================
Fichier : utils/metadata_helper.py

Fonctions pour récupérer métadonnées depuis PostgreSQL :
- Structure colonnes (ProginovColumns)
- Primary keys (ProginovIndexes)
- Configuration tables (etl_tables)
============================================================================
"""

import psycopg2
from typing import List, Dict, Optional
import sys
from pathlib import Path

# Ajouter le chemin du projet
sys.path.append(str(Path(__file__).parent.parent))
from flows.config.pg_config import config


def get_connection():
    """Créer connexion PostgreSQL"""
    return psycopg2.connect(config.get_connection_string())


def get_table_columns(table_name: str) -> List[Dict[str, any]]:
    """
    Récupérer structure colonnes depuis metadata.ProginovColumns
    
    Returns:
        List[Dict] avec keys: column_name, data_type, is_mandatory
    """
    conn = get_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("""
            SELECT 
                "ColumnName" as column_name,
                "DataType" as data_type,
                "IsMandatory" as is_mandatory,
                "Width" as width,
                "Scale" as scale
            FROM metadata.proginovcolumns
            WHERE "TableName" = %s
            ORDER BY "ProgressOrder"
        """, (table_name,))
        
        columns = []
        for row in cur.fetchall():
            columns.append({
                'column_name': row[0],
                'data_type': row[1],
                'is_mandatory': row[2],
                'width': row[3],
                'scale': row[4]
            })
        
        return columns
        
    finally:
        cur.close()
        conn.close()


def get_primary_keys(table_name: str) -> List[str]:
    """
    Récupérer primary keys depuis metadata.ProginovIndexes
    
    Returns:
        List[str] des noms de colonnes PK
    """
    conn = get_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("""
            SELECT "ColumnName"
            FROM metadata.proginovindexes
            WHERE "TableName" = %s
              AND "IsPrimary" = true
            ORDER BY "SeqNo"
        """, (table_name,))
        
        return [row[0] for row in cur.fetchall()]
        
    finally:
        cur.close()
        conn.close()


def get_table_config(table_name: str) -> Optional[Dict[str, any]]:
    """
    Récupérer config table depuis metadata.etl_tables
    
    Returns:
        Dict avec keys: is_active, frequency, load_mode, primary_keys
    """
    conn = get_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("""
            SELECT 
                "IsActive" as is_active,
                "Frequency" as frequency,
                "PrimaryKeyCols" as primary_keys
            FROM metadata.etl_tables
            WHERE "TableName" = %s
        """, (table_name,))
        
        row = cur.fetchone()
        if not row:
            return None
        
        return {
            'is_active': row[0],
            'frequency': row[1],
            'primary_keys': [pk.strip() for pk in row[2].split(',')] if row[2] else [],
            'load_mode': 'INCREMENTAL'  # Par défaut
        }
        
    finally:
        cur.close()
        conn.close()


def get_all_active_tables() -> List[str]:
    """
    Récupérer liste des tables actives
    
    Returns:
        List[str] des noms de tables
    """
    conn = get_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("""
            SELECT "TableName"
            FROM metadata.etl_tables
            WHERE "IsActive" = true
            ORDER BY "TableName"
        """)
        
        return [row[0] for row in cur.fetchall()]
        
    finally:
        cur.close()
        conn.close()


def map_progress_to_postgres(progress_type: str, width: Optional[int] = None, scale: Optional[int] = None) -> str:
    """
    Mapper type Progress → PostgreSQL
    
    Args:
        progress_type: Type Progress (ex: 'character', 'integer', 'decimal')
        width: Largeur colonne
        scale: Précision décimale
    
    Returns:
        str: Type PostgreSQL (ex: 'VARCHAR(50)', 'INTEGER', 'NUMERIC(10,2)')
    """
    type_map = {
        'character': f'VARCHAR({width})' if width else 'VARCHAR(255)',
        'varchar': f'VARCHAR({width})' if width else 'VARCHAR(255)',
        'integer': 'INTEGER',
        'int': 'INTEGER',
        'bigint': 'BIGINT',
        'smallint': 'SMALLINT',
        'decimal': f'NUMERIC({width},{scale})' if width and scale else 'NUMERIC',
        'numeric': f'NUMERIC({width},{scale})' if width and scale else 'NUMERIC',
        'float': 'DOUBLE PRECISION',
        'double': 'DOUBLE PRECISION',
        'date': 'DATE',
        'datetime': 'TIMESTAMP',
        'timestamp': 'TIMESTAMP',
        'boolean': 'BOOLEAN',
        'logical': 'BOOLEAN',
        'text': 'TEXT',
        'blob': 'BYTEA',
        'clob': 'TEXT'
    }
    
    pg_type = type_map.get(progress_type.lower(), 'TEXT')
    return pg_type


def get_business_columns(table_name: str) -> List[str]:

    # auto-remove prefix raw_ ou stg_ si présent
    if table_name.lower().startswith(("raw_", "staging_", "stg_")):
        table_name = table_name.split("_", 1)[1]

    columns = get_table_columns(table_name)

    if not columns:
        return []

    return [
        col['column_name']
        for col in columns
        if not col['column_name'].startswith('_etl_')
    ]

def normalize_column_name(col_name: str) -> str:
    """
    Normaliser nom de colonne SQL → Python/dbt
    
    Args:
        col_name: Nom colonne original (ex: "cod_cli", "TabPart_client")
    
    Returns:
        str: Nom normalisé (ex: "cod_cli", "tabpart_client")
    """
    # Supprimer guillemets
    col_name = col_name.strip('"').strip()
    
    # Remplacer tirets et espaces par underscore
    col_name = col_name.replace('-', '_').replace(' ', '_')
    
    # Convertir en lowercase si commence par majuscule
    if col_name and col_name[0].isupper():
        col_name = col_name.lower()
    
    return col_name


# ============================================================================
# TESTS
# ============================================================================

if __name__ == "__main__":
    # Test récupération métadonnées
    print("=" * 60)
    print("TEST METADATA HELPER")
    print("=" * 60)
    
    # Test 1: Liste tables actives
    print("\n1. Tables actives:")
    tables = get_all_active_tables()
    print(f"   {len(tables)} table(s): {', '.join(tables[:5])}...")
    
    # Test 2: Colonnes d'une table
    if tables:
        test_table = tables[0]
        print(f"\n2. Colonnes de {test_table}:")
        columns = get_table_columns(test_table)
        print(f"   {len(columns)} colonne(s)")
        for col in columns[:3]:
            pg_type = map_progress_to_postgres(
                col['data_type'], 
                col['width'], 
                col['scale']
            )
            print(f"   - {col['column_name']}: {col['data_type']} → {pg_type}")
    
    # Test 3: Primary keys
        print(f"\n3. Primary keys de {test_table}:")
        pks = get_primary_keys(test_table)
        print(f"   {pks}")
    
    # Test 4: Config table
        print(f"\n4. Config de {test_table}:")
        config_table = get_table_config(test_table)
        if config_table:
            print(f"   Active: {config_table['is_active']}")
            print(f"   Frequency: {config_table['frequency']}")
            print(f"   PK: {config_table['primary_keys']}")
    
    print("\n" + "=" * 60)