"""
============================================================================
DDL Generator - Génération DDL automatique
============================================================================
Fichier : utils/ddl_generator.py

Génération DDL pour tables RAW et ODS depuis métadonnées Progress
============================================================================
"""

import psycopg2
from typing import List, Dict
import sys
from pathlib import Path

# Ajouter le chemin du projet
sys.path.append(str(Path(__file__).parent.parent))
from flows.config.pg_config import config
from utils.metadata_helper import (
    get_table_columns,
    get_primary_keys,
    map_progress_to_postgres,
    normalize_column_name
)


def generate_raw_table_ddl(table_name: str) -> str:
    """
    Générer DDL pour table RAW
    
    Args:
        table_name: Nom de la table
    
    Returns:
        str: DDL CREATE TABLE
    """
    columns = get_table_columns(table_name)
    
    col_defs = []
    
    # Colonnes métier depuis Progress
    for col in columns:
        col_name = col['column_name']
        pg_type = map_progress_to_postgres(
            col['data_type'],
            col['width'],
            col['scale']
        )
        nullable = '' if col['is_mandatory'] else ''  # Pas de NOT NULL en RAW
        
        col_defs.append(f'    "{col_name}" {pg_type}{nullable}')
    
    # Colonnes techniques RAW
    col_defs.extend([
        '    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP',
        '    _source_file VARCHAR(500)',
        '    _sftp_log_id BIGINT'
    ])
    
    # DDL
    cols_joined = ',\n'.join(col_defs)
    ddl = f"""CREATE TABLE IF NOT EXISTS raw.raw_{table_name.lower()} (
{cols_joined}
);

-- Index sur colonnes techniques
CREATE INDEX IF NOT EXISTS idx_raw_{table_name.lower()}_loaded 
    ON raw.raw_{table_name.lower()}(_loaded_at);
"""
    
    return ddl


def generate_ods_table_ddl(table_name: str) -> str:
    """
    Générer DDL pour table ODS avec PK et colonnes techniques
    
    Args:
        table_name: Nom de la table
    
    Returns:
        str: DDL CREATE TABLE
    """
    columns = get_table_columns(table_name)
    pk_cols = get_primary_keys(table_name)
    
    col_defs = []
    
    # Colonnes métier depuis Progress
    for col in columns:
        col_name = col['column_name']
        pg_type = map_progress_to_postgres(
            col['data_type'],
            col['width'],
            col['scale']
        )
        
        # NOT NULL sur PK
        is_pk = col_name in pk_cols
        nullable = ' NOT NULL' if is_pk or col['is_mandatory'] else ''
        
        col_defs.append(f'    "{col_name}" {pg_type}{nullable}')
    
    # Colonnes techniques ODS
    col_defs.extend([
        '    _etl_loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP',
        '    _etl_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP',
        '    _etl_run_id UUID',
        '    _etl_source VARCHAR(50) DEFAULT \'CBM_DATA01\'',
        '    _etl_hashdiff VARCHAR(64)',
        '    _etl_is_active BOOLEAN DEFAULT TRUE',
        '    _etl_valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP',
        '    _etl_valid_to TIMESTAMP'
    ])
    
    # Primary key constraint
    if pk_cols:
        pk_constraint = ', '.join([f'"{pk}"' for pk in pk_cols])
        col_defs.append(f'    PRIMARY KEY ({pk_constraint})')
    
    # DDL
    cols_joined = ',\n'.join(col_defs)
    ddl = f"""CREATE TABLE IF NOT EXISTS ods.{table_name.lower()} (
{cols_joined}
);

-- Index sur colonnes techniques
CREATE INDEX IF NOT EXISTS idx_ods_{table_name.lower()}_hashdiff 
    ON ods.{table_name.lower()}(_etl_hashdiff);

CREATE INDEX IF NOT EXISTS idx_ods_{table_name.lower()}_updated 
    ON ods.{table_name.lower()}(_etl_updated_at);

CREATE INDEX IF NOT EXISTS idx_ods_{table_name.lower()}_run 
    ON ods.{table_name.lower()}(_etl_run_id);
"""
    
    return ddl


def generate_ods_extent_table_ddl(
    table_name: str,
    ods_columns: List[str],
    column_types: Dict[str, str]
) -> str:
    """
    Générer DDL pour table ODS avec extent éclaté et types corrects
    
    Args:
        table_name: Nom de la table (ex: 'client')
        ods_columns: Liste colonnes ODS finales (avec extent éclatés)
        column_types: Dict {colonne: type_pg} pour colonnes extent éclatées
    
    Returns:
        str: DDL CREATE TABLE avec types corrects
    """
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    try:
        # Récupérer types depuis STAGING pour colonnes non-extent
        cur.execute(f"""
            SELECT 
                column_name,
                CASE 
                    WHEN data_type = 'character varying' THEN 
                        'VARCHAR(' || COALESCE(character_maximum_length::text, '255') || ')'
                    WHEN data_type = 'numeric' THEN 
                        'NUMERIC(' || numeric_precision || ',' || numeric_scale || ')'
                    WHEN data_type = 'integer' THEN 'INTEGER'
                    WHEN data_type = 'bigint' THEN 'BIGINT'
                    WHEN data_type = 'boolean' THEN 'BOOLEAN'
                    WHEN data_type = 'date' THEN 'DATE'
                    WHEN data_type = 'timestamp without time zone' THEN 'TIMESTAMP'
                    WHEN data_type = 'text' THEN 'TEXT'
                    ELSE 'TEXT'
                END as pg_type
            FROM information_schema.columns
            WHERE table_schema = 'staging_etl'
              AND table_name = 'stg_{table_name.lower()}'
            ORDER BY ordinal_position
        """)
        
        staging_types = {row[0]: row[1] for row in cur.fetchall()}
        
        # Récupérer PK depuis metadata
        pk_cols = get_primary_keys(table_name)
        
    finally:
        cur.close()
        conn.close()
    
    col_defs = []
    
    # Liste des colonnes techniques à ne pas dupliquer
    technical_cols = {
        '_etl_loaded_at', '_etl_updated_at', '_etl_run_id', 
        '_etl_source', '_etl_hashdiff', '_etl_is_active', 
        '_etl_valid_from', '_etl_valid_to'
    }
    
    # Construire définitions colonnes (SANS colonnes techniques)
    for col in ods_columns:
        # Ignorer colonnes techniques (déjà dans staging)
        if col in technical_cols:
            continue
        
        # ✅ PRIORITÉ 1 : column_types (extent éclatés avec types corrects)
        if col in column_types:
            pg_type = column_types[col]
        
        # ✅ PRIORITÉ 2 : staging_types (colonnes non-extent)
        elif col in staging_types:
            pg_type = staging_types[col]
        
        # ❌ FALLBACK : Ne devrait JAMAIS arriver
        else:
            print(f"⚠️ WARNING: Colonne {col} sans type défini, fallback TEXT")
            pg_type = 'TEXT'
        
        # NOT NULL sur PK uniquement
        is_pk = col in pk_cols
        nullable = ' NOT NULL' if is_pk else ''
        
        col_defs.append(f'    "{col}" {pg_type}{nullable}')
    
    # Colonnes techniques ODS (ajoutées UNE SEULE FOIS)
    col_defs.extend([
        '    _etl_loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP',
        '    _etl_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP',
        '    _etl_run_id VARCHAR(50)',
        '    _etl_source VARCHAR(50) DEFAULT \'CBM_DATA01\'',
        '    _etl_hashdiff VARCHAR(64)',
        '    _etl_is_active BOOLEAN DEFAULT TRUE',
        '    _etl_valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP',
        '    _etl_valid_to TIMESTAMP'
    ])
    
    # Primary key constraint
    if pk_cols:
        # Filtrer PK qui existent réellement dans ods_columns
        existing_pk = [pk for pk in pk_cols if pk in ods_columns]
        if existing_pk:
            pk_constraint = ', '.join([f'"{pk}"' for pk in existing_pk])
            col_defs.append(f'    PRIMARY KEY ({pk_constraint})')
    
    # DDL
    cols_joined = ',\n'.join(col_defs)
    ddl = f"""CREATE TABLE ods.{table_name.lower()} (
{cols_joined}
)"""
    
    return ddl

def generate_ods_indexes_ddl(table_name: str) -> str:
    """
    Générer DDL pour index ODS
    
    Args:
        table_name: Nom de la table
    
    Returns:
        str: DDL CREATE INDEX
    """
    return f"""
-- Index sur colonnes techniques ODS
CREATE INDEX IF NOT EXISTS idx_ods_{table_name.lower()}_hashdiff 
    ON ods.{table_name.lower()}(_etl_hashdiff);

CREATE INDEX IF NOT EXISTS idx_ods_{table_name.lower()}_valid_from 
    ON ods.{table_name.lower()}(_etl_valid_from);

CREATE INDEX IF NOT EXISTS idx_ods_{table_name.lower()}_run 
    ON ods.{table_name.lower()}(_etl_run_id);
"""



def create_table_if_not_exists(table_name: str, schema: str):
    """
    Créer table RAW ou ODS si elle n'existe pas
    
    Args:
        table_name: Nom de la table
        schema: 'raw' ou 'ods'
    """
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    try:
        if schema == 'raw':
            ddl = generate_raw_table_ddl(table_name)
        elif schema == 'ods':
            ddl = generate_ods_table_ddl(table_name)
        else:
            raise ValueError(f"Schema invalide: {schema}")
        
        cur.execute(ddl)
        conn.commit()
        
        print(f"✅ Table {schema}.{table_name.lower()} créée ou vérifiée")
        
    except Exception as e:
        conn.rollback()
        print(f"❌ Erreur création table {schema}.{table_name}: {e}")
        raise
        
    finally:
        cur.close()
        conn.close()


def generate_all_tables_ddl(schema: str, tables: List[str] = None) -> str:
    """
    Générer DDL pour toutes les tables
    
    Args:
        schema: 'raw' ou 'ods'
        tables: Liste tables (None = toutes les tables actives)
    
    Returns:
        str: DDL complet
    """
    from utils.metadata_helper import get_all_active_tables
    
    if tables is None:
        tables = get_all_active_tables()
    
    ddl_parts = [f"-- ============================================================================"]
    ddl_parts.append(f"-- DDL Tables {schema.upper()}")
    ddl_parts.append(f"-- Généré automatiquement depuis métadonnées Progress")
    ddl_parts.append(f"-- ============================================================================\n")
    
    for table in tables:
        try:
            if schema == 'raw':
                ddl = generate_raw_table_ddl(table)
            else:
                ddl = generate_ods_table_ddl(table)
            
            ddl_parts.append(f"-- Table: {table}")
            ddl_parts.append(ddl)
            ddl_parts.append("")
            
        except Exception as e:
            ddl_parts.append(f"-- ❌ Erreur {table}: {e}\n")
    
    return '\n'.join(ddl_parts)


# ============================================================================
# TESTS
# ============================================================================

if __name__ == "__main__":
    print("=" * 60)
    print("TEST DDL GENERATOR")
    print("=" * 60)
    
    # Test 1: Générer DDL RAW
    print("\n1. DDL RAW pour 'client':")
    ddl_raw = generate_raw_table_ddl('client')
    print(ddl_raw[:400] + "...")
    
    # Test 2: Générer DDL ODS
    print("\n2. DDL ODS pour 'client':")
    ddl_ods = generate_ods_table_ddl('client')
    print(ddl_ods[:400] + "...")
    
    # Test 3: Compter lignes générées
    from utils.metadata_helper import get_all_active_tables
    tables = get_all_active_tables()
    print(f"\n3. Génération DDL pour {len(tables)} tables...")
    
    ddl_all = generate_all_tables_ddl('raw', tables[:3])
    lines = ddl_all.count('\n')
    print(f"   {lines} lignes de DDL générées")
    
    print("\n" + "=" * 60)