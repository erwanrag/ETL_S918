"""
============================================================================
Configuration Métadonnées Tables
============================================================================
Fichier :/data/Prefect/projects/ETL/flows/config/table_metadata.py

Lit dynamiquement les clés primaires depuis metadata.etl_tables
============================================================================
"""

import psycopg2
from typing import Optional, List, Dict
import sys
import logging

# Configure logging
logger = logging.getLogger(__name__)


from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))
from shared.config import config

_TABLE_METADATA_CACHE: Dict[str, dict] = {}
_CACHE_LOADED = False


def load_table_metadata() -> Dict[str, dict]:
    """
    Charge metadata pour toutes les tables actives
    Cache par TableName ET par ConfigName
    """
    global _TABLE_METADATA_CACHE, _CACHE_LOADED
    
    if _CACHE_LOADED:
        return _TABLE_METADATA_CACHE
    
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    try:
        cur.execute("""
            SELECT 
                "TableName",
                "ConfigName",
                "PrimaryKeyCols",
                "HasTimestamps",
                "Notes"
            FROM metadata.etl_tables
            WHERE "IsActive" = TRUE
        """)
        
        rows = cur.fetchall()
        
        for row in rows:
            table_name = row[0].lower()
            config_name = row[1]
            pk_columns_str = row[2]
            has_timestamps = row[3]
            description = row[4]
            
            if pk_columns_str:
                pk_columns = [col.strip() for col in pk_columns_str.split(',')]
            else:
                pk_columns = []
            
            # PK spéciales (hashdiff, uniq_id) = pas de vraie PK business
            is_special_pk = pk_columns_str in ['hashdiff', 'uniq_id'] if pk_columns_str else False
            
            # Force FULL si pas de PK ou PK spéciale
            has_usable_pk = bool(pk_columns) and not is_special_pk
            force_full = not has_usable_pk
            
            metadata = {
                'pk_columns': pk_columns,
                'has_pk': has_usable_pk,
                'has_timestamps': has_timestamps,
                'force_full': force_full,
                'description': description or ''
            }
            
            # [FIX] Stocker par TableName
            _TABLE_METADATA_CACHE[table_name] = metadata
            
            # [FIX] Stocker AUSSI par ConfigName si existe
            if config_name and config_name != table_name:
                _TABLE_METADATA_CACHE[config_name.lower()] = metadata
        
        _CACHE_LOADED = True
        logger.info(f"Metadata loaded: {len(rows)} configs")
        
    finally:
        cur.close()
        conn.close()
    
    return _TABLE_METADATA_CACHE


def get_table_metadata(table_name: str) -> Optional[dict]:
    """
    Récupère metadata par TableName OU ConfigName
    
    Args:
        table_name: 'client' ou 'lisval_fou_production'
    """
    metadata = load_table_metadata()
    return metadata.get(table_name.lower())


def get_primary_keys(table_name: str) -> List[str]:
    """
    Récupère les colonnes PK pour une table
    
    Args:
        table_name: 'client' ou 'lisval_fou_production'
    
    Returns:
        Liste des colonnes PK, ex: ['cod_cli'] ou ['cod_pro', 'cod_fou']
    """
    # Essayer d'abord le cache
    meta = get_table_metadata(table_name)
    if meta:
        return meta.get('pk_columns', [])
    
    # Sinon lookup direct en base
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    try:
        cur.execute("""
            SELECT "PrimaryKeyCols"
            FROM metadata.etl_tables
            WHERE "ConfigName" = %s OR "TableName" = %s
            LIMIT 1
        """, (table_name, table_name))
        
        row = cur.fetchone()
        
        if row and row[0]:
            pk_columns = [col.strip() for col in row[0].split(',')]
            return pk_columns
        else:
            return []
            
    finally:
        cur.close()
        conn.close()


def has_primary_key(table_name: str) -> bool:
    """
    Vérifie si la table a une PK utilisable
    
    Args:
        table_name: 'client' ou 'lisval_fou_production'
    """
    meta = get_table_metadata(table_name)
    if not meta:
        return False
    return meta.get('has_pk', False)


def should_force_full(table_name: str) -> bool:
    """
    Détermine si la table doit forcer mode FULL
    
    Args:
        table_name: 'client' ou 'lisval_fou_production'
    """
    meta = get_table_metadata(table_name)
    if not meta:
        return True
    return meta.get('force_full', False)


def get_optimal_load_mode(table_name: str) -> str:
    """
    Détermine le meilleur load_mode pour une table
    
    Returns:
        'INCREMENTAL', 'FULL', ou 'FULL_RESET'
    """
    if should_force_full(table_name):
        return "FULL"
    if has_primary_key(table_name):
        return "INCREMENTAL"
    return "FULL"


def get_table_description(table_name: str) -> str:
    """
    Récupère la description d'une table
    
    Args:
        table_name: 'client' ou 'lisval_fou_production'
    """
    meta = get_table_metadata(table_name)
    if meta:
        return meta.get('description', '')
    return ''


def reload_metadata():
    """Force le rechargement du cache metadata"""
    global _CACHE_LOADED
    _CACHE_LOADED = False
    _TABLE_METADATA_CACHE.clear()
    load_table_metadata()


# ============================================================
# Chargement initial au démarrage
# ============================================================
try:
    load_table_metadata()
except Exception as e:
    # Use logging instead of print to avoid closed file errors
    logger.warning(f"Could not load metadata at module import: {e}")
    # Continue anyway - metadata will be loaded on first use