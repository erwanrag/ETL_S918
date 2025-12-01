"""
============================================================================
Configuration Métadonnées Tables
============================================================================
Fichier : E:/Prefect/projects/ETL/flows/config/table_metadata.py

Lit dynamiquement les clés primaires depuis metadata.etl_tables
============================================================================
"""

import psycopg2
from typing import Optional, List, Dict
import sys

sys.path.append(r'E:\Prefect\projects\ETL')
from flows.config.pg_config import config

_TABLE_METADATA_CACHE: Dict[str, dict] = {}
_CACHE_LOADED = False


def load_table_metadata() -> Dict[str, dict]:
    global _TABLE_METADATA_CACHE, _CACHE_LOADED
    
    if _CACHE_LOADED:
        return _TABLE_METADATA_CACHE
    
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    try:
        cur.execute("""
            SELECT 
                "TableName",
                "PrimaryKeyCols",
                "HasTimestamps",
                "Notes"
            FROM metadata.etl_tables
            WHERE "IsActive" = TRUE
        """)
        
        rows = cur.fetchall()
        
        for row in rows:
            table_name = row[0].lower()
            pk_columns_str = row[1]
            has_timestamps = row[2]
            description = row[3]
            
            if pk_columns_str:
                pk_columns = [col.strip() for col in pk_columns_str.split(',')]
            else:
                pk_columns = []
            
            # PK spéciales (hashdiff, uniq_id) = pas de vraie PK business
            is_special_pk = pk_columns_str in ['hashdiff', 'uniq_id'] if pk_columns_str else False
            
            # Force FULL si pas de PK ou PK spéciale
            has_usable_pk = bool(pk_columns) and not is_special_pk
            force_full = not has_usable_pk
            
            _TABLE_METADATA_CACHE[table_name] = {
                'pk_columns': pk_columns,
                'has_pk': has_usable_pk,
                'has_timestamps': has_timestamps,
                'force_full': force_full,
                'description': description or ''
            }
        
        _CACHE_LOADED = True
        print(f"✅ Métadonnées chargées : {len(_TABLE_METADATA_CACHE)} tables")
        
    finally:
        cur.close()
        conn.close()
    
    return _TABLE_METADATA_CACHE


def get_table_metadata(table_name: str) -> Optional[dict]:
    metadata = load_table_metadata()
    return metadata.get(table_name.lower())


def get_primary_keys(table_name: str) -> Optional[List[str]]:
    meta = get_table_metadata(table_name)
    if meta and meta['has_pk']:
        return meta['pk_columns']
    return None


def has_primary_key(table_name: str) -> bool:
    meta = get_table_metadata(table_name)
    if not meta:
        return False
    return meta.get('has_pk', False)


def should_force_full(table_name: str) -> bool:
    meta = get_table_metadata(table_name)
    if not meta:
        return True
    return meta.get('force_full', False)


def get_optimal_load_mode(table_name: str) -> str:
    if should_force_full(table_name):
        return "FULL"
    if has_primary_key(table_name):
        return "INCREMENTAL"
    return "FULL"


def get_table_description(table_name: str) -> str:
    meta = get_table_metadata(table_name)
    if meta:
        return meta.get('description', '')
    return ''


def reload_metadata():
    global _CACHE_LOADED
    _CACHE_LOADED = False
    _TABLE_METADATA_CACHE.clear()
    load_table_metadata()


try:
    load_table_metadata()
except Exception as e:
    print(f"⚠️ Impossible de charger les métadonnées : {e}")