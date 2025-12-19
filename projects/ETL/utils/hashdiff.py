"""
============================================================================
Hashdiff - Calcul SHA256 pour change detection
============================================================================
Fichier : utils/hashdiff.py

Fonctions pour calculer hashdiff (SHA256) sur colonnes métier
pour détecter les changements entre staging et ODS
============================================================================
"""

import hashlib
import pandas as pd
import psycopg2
from typing import List, Optional, Dict
import sys
from pathlib import Path

# Ajouter le chemin du projet
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from shared.config import config, sftp_config
from utils.metadata_helper import get_business_columns


def calculate_hashdiff_pandas(df: pd.DataFrame, columns: List[str]) -> pd.Series:
    """
    Calculer hashdiff SHA256 sur DataFrame pandas
    
    Args:
        df: DataFrame avec données
        columns: Liste colonnes à inclure dans le hashdiff
    
    Returns:
        pd.Series avec hashdiff pour chaque ligne
    """
    def hash_row(row):
        # Concaténer colonnes avec | comme séparateur
        values = [str(row[col]) if pd.notna(row[col]) else '' for col in columns]
        concat_str = '|'.join(values)
        
        # Calculer SHA256
        hash_obj = hashlib.sha256(concat_str.encode('utf-8'))
        return hash_obj.hexdigest()
    
    return df.apply(hash_row, axis=1)


def calculate_hashdiff_sql(table_name: str, schema: str, run_id: Optional[str] = None) -> str:
    """
    Générer SQL pour calculer hashdiff en base PostgreSQL
    Gestion limite 100 arguments PostgreSQL via chunks
    """
    from utils.metadata_helper import get_business_columns
    
    # Récupérer colonnes métier
    business_cols = get_business_columns(table_name)
    
    # Gérer limite 100 arguments PostgreSQL
    if len(business_cols) <= 95:
        # Méthode simple
        concat_parts = [f"COALESCE(\"{col}\"::TEXT, '')" for col in business_cols]
        concat_expr = f"CONCAT_WS('|', {', '.join(concat_parts)})"
    else:
        # Méthode par chunks
        chunk_size = 95
        chunks = []
        for i in range(0, len(business_cols), chunk_size):
            chunk_cols = business_cols[i:i+chunk_size]
            concat_parts = [f"COALESCE(\"{col}\"::TEXT, '')" for col in chunk_cols]
            chunks.append(f"CONCAT_WS('|', {', '.join(concat_parts)})")
        
        concat_expr = " || '|' || ".join(chunks)
    
    # SQL UPDATE avec SHA256
    sql = f"""
        UPDATE {schema}.{table_name}
        SET _etl_hashdiff = ENCODE(
            SHA256({concat_expr}::BYTEA),
            'hex'
        )
    """
    
    if run_id:
        sql += f"\n        WHERE _etl_run_id = '{run_id}'"
    
    return sql

def execute_hashdiff_update(table_name: str, schema: str, run_id: Optional[str] = None):
    """
    Exécuter calcul hashdiff en base
    
    Args:
        table_name: Nom de la table
        schema: Schéma (staging, ods)
        run_id: UUID du run (optionnel)
    """
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    try:
        sql = calculate_hashdiff_sql(table_name, schema, run_id)
        cur.execute(sql)
        conn.commit()
        
        rows_updated = cur.rowcount
        return rows_updated
        
    finally:
        cur.close()
        conn.close()


def detect_changes(table_name: str, run_id: str) -> Dict[str, int]:
    """
    Détecter changements entre staging et ODS via hashdiff
    
    Args:
        table_name: Nom de la table
        run_id: UUID du run
    
    Returns:
        Dict avec counts: new, modified, unchanged
    """
    from utils.metadata_helper import get_primary_keys
    
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    try:
        # Récupérer PK
        pk_cols = get_primary_keys(table_name)
        pk_join = ' AND '.join([f's."{pk}" = o."{pk}"' for pk in pk_cols])
        
        # Query pour compter changements
        sql = f"""
            WITH changes AS (
                SELECT 
                    s.*,
                    CASE 
                        WHEN o._etl_hashdiff IS NULL THEN 'new'
                        WHEN o._etl_hashdiff != s._etl_hashdiff THEN 'modified'
                        ELSE 'unchanged'
                    END as change_type
                FROM staging.{table_name} s
                LEFT JOIN ods.{table_name} o ON {pk_join}
                WHERE s._etl_run_id = %s
            )
            SELECT 
                change_type,
                COUNT(*) as count
            FROM changes
            GROUP BY change_type
        """
        
        cur.execute(sql, (run_id,))
        
        results = {'new': 0, 'modified': 0, 'unchanged': 0}
        for row in cur.fetchall():
            results[row[0]] = row[1]
        
        return results
        
    finally:
        cur.close()
        conn.close()


def get_changed_rows(table_name: str, run_id: str, change_type: str = 'modified') -> pd.DataFrame:
    """
    Récupérer les lignes modifiées
    
    Args:
        table_name: Nom de la table
        run_id: UUID du run
        change_type: Type de changement ('new', 'modified')
    
    Returns:
        DataFrame avec lignes modifiées
    """
    from utils.metadata_helper import get_primary_keys
    
    conn = psycopg2.connect(config.get_connection_string())
    
    try:
        # Récupérer PK
        pk_cols = get_primary_keys(table_name)
        pk_join = ' AND '.join([f's."{pk}" = o."{pk}"' for pk in pk_cols])
        
        # Query
        if change_type == 'new':
            condition = 'o._etl_hashdiff IS NULL'
        elif change_type == 'modified':
            condition = 'o._etl_hashdiff != s._etl_hashdiff'
        else:
            condition = 'TRUE'
        
        sql = f"""
            SELECT s.*
            FROM staging.{table_name} s
            LEFT JOIN ods.{table_name} o ON {pk_join}
            WHERE s._etl_run_id = %s
              AND {condition}
        """
        
        df = pd.read_sql(sql, conn, params=(run_id,))
        return df
        
    finally:
        conn.close()


# ============================================================================
# TESTS
# ============================================================================

if __name__ == "__main__":
    print("=" * 60)
    print("TEST HASHDIFF")
    print("=" * 60)
    
    # Test 1: Générer SQL hashdiff
    print("\n1. Générer SQL hashdiff pour 'client':")
    sql = calculate_hashdiff_sql('client', 'staging', 'test-run-id')
    print(sql[:300] + "...")
    
    # Test 2: Calculer hashdiff sur DataFrame
    print("\n2. Test hashdiff sur DataFrame pandas:")
    test_df = pd.DataFrame({
        'cod_cli': ['CLI001', 'CLI002', 'CLI003'],
        'nom_cli': ['Client A', 'Client B', 'Client C'],
        'ville': ['Paris', 'Lyon', None]
    })
    
    hashdiff = calculate_hashdiff_pandas(test_df, ['cod_cli', 'nom_cli', 'ville'])
    print(f"   {len(hashdiff)} hashdiff calculés")
    print(f"   Exemple: {hashdiff.iloc[0][:16]}...")
    
    print("\n" + "=" * 60)