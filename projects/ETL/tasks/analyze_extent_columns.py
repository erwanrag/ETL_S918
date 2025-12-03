"""
============================================================================
Utilitaire : Éclater colonnes EXTENT (arrays Progress)
============================================================================
Objectif : Transformer zal[5] → zal_1, zal_2, zal_3, zal_4, zal_5
"""

import psycopg2
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))
from flows.config.pg_config import config


def get_extent_columns():
    """Récupérer colonnes avec extent > 0"""
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    cur.execute("""
        SELECT 
            "TableName",
            "ColumnName",
            "Extent"
        FROM metadata.proginovcolumns
        WHERE "Extent" > 0
        ORDER BY "TableName", "ColumnName"
    """)
    
    results = {}
    for table, col, extent in cur.fetchall():
        if table not in results:
            results[table] = []
        results[table].append({'column': col, 'extent': extent})
    
    cur.close()
    conn.close()
    
    return results


def generate_extent_columns(column_name: str, extent: int) -> list:
    """
    Générer liste colonnes éclatées
    
    Args:
        column_name: Nom colonne (ex: 'zal')
        extent: Nombre éléments (ex: 5)
    
    Returns:
        list: ['zal_1', 'zal_2', 'zal_3', 'zal_4', 'zal_5']
    """
    return [f"{column_name}_{i+1}" for i in range(extent)]


if __name__ == "__main__":
    print("=" * 70)
    print("ANALYSE COLONNES EXTENT")
    print("=" * 70)
    
    extent_cols = get_extent_columns()
    
    for table, columns in extent_cols.items():
        print(f"\n[LIST] {table}")
        for col_info in columns:
            col_name = col_info['column']
            extent = col_info['extent']
            expanded = generate_extent_columns(col_name, extent)
            print(f"  {col_name}[{extent}] → {', '.join(expanded)}")
    
    print("\n" + "=" * 70)
    print(f"Total : {len(extent_cols)} table(s) avec colonnes extent")
    print("=" * 70)
    
    print("\n[WARN] ACTION REQUISE :")
    print("  Ces colonnes doivent être éclatées en ODS pour une structure propre")
    print("  Exemple : zal VARCHAR → zal_1, zal_2, zal_3, zal_4, zal_5")