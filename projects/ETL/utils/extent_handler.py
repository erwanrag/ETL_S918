"""
============================================================================
Module : Gestion des colonnes EXTENT (arrays Progress)
============================================================================
SÉPARATEUR : Point-virgule (;)
IMPORTANT : Progress stocke les noms de tables en MAJUSCULES
============================================================================
"""

import psycopg2
from typing import Dict, List, Tuple
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))
from flows.config.pg_config import config


def get_extent_columns_for_table(table_name: str) -> Dict[str, int]:
    """
    Récupérer colonnes avec extent > 0
    
    IMPORTANT : Progress stocke les noms en MAJUSCULES
    """
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    try:
        # ✅ CORRECTION : Chercher en MAJUSCULES
        cur.execute("""
            SELECT "ColumnName", "Extent"
            FROM metadata.proginovcolumns
            WHERE UPPER("TableName") = UPPER(%s)
              AND "Extent" > 0
            ORDER BY "ColumnName"
        """, (table_name,))
        
        return {row[0]: row[1] for row in cur.fetchall()}
    finally:
        cur.close()
        conn.close()


def generate_extent_columns(column_name: str, extent: int) -> List[str]:
    """Générer liste colonnes éclatées"""
    return [f"{column_name}_{i+1}" for i in range(extent)]


def get_extent_mapping(table_name: str) -> Dict[str, List[str]]:
    """Obtenir mapping extent → colonnes éclatées"""
    extent_cols = get_extent_columns_for_table(table_name)
    
    mapping = {}
    for col_name, extent in extent_cols.items():
        mapping[col_name] = generate_extent_columns(col_name, extent)
    
    return mapping


def build_ods_select_with_extent(
    table_name: str,
    staging_columns: List[str]
) -> Tuple[str, List[str]]:
    """
    Construire SELECT pour ODS avec éclatement extent
    
    SÉPARATEUR : Point-virgule (;)
    
    Exemple données Progress : zal = "AAA;;;;"
    Résultat :
        zal_1 = "AAA"
        zal_2 = ""
        zal_3 = ""
        zal_4 = ""
        zal_5 = ""
    """
    extent_mapping = get_extent_columns_for_table(table_name)
    
    select_parts = []
    ods_columns = []
    
    for col in staging_columns:
        # Si colonne extent, l'éclater
        if col in extent_mapping:
            extent = extent_mapping[col]
            for i in range(1, extent + 1):
                expanded_col = f"{col}_{i}"
                # ✅ SÉPARATEUR : Point-virgule (;)
                select_parts.append(
                    f"split_part(\"{col}\", ';', {i}) AS {expanded_col}"
                )
                ods_columns.append(expanded_col)
        else:
            # Colonne normale (y compris _etl_*)
            select_parts.append(f'"{col}"')
            ods_columns.append(col)
    
    select_clause = ',\n            '.join(select_parts)
    
    return select_clause, ods_columns


def has_extent_columns(table_name: str) -> bool:
    """Vérifier si table a colonnes extent"""
    extent_cols = get_extent_columns_for_table(table_name)
    return len(extent_cols) > 0


def count_extent_expansion(table_name: str) -> Dict[str, int]:
    """Calculer statistiques expansion"""
    extent_cols = get_extent_columns_for_table(table_name)
    
    if not extent_cols:
        return {
            'extent_columns': 0,
            'total_expanded': 0,
            'expansion_ratio': 0
        }
    
    total_expanded = sum(extent_cols.values())
    
    return {
        'extent_columns': len(extent_cols),
        'total_expanded': total_expanded,
        'expansion_ratio': total_expanded / len(extent_cols)
    }


# ============================================================================
# TESTS
# ============================================================================

if __name__ == "__main__":
    print("=" * 70)
    print("TEST EXTENT HANDLER")
    print("=" * 70)
    
    # Test 1: Colonnes extent pour client
    print("\n1. Colonnes extent pour 'client':")
    extent_cols = get_extent_columns_for_table('client')
    if extent_cols:
        for col, extent in list(extent_cols.items())[:5]:  # 5 premiers
            expanded = generate_extent_columns(col, extent)
            print(f"   {col}[{extent}] → {', '.join(expanded)}")
        print(f"   ... et {len(extent_cols) - 5} autres colonnes")
    else:
        print("   ❌ Aucune colonne extent trouvée")
    
    # Test 2: Statistiques
    print("\n2. Statistiques expansion pour 'client':")
    stats = count_extent_expansion('client')
    print(f"   Colonnes extent: {stats['extent_columns']}")
    print(f"   Total éclaté: {stats['total_expanded']}")
    if stats['extent_columns'] > 0:
        print(f"   Ratio moyen: {stats['expansion_ratio']:.1f}")
    
    # Test 3: Vérifier présence
    print("\n3. Vérification présence extent:")
    for table in ['client', 'produit', 'stock']:
        has_extent = has_extent_columns(table)
        extent_count = len(get_extent_columns_for_table(table))
        status = f"✅ {extent_count} colonne(s)" if has_extent else "❌ AUCUNE"
        print(f"   {table}: {status}")
    
    # Test 4: SELECT
    print("\n4. Test SELECT avec extent:")
    staging_cols = ['cod_cli', 'nom_cli', 'zal', 'commerc', '_etl_hashdiff']
    select_clause, ods_cols = build_ods_select_with_extent('client', staging_cols)
    print(f"   Staging: {len(staging_cols)} colonnes")
    print(f"   ODS: {len(ods_cols)} colonnes")
    print(f"\n   Exemple SELECT (200 premiers chars):")
    print(f"   {select_clause[:200]}...")
    
    print("\n" + "=" * 70)
    print("TESTS TERMINÉS")
    print("=" * 70)