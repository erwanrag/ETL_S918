"""
============================================================================
Module : Gestion des colonnes EXTENT (arrays Progress) - VERSION AMÉLIORÉE
============================================================================
SÉPARATEUR : Point-virgule (;)
IMPORTANT : Progress stocke les noms de tables en MAJUSCULES

AMÉLIORATIONS :
✅ Typage intelligent des colonnes éclatées (pas tout en TEXT)
✅ Génération des commentaires SQL depuis Label
✅ Gestion NULL pour valeurs vides et "?"
✅ Support CREATE TABLE avec types corrects

TYPAGE :
- ProgressType=character → VARCHAR(Width)
- ProgressType=decimal → NUMERIC(Width, Scale)
- ProgressType=integer → INTEGER
- ProgressType=date → DATE
- ProgressType=logical → BOOLEAN
============================================================================
"""

import psycopg2
from typing import Dict, List, Tuple, Optional
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))
from flows.config.pg_config import config


def get_extent_columns_with_metadata(table_name: str) -> Dict[str, Dict]:
    """
    Récupérer colonnes extent avec métadonnées complètes
    
    Returns:
        Dict[column_name, {
            'extent': int,
            'progress_type': str,
            'data_type': str,
            'width': int,
            'scale': int,
            'label': str,
            'description': str
        }]
    """
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    try:
        cur.execute("""
            SELECT 
                "ColumnName",
                "Extent",
                "ProgressType",
                "DataType",
                "Width",
                "Scale",
                "Label",
                "Description"
            FROM metadata.proginovcolumns
            WHERE UPPER("TableName") = UPPER(%s)
              AND "Extent" > 0
            ORDER BY "ColumnName"
        """, (table_name,))
        
        result = {}
        for row in cur.fetchall():
            result[row[0]] = {
                'extent': row[1],
                'progress_type': (row[2] or '').lower(),
                'data_type': (row[3] or '').lower(),
                'width': row[4] or 0,
                'scale': row[5] or 0,
                'label': row[6] or '',
                'description': row[7] or ''
            }
        
        return result
        
    finally:
        cur.close()
        conn.close()


def get_extent_columns_for_table(table_name: str) -> Dict[str, int]:
    """
    Récupérer colonnes avec extent > 0 (version simple, rétrocompatibilité)
    """
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    try:
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


def get_pg_type_for_extent_column(progress_type: str, data_type: str, width: int, scale: int) -> str:
    """
    Déterminer le type PostgreSQL pour une colonne extent éclatée
    
    IMPORTANT : Pour les colonnes extent, Progress stocke TOUJOURS en VARCHAR
    mais ProgressType indique le vrai type sémantique !
    
    Args:
        progress_type: Type Progress (character, decimal, integer, date, logical)
        data_type: Type de données (toujours varchar pour extent)
        width: Largeur
        scale: Décimales
    
    Returns:
        str: Type PostgreSQL (VARCHAR(n), NUMERIC(p,s), INTEGER, DATE, BOOLEAN)
    """
    pt = progress_type.lower()
    
    # ⚠️ CRITICAL: Convertir width et scale en int (peuvent être string depuis DB)
    try:
        width = int(width) if width else 0
    except (ValueError, TypeError):
        width = 0
    
    try:
        scale = int(scale) if scale else 0
    except (ValueError, TypeError):
        scale = 0
    
    # ============================================================
    # PRIORITÉ 1 : ProgressType (type sémantique)
    # ============================================================
    
    # DECIMAL/NUMERIC → NUMERIC
    if pt in ('decimal', 'numeric'):
        if scale and scale > 0:
            return f'NUMERIC({width},{scale})'
        else:
            return f'NUMERIC({width},0)'
    
    # INTEGER → INTEGER
    elif pt in ('integer', 'int', 'int64'):
        return 'INTEGER'
    
    # DATE → DATE
    elif pt == 'date':
        return 'DATE'
    
    # LOGICAL → BOOLEAN
    elif pt in ('logical', 'bit'):
        return 'BOOLEAN'
    
    # CHARACTER → VARCHAR
    elif pt == 'character':
        # Limiter à 255 pour éviter les colonnes trop larges
        actual_width = min(width, 255) if width > 0 else 255
        return f'VARCHAR({actual_width})'
    
    # ============================================================
    # FALLBACK : Si ProgressType inconnu, utiliser DataType
    # ============================================================
    else:
        dt = data_type.lower()
        if dt in ('decimal', 'numeric'):
            return f'NUMERIC({width},{scale})' if scale > 0 else f'NUMERIC({width},0)'
        elif dt in ('integer', 'int'):
            return 'INTEGER'
        elif dt == 'date':
            return 'DATE'
        elif dt in ('logical', 'bit'):
            return 'BOOLEAN'
        elif dt == 'varchar':
            actual_width = min(width, 255) if width > 0 else 255
            return f'VARCHAR({actual_width})'
        else:
            return 'TEXT'


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


def build_ods_select_with_extent_typed(
    table_name: str,
    staging_columns: List[str]
) -> Tuple[str, List[str], Dict[str, str]]:
    """
    🆕 VERSION AMÉLIORÉE : Construire SELECT avec typage ET cast intelligent
    
    SÉPARATEUR : Point-virgule (;)
    
    Returns:
        Tuple[
            select_clause: str,  # Clause SELECT avec CAST
            ods_columns: List[str],  # Liste des colonnes ODS
            column_types: Dict[str, str]  # Mapping colonne → type PG
        ]
    
    Exemple :
        zal (character, Extent=5) →
            NULLIF(TRIM(split_part("zal", ';', 1)), '')::VARCHAR(35) AS zal_1,
            NULLIF(TRIM(split_part("zal", ';', 2)), '')::VARCHAR(35) AS zal_2,
            ...
        
        znu (decimal, Extent=5, Width=16, Scale=4) →
            NULLIF(split_part("znu", ';', 1), '')::NUMERIC(16,4) AS znu_1,
            NULLIF(split_part("znu", ';', 2), '')::NUMERIC(16,4) AS znu_2,
            ...
    """
    extent_metadata = get_extent_columns_with_metadata(table_name)
    
    select_parts = []
    ods_columns = []
    column_types = {}
    
    for col in staging_columns:
        # ============================================================
        # COLONNE EXTENT : Éclater avec typage intelligent
        # ============================================================
        if col in extent_metadata:
            meta = extent_metadata[col]
            extent = meta['extent']
            pg_type = get_pg_type_for_extent_column(
                meta['progress_type'],
                meta['data_type'],
                meta['width'],
                meta['scale']
            )
            
            for i in range(1, extent + 1):
                expanded_col = f"{col}_{i}"
                
                # 🔥 GESTION NULL STRICTE : Toutes valeurs vides → NULL
                if pg_type.startswith('VARCHAR'):
                    # VARCHAR : NULLIF pour "", "?", espaces
                    expr = f"""NULLIF(NULLIF(NULLIF(TRIM(split_part("{col}", ';', {i})), ''), '?'), ' ')::{pg_type}"""
                    
                elif pg_type.startswith('NUMERIC') or pg_type == 'INTEGER':
                    # NUMERIC/INTEGER : Conversion stricte avec gestion erreurs
                    expr = f"""CASE 
                        WHEN TRIM(split_part("{col}", ';', {i})) IN ('', '?', ' ') THEN NULL
                        WHEN TRIM(split_part("{col}", ';', {i})) ~ '^-?[0-9]+\.?[0-9]*$' THEN 
                            NULLIF(TRIM(split_part("{col}", ';', {i})), '0')::{pg_type}
                        ELSE NULL
                    END"""
                    
                elif pg_type == 'DATE':
                    # DATE : Conversion stricte avec validation format
                    expr = f"""CASE 
                        WHEN TRIM(split_part("{col}", ';', {i})) IN ('', '?', '00/00/00', '00-00-00', ' ') THEN NULL
                        WHEN TRIM(split_part("{col}", ';', {i})) ~ '^[0-9]{{2}}/[0-9]{{2}}/[0-9]{{4}}$' THEN 
                            TO_DATE(TRIM(split_part("{col}", ';', {i})), 'MM/DD/YYYY')
                        WHEN TRIM(split_part("{col}", ';', {i})) ~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}$' THEN 
                            TRIM(split_part("{col}", ';', {i}))::DATE
                        ELSE NULL
                    END"""
                    
                elif pg_type == 'BOOLEAN':
                    # BOOLEAN : Conversion stricte yes/no
                    expr = f"""CASE 
                        WHEN LOWER(TRIM(split_part("{col}", ';', {i}))) IN ('yes', 'true', '1') THEN TRUE
                        WHEN LOWER(TRIM(split_part("{col}", ';', {i}))) IN ('no', 'false', '0') THEN FALSE
                        ELSE NULL
                    END"""
                    
                else:
                    # TEXT : NULLIF pour valeurs vides
                    expr = f"""NULLIF(NULLIF(TRIM(split_part("{col}", ';', {i})), ''), '?')"""
                
                select_parts.append(f"{expr} AS {expanded_col}")
                ods_columns.append(expanded_col)
                column_types[expanded_col] = pg_type
        
        # ============================================================
        # COLONNE NORMALE : Passer telle quelle
        # ============================================================
        else:
            select_parts.append(f'"{col}"')
            ods_columns.append(col)
            # Type non défini pour colonnes normales (déjà typées dans STAGING)
    
    select_clause = ',\n            '.join(select_parts)
    
    return select_clause, ods_columns, column_types


def build_ods_select_with_extent(
    table_name: str,
    staging_columns: List[str]
) -> Tuple[str, List[str]]:
    """
    VERSION COMPATIBILITÉ : Construire SELECT sans typage (comme avant)
    
    Pour rétrocompatibilité avec code existant
    """
    select_clause, ods_columns, _ = build_ods_select_with_extent_typed(
        table_name, staging_columns
    )
    return select_clause, ods_columns


def generate_column_comments(
    table_name: str,
    schema: str = 'ods'
) -> List[str]:
    """
    🆕 Générer les commentaires SQL pour colonnes extent éclatées
    
    Args:
        table_name: Nom de la table (ex: 'client')
        schema: Schéma PostgreSQL (défaut: 'ods')
    
    Returns:
        List[str]: Liste de commandes COMMENT ON COLUMN
    
    Example:
        >>> comments = generate_column_comments('client')
        >>> print(comments[0])
        COMMENT ON COLUMN ods.client.zal_1 IS 'Libre caractère - Élément 1/5';
    """
    extent_metadata = get_extent_columns_with_metadata(table_name)
    comments = []
    
    for col_name, meta in extent_metadata.items():
        extent = meta['extent']
        label = meta['label'].strip("'\"") if meta['label'] else f"Colonne {col_name}"
        
        # 🔥 ÉCHAPPER les quotes simples pour SQL
        label = label.replace("'", "''")
        
        for i in range(1, extent + 1):
            expanded_col = f"{col_name}_{i}"
            comment_text = f"{label} - Élément {i}/{extent}"
            
            sql = f"COMMENT ON COLUMN {schema}.{table_name}.{expanded_col} IS '{comment_text}';"
            comments.append(sql)
    
    return comments


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
        'expansion_ratio': total_expanded / len(extent_cols) if len(extent_cols) > 0 else 0
    }


# ============================================================================
# TESTS
# ============================================================================

if __name__ == "__main__":
    print("=" * 70)
    print("TEST EXTENT HANDLER AMÉLIORÉ")
    print("=" * 70)
    
    # Test 1: Métadonnées complètes
    print("\n1. Métadonnées extent pour 'client' (5 premiers):")
    extent_meta = get_extent_columns_with_metadata('client')
    for i, (col, meta) in enumerate(list(extent_meta.items())[:5]):
        pg_type = get_pg_type_for_extent_column(
            meta['progress_type'],
            meta['data_type'],
            meta['width'],
            meta['scale']
        )
        print(f"   {col}[{meta['extent']}] → {pg_type}")
        print(f"      Label: {meta['label']}")
    
    # Test 2: SELECT typé
    print("\n2. Test SELECT avec typage:")
    staging_cols = ['cod_cli', 'nom_cli', 'zal', 'znu', '_etl_hashdiff']
    select_clause, ods_cols, col_types = build_ods_select_with_extent_typed('client', staging_cols)
    print(f"   Staging: {len(staging_cols)} colonnes")
    print(f"   ODS: {len(ods_cols)} colonnes")
    print(f"\n   Types générés:")
    for col, typ in list(col_types.items())[:5]:
        print(f"      {col}: {typ}")
    
    # Test 3: Commentaires SQL
    print("\n3. Commentaires SQL (3 premiers):")
    comments = generate_column_comments('client')
    for comment in comments[:3]:
        print(f"   {comment}")
    
    # Test 4: Statistiques
    print("\n4. Statistiques expansion:")
    for table in ['client', 'produit', 'clcondi']:
        stats = count_extent_expansion(table)
        if stats['extent_columns'] > 0:
            print(f"   {table}: {stats['extent_columns']} cols → {stats['total_expanded']} cols éclatées (ratio: {stats['expansion_ratio']:.1f})")
    
    print("\n" + "=" * 70)
    print("TESTS TERMINÉS")
    print("=" * 70)