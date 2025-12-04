"""
============================================================================
Module : Gestion des colonnes EXTENT (arrays Progress) - VERSION AMÉLIORÉE
============================================================================
SÉPARATEUR : Point-virgule (;)
IMPORTANT : Progress stocke les noms de tables en MAJUSCULES

AMÉLIORATIONS :
[OK] Typage intelligent des colonnes éclatées (pas tout en TEXT)
[OK] Génération des commentaires SQL depuis Label (Encodage UTF-8 corrigé)
[OK] Nettoyage des labels (suppression des quotes superflues)
[OK] Gestion NULL pour valeurs vides et "?"
[OK] Support CREATE TABLE avec types corrects

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


def _clean_progress_label(raw_label: Optional[str]) -> str:
    """
    Nettoie le label brut venant des métadonnées Progress.
    1. Gère les NULL
    2. Supprime les guillemets englobants ("Label" -> Label)
    3. Supprime les espaces superflus
    """
    if not raw_label:
        return ""
    
    # Nettoyage standard (strip espaces)
    clean = raw_label.strip()
    
    # Progress stocke souvent les labels avec des guillemets (ex: "Code Client")
    # On enlève les guillemets simples et doubles aux extrémités
    clean = clean.strip('"').strip("'")
    
    return clean


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
            'label': str,      # Nettoyé et encodé
            'description': str # Nettoyé et encodé
        }]
    """
    conn = psycopg2.connect(config.get_connection_string())
    
    try:
        # [CORRECTION] Force l'encodage client à UTF8 pour éviter les problèmes d'accents
        conn.set_client_encoding('UTF8')
        cur = conn.cursor()
        
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
            # Nettoyage immédiat des labels et descriptions
            cleaned_label = _clean_progress_label(row[6])
            cleaned_desc = _clean_progress_label(row[7])

            result[row[0]] = {
                'extent': row[1],
                'progress_type': (row[2] or '').lower(),
                'data_type': (row[3] or '').lower(),
                'width': row[4] or 0,
                'scale': row[5] or 0,
                'label': cleaned_label,
                'description': cleaned_desc
            }
        
        return result
        
    finally:
        if conn:
            conn.close()


def get_extent_columns_for_table(table_name: str) -> Dict[str, int]:
    """
    Récupérer colonnes avec extent > 0 (version simple, rétrocompatibilité)
    """
    conn = psycopg2.connect(config.get_connection_string())
    try:
        conn.set_client_encoding('UTF8') # Sécurité encodage
        cur = conn.cursor()
        
        cur.execute("""
            SELECT "ColumnName", "Extent"
            FROM metadata.proginovcolumns
            WHERE UPPER("TableName") = UPPER(%s)
              AND "Extent" > 0
            ORDER BY "ColumnName"
        """, (table_name,))
        
        return {row[0]: row[1] for row in cur.fetchall()}
    finally:
        conn.close()


def get_pg_type_for_extent_column(progress_type: str, data_type: str, width: int, scale: int) -> str:
    """
    Déterminer le type PostgreSQL pour une colonne extent éclatée
    
    IMPORTANT : Pour les colonnes extent, Progress stocke TOUJOURS en VARCHAR
    mais ProgressType indique le vrai type sémantique !
    """
    pt = progress_type.lower()
    
    # [WARN] CRITICAL: Convertir width et scale en int
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
    [NEW] VERSION AMÉLIORÉE : Construire SELECT avec typage ET cast intelligent
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
                
                # [CRITICAL] GESTION NULL STRICTE
                if pg_type.startswith('VARCHAR'):
                    expr = f"""NULLIF(NULLIF(NULLIF(TRIM(split_part("{col}", ';', {i})), ''), '?'), ' ')::{pg_type}"""
                
                elif pg_type.startswith('NUMERIC') or pg_type == 'INTEGER':
                    expr = f"""CASE 
                        WHEN TRIM(split_part("{col}", ';', {i})) IN ('', '?', ' ') THEN NULL
                        WHEN TRIM(split_part("{col}", ';', {i})) ~ '^-?[0-9]+\.?[0-9]*$' THEN 
                            NULLIF(TRIM(split_part("{col}", ';', {i})), '0')::{pg_type}
                        ELSE NULL
                    END"""
                    
                elif pg_type == 'DATE':
                    expr = f"""CASE 
                        WHEN TRIM(split_part("{col}", ';', {i})) IN ('', '?', '00/00/00', '00-00-00', ' ') THEN NULL
                        WHEN TRIM(split_part("{col}", ';', {i})) ~ '^[0-9]{{2}}/[0-9]{{2}}/[0-9]{{4}}$' THEN 
                            TO_DATE(TRIM(split_part("{col}", ';', {i})), 'MM/DD/YYYY')
                        WHEN TRIM(split_part("{col}", ';', {i})) ~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}$' THEN 
                            TRIM(split_part("{col}", ';', {i}))::DATE
                        ELSE NULL
                    END"""
                    
                elif pg_type == 'BOOLEAN':
                    expr = f"""CASE 
                        WHEN LOWER(TRIM(split_part("{col}", ';', {i}))) IN ('yes', 'true', '1') THEN TRUE
                        WHEN LOWER(TRIM(split_part("{col}", ';', {i}))) IN ('no', 'false', '0') THEN FALSE
                        ELSE NULL
                    END"""
                    
                else:
                    expr = f"""NULLIF(NULLIF(TRIM(split_part("{col}", ';', {i})), ''), '?')"""
                
                select_parts.append(f"{expr} AS {expanded_col}")
                ods_columns.append(expanded_col)
                column_types[expanded_col] = pg_type
        
        # ============================================================
        # COLONNE NORMALE
        # ============================================================
        else:
            select_parts.append(f'"{col}"')
            ods_columns.append(col)
    
    select_clause = ',\n            '.join(select_parts)
    
    return select_clause, ods_columns, column_types


def build_ods_select_with_extent(
    table_name: str,
    staging_columns: List[str]
) -> Tuple[str, List[str]]:
    """VERSION COMPATIBILITÉ"""
    select_clause, ods_columns, _ = build_ods_select_with_extent_typed(
        table_name, staging_columns
    )
    return select_clause, ods_columns


def generate_column_comments(
    table_name: str,
    schema: str = 'ods'
) -> List[str]:
    """
    [NEW] Générer les commentaires SQL pour colonnes extent éclatées
    
    Corrections :
    1. Utilise les labels nettoyés (sans quotes superflues)
    2. Échappe correctement les apostrophes pour le SQL (O'Neil -> O''Neil)
    """
    extent_metadata = get_extent_columns_with_metadata(table_name)
    comments = []
    
    for col_name, meta in extent_metadata.items():
        extent = meta['extent']
        
        # Récupération du label nettoyé ou fallback sur le nom de colonne
        base_label = meta['label'] if meta['label'] else f"Colonne {col_name}"
        
        # [CRITICAL] Échappement des apostrophes pour la commande SQL
        # Si le label contient ' (ex: Chiffre d'affaire), il devient Chiffre d''affaire
        safe_label = base_label.replace("'", "''")
        
        for i in range(1, extent + 1):
            expanded_col = f"{col_name}_{i}"
            comment_text = f"{safe_label} - Elément {i}/{extent}"
            
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
    print("TEST EXTENT HANDLER AMÉLIORÉ (UTF-8 & Labels)")
    print("=" * 70)
    
    # Simuler un nom de table pour le test
    # (Assurez-vous que la table existe dans metadata.proginovcolumns pour un vrai test)
    target_table = 'client' 
    
    print(f"\n1. Métadonnées extent pour '{target_table}' (5 premiers):")
    try:
        extent_meta = get_extent_columns_with_metadata(target_table)
        for i, (col, meta) in enumerate(list(extent_meta.items())[:5]):
            print(f"   {col}[{meta['extent']}]")
            print(f"      Label (Raw) cleaned : [{meta['label']}]")
            print(f"      Type PG             : {get_pg_type_for_extent_column(meta['progress_type'], meta['data_type'], meta['width'], meta['scale'])}")
    except Exception as e:
        print(f"   Erreur connexion DB: {e}")

    print("\n2. Commentaires SQL générés (Exemple avec échappement):")
    # Test unitaire sur la génération de commentaire
    fake_meta = {
        'ztx': {
            'extent': 2, 
            'label': "Chiffre d'affaire HT", # Label avec apostrophe pour test
            'progress_type': 'decimal'
        }
    }
    
    # Simulation manuelle pour valider la logique generate_column_comments sans DB
    lbl = fake_meta['ztx']['label'].replace("'", "''")
    for i in range(1, 3):
        print(f"   COMMENT ON COLUMN ods.test.ztx_{i} IS '{lbl} - Elément {i}/2';")

    print("\n" + "=" * 70)
    print("TESTS TERMINÉS")
    print("=" * 70)