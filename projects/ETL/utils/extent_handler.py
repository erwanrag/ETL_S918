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
from utils.custom_types import build_column_definition
from utils.metadata_helper import get_columns_metadata

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
    """
    conn = psycopg2.connect(config.get_connection_string())
    
    try:
        conn.set_client_encoding('UTF8')
        cur = conn.cursor()
        
        # Essayer d'abord avec le nom donné
        cur.execute("""
            SELECT 
                "ColumnName", "Extent", "ProgressType", "DataType",
                "Width", "Scale", "Label", "Description"
            FROM metadata.proginovcolumns
            WHERE UPPER("TableName") = UPPER(%s)
              AND "Extent" > 0
            ORDER BY "ColumnName"
        """, (table_name,))
        
        rows = cur.fetchall()
        
        # Si rien trouvé, résoudre via etl_tables
        if not rows:
            cur.execute("""
                SELECT "TableName"
                FROM metadata.etl_tables
                WHERE "ConfigName" = %s
                LIMIT 1
            """, (table_name,))
            
            row = cur.fetchone()
            if row:
                base_table = row[0]
                cur.execute("""
                    SELECT 
                        "ColumnName", "Extent", "ProgressType", "DataType",
                        "Width", "Scale", "Label", "Description"
                    FROM metadata.proginovcolumns
                    WHERE UPPER("TableName") = UPPER(%s)
                      AND "Extent" > 0
                    ORDER BY "ColumnName"
                """, (base_table,))
                
                rows = cur.fetchall()
        
        # Construire résultat
        result = {}
        for row in rows:
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
    Récupérer colonnes avec extent > 0
    
    IMPORTANT: Cherche d'abord par ConfigName puis par TableName
    """
    conn = psycopg2.connect(config.get_connection_string())
    try:
        conn.set_client_encoding('UTF8')
        cur = conn.cursor()
        
        # Essayer d'abord avec le nom donné (peut être ConfigName)
        cur.execute("""
            SELECT "ColumnName", "Extent"
            FROM metadata.proginovcolumns
            WHERE UPPER("TableName") = UPPER(%s)
              AND "Extent" > 0
            ORDER BY "ColumnName"
        """, (table_name,))
        
        result = {row[0]: row[1] for row in cur.fetchall()}
        
        # Si rien trouvé, essayer de résoudre via etl_tables
        if not result:
            cur.execute("""
                SELECT "TableName"
                FROM metadata.etl_tables
                WHERE "ConfigName" = %s
                LIMIT 1
            """, (table_name,))
            
            row = cur.fetchone()
            if row:
                base_table = row[0]
                cur.execute("""
                    SELECT "ColumnName", "Extent"
                    FROM metadata.proginovcolumns
                    WHERE UPPER("TableName") = UPPER(%s)
                      AND "Extent" > 0
                    ORDER BY "ColumnName"
                """, (base_table,))
                
                result = {row[0]: row[1] for row in cur.fetchall()}
        
        return result
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


def build_ods_select_with_extent_typed(table_name: str, raw_columns: List[str]):
    """
    Version PRO :
    - utilise build_column_definition pour récupérer le type cible
    - CAST toutes les colonnes (extent + normales) vers ce type
    - pas de CASE custom, pas de logique manuelle
    """

    cols_meta = get_columns_metadata(table_name)
    extent_metadata = get_extent_columns_with_metadata(table_name)

    select_parts = []
    expanded_columns = []
    column_types = {}

    for col in raw_columns:

        # ================================================
        # 1) COLONNE EXTENT : éclatement + typage metadata
        # ================================================
        if col in extent_metadata:
            meta = extent_metadata[col]
            extent = meta['extent']

            for i in range(1, extent + 1):
                expanded_col = f"{col}_{i}"

                # type final via la même logique que STAGING
                pg_type = get_pg_type_for_extent_column(
                    meta['progress_type'],
                    meta['data_type'],
                    meta['width'],
                    meta['scale']
                )

                clean_expr = f"TRIM(split_part(\"{col}\", ';', {i}))"

                # ---------------------
                # TYPE DATE
                # ---------------------
                if pg_type.upper() == "DATE":
                    expr = f"""
                        CASE
                            -- Valeurs invalides connues
                            WHEN {clean_expr} IN 
                                ('', '?', '??', '???', '00000000','00/00/0000','00-00-0000') 
                            THEN NULL

                            -- Format YYYYMMDD (Progress)
                            WHEN {clean_expr} ~ '^[0-9]{{8}}$'
                            THEN TO_DATE({clean_expr}, 'YYYYMMDD')

                            -- Format FR DD/MM/YYYY
                            WHEN {clean_expr} ~ '^[0-9]{{2}}/[0-9]{{2}}/[0-9]{{4}}$'
                                AND split_part({clean_expr}, '/', 1)::int BETWEEN 1 AND 31
                                AND split_part({clean_expr}, '/', 2)::int BETWEEN 1 AND 12
                            THEN TO_DATE({clean_expr}, 'DD/MM/YYYY')

                            -- Format US MM/DD/YYYY → NOUVEAU
                            WHEN {clean_expr} ~ '^[0-9]{{2}}/[0-9]{{2}}/[0-9]{{4}}$'
                                AND split_part({clean_expr}, '/', 1)::int BETWEEN 1 AND 12
                                AND split_part({clean_expr}, '/', 2)::int BETWEEN 1 AND 31
                            THEN TO_DATE({clean_expr}, 'MM/DD/YYYY')

                            -- ISO
                            WHEN {clean_expr} ~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}$'
                            THEN {clean_expr}::DATE

                            ELSE NULL
                        END AS "{expanded_col}"
                    """


                # ---------------------
                # TYPE NUMERIC
                # ---------------------
                elif "NUMERIC" in pg_type.upper():
                    expr = f"""
                        CASE
                            WHEN {clean_expr} IN ('', '?', '??', '???', '-', 'NULL')
                            THEN NULL
                            WHEN {clean_expr} ~ '^[0-9]+(\\.[0-9]+)?$'
                            THEN {clean_expr}::{pg_type}
                            ELSE NULL
                        END AS "{expanded_col}"
                    """

                # ---------------------
                # TYPE INTEGER
                # ---------------------
                elif pg_type.upper() == "INTEGER":
                    expr = f"""
                        CASE
                            WHEN {clean_expr} IN ('', '?', '??', '???', '-', 'NULL')
                            THEN NULL
                            WHEN {clean_expr} ~ '^[0-9]+$'
                            THEN {clean_expr}::INTEGER
                            ELSE NULL
                        END AS "{expanded_col}"
                    """

                # ---------------------
                # TYPE BOOLEAN
                # ---------------------
                elif pg_type.upper() == "BOOLEAN":
                    expr = f"""
                        CASE
                            WHEN {clean_expr} IN ('', '?', '??', 'NULL')
                            THEN NULL
                            WHEN LOWER({clean_expr}) IN ('1','true','t','yes','y')
                            THEN TRUE
                            WHEN LOWER({clean_expr}) IN ('0','false','f','no','n')
                            THEN FALSE
                            ELSE NULL
                        END AS "{expanded_col}"
                    """

                # ---------------------
                # TYPE VARCHAR / autres TEXT
                # ---------------------
                else:
                    expr = f"""
                        NULLIF({clean_expr}, '')::{pg_type} AS "{expanded_col}"
                    """



                select_parts.append(expr)
                expanded_columns.append(expanded_col)
                column_types[expanded_col] = pg_type

        # ======================================================
        # 2) COLONNE NORMALE : typage générique via metadata
        # ======================================================
        elif col in cols_meta:

            meta = cols_meta[col]

            # Récupération du type PostgreSQL EXACT utilisé dans STAGING
            ddl = build_column_definition(meta)  # ex:   dat_crt DATE
            _, pg_type = ddl.split(" ", 1)       # on extrait juste la partie type

            expr = f"""
                NULLIF("{col}"::text, '')::{pg_type} AS "{col}"
            """

            select_parts.append(expr)
            expanded_columns.append(col)
            column_types[col] = pg_type

        else:
            # Colonne technique RAW → fallback TEXT
            expr = f'NULLIF("{col}"::text, \'\') AS "{col}"'
            select_parts.append(expr)
            expanded_columns.append(col)
            column_types[col] = "TEXT"

    select_clause = ",\n".join(select_parts)

    return select_clause, expanded_columns, column_types


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