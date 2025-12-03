"""
============================================================================
Type Utilities - RAW → STAGING
============================================================================
Objectif :
- Mapper ProgressType / DataType vers types PostgreSQL STAGING
- Générer automatiquement les définitions de colonnes typées pour STG
- Utilisé par tasks/staging_tasks.py
============================================================================
"""

from typing import Optional, Dict


# ============================================================================
# MAPPING ProgressType → PostgreSQL STAGING
# ============================================================================

POSTGRES_TYPE_MAP = {
    "character": "TEXT",
    "varchar": "TEXT",               # Proginov → staging = TEXT
    "logical": "BOOLEAN",
    "bit": "BOOLEAN",
    "integer": "INTEGER",
    "int": "INTEGER",
    "int64": "BIGINT",
    "decimal": "NUMERIC",
    "numeric": "NUMERIC",
    "date": "DATE",
    "datetime": "TIMESTAMP",
    "timestamp": "TIMESTAMP",
}


def get_pg_type(progress_type: Optional[str],
                data_type: Optional[str],
                width: Optional[int],
                scale: Optional[int],
                extent: Optional[int] = 0) -> str:
    """
    Détermine le meilleur type PostgreSQL pour STAGING en fonction de metadata.
    
    🔥 RÈGLE CRITIQUE : Si extent > 0, TOUJOURS retourner TEXT
    Car Progress stocke les arrays comme "val1;val2;val3" en VARCHAR
    
    Args:
        progress_type: metadata.proginovcolumns.ProgressType
        data_type: metadata.proginovcolumns.DataType (varchar, integer…)
        width: Taille du champ Progress (peu utile pour PostgreSQL)
        scale: précision des décimaux pour NUMERIC
        extent: Extent > 0 indique un array Progress → VARCHAR multi-values

    Returns:
        Type PostgreSQL STAGING (str)
    """
    
    # 🔥 RÈGLE #1 : EXTENT > 0 → TOUJOURS TEXT (multi-values)
    if extent and extent > 0:
        return "TEXT"

    # Sécurité
    if progress_type:
        progress_type = progress_type.lower().strip()

    if data_type:
        data_type = data_type.lower().strip()

    # 1️⃣ TYPE LOGICAL
    if progress_type in ("logical", "bit"):
        return "BOOLEAN"

    # 2️⃣ TYPE INTEGER
    if progress_type in ("integer", "int", "int64"):
        return "INTEGER"

    # 3️⃣ TYPE NUMERIC
    if progress_type in ("decimal", "numeric"):
        # On définit la précision NUMERIC(p,s) si width/scale existent
        if width and scale:
            return f"NUMERIC({width},{scale})"
        if width:
            return f"NUMERIC({width})"
        return "NUMERIC"

    # 4️⃣ TYPE DATE
    if progress_type == "date":
        return "DATE"

    # 5️⃣ TYPE DATETIME
    if progress_type in ("datetime", "timestamp"):
        return "TIMESTAMP"

    # 6️⃣ TYPE CHARACTER
    if progress_type == "character":
        return "TEXT"

    # 7️⃣ data_type comme fallback
    if data_type in ("varchar", "character"):
        return "TEXT"
    if data_type in ("int", "integer"):
        return "INTEGER"
    if data_type in ("bigint",):
        return "BIGINT"

    # 8️⃣ fallback final → TEXT
    return "TEXT"


# ============================================================================
# Générer définition SQL colonne STAGING
# ============================================================================

def build_column_definition(col: Dict) -> str:
    """
    Construit la définition SQL pour une colonne STAGING.

    Args:
        col: dict metadata d'une ligne metadata.proginovcolumns :
             {
               "ColumnName": "nom_cli",
               "DataType": "varchar",
               "Width": 31000,
               "Scale": "0",
               "ProgressType": "character",
               "Extent": 0,
               ...
             }

    Returns:
        '"nom_cli" TEXT' ou '"statut" INTEGER' etc.
    """
    name = col["ColumnName"]
    
    # Récupérer Extent depuis metadata
    extent = col.get("Extent", 0)
    if extent is None:
        extent = 0
    
    pg_type = get_pg_type(
        progress_type=col.get("ProgressType"),
        data_type=col.get("DataType"),
        width=col.get("Width"),
        scale=col.get("Scale"),
        extent=extent  # 🔥 NOUVEAU : passer extent
    )
    return f'"{name}" {pg_type}'


# ============================================================================
# Générer liste de colonnes SQL pour CREATE TABLE
# ============================================================================

def build_table_columns_sql(columns_metadata: Dict[str, Dict]) -> str:
    """
    Prend un dict {colname: metadata} et génère la liste SQL des colonnes
    pour CREATE TABLE staging_etl.stg_xxx.

    Args:
        columns_metadata: dict colonne → metadata

    Returns:
        SQL string :
            "col1" INTEGER,
            "col2" TEXT,
            "col3" NUMERIC(10,2)
    """
    column_defs = [
        build_column_definition(col_meta)
        for col_meta in columns_metadata.values()
    ]

    return ",\n    ".join(column_defs)