"""
============================================================================
Cleaning Utilities - RAW → STAGING
============================================================================
Objectif :
- Nettoyage standardisé des colonnes RAW avant typage en STAGING
- Appliquer les règles : TRIM, NULL, boolean, numeric, dates, etc.
- Fonctions utilisées par les tasks du flow RAW → STAGING
============================================================================
"""

import re
from typing import Optional


# ============================================================================
# NULL CLEANING
# ============================================================================

def clean_null(value: Optional[str]):
    """
    Normalise les valeurs vides en NULL.
    Gestion :
        - None
        - ""
        - " "
        - "\t"
        - valeurs spéciales Proginov ("?", ".", ",", "---")
    """
    if value is None:
        return None

    if isinstance(value, str):
        stripped = value.strip()
        if stripped in ("", "?", ".", "---", "NULL"):
            return None

    return value


# ============================================================================
# BOOLEAN CLEANING
# ============================================================================

def clean_boolean(value: Optional[str]):
    """
    Convertir en booléen :
        - "1", "Y", "YES", "TRUE"  → True
        - "0", "N", "NO", "FALSE"  → False
        - empty → NULL
    """
    if value is None:
        return None

    if isinstance(value, bool):
        return value

    v = str(value).strip().upper()

    if v in ("1", "Y", "YES", "TRUE", "OUI"):
        return True
    if v in ("0", "N", "NO", "FALSE", "NON"):
        return False

    return None  # Valeur incohérente → NULL


# ============================================================================
# NUMERIC CLEANING
# ============================================================================

def clean_numeric(value: Optional[str]):
    """
    Nettoyage des numériques Proginov :
        - "", " " → NULL
        - suppression des masques (">>>>>9", "->>>9.99", etc.)
        - suppression des espaces et virgules
    """
    if value is None:
        return None

    if isinstance(value, (float, int)):
        return value

    v = str(value).strip()

    if v == "":
        return None

    # On retire les masques Pure Progress (>>>>>>9 etc.)
    v = re.sub(r"[^\d\-,.]", "", v)

    # Cas d'un entier simple
    if re.fullmatch(r"-?\d+", v):
        return int(v)

    # Cas d'un decimal
    if re.fullmatch(r"-?\d+(\.\d+)?", v):
        return float(v)

    return None


# ============================================================================
# DATE CLEANING
# ============================================================================

def clean_date(value: Optional[str]):
    """
    Convertit les dates Proginov '99/99/99' → NULL
    Attendu RAW :
        - "2024-01-15"
        - "1999-03-18"
        - "00/00/00"
        - ""  etc.
    Renvoie :
        - ISO yyyy-mm-dd
        - None si impossible
    """
    if value is None:
        return None

    v = str(value).strip()

    if v in ("", "00/00/00", "00-00-00"):
        return None

    # Déjà ISO
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", v):
        return v

    # Format JJ/MM/AA ou JJ-MM-AA
    if re.fullmatch(r"\d{2}[/-]\d{2}[/-]\d{2,4}", v):
        try:
            d, m, y = re.split(r"[/-]", v)
            # Fix année sur 2 ou 4 chars
            if len(y) == 2:
                y = "20" + y if int(y) < 50 else "19" + y
            return f"{y}-{m}-{d}"
        except Exception:
            return None

    return None


# ============================================================================
# TEXT CLEANING
# ============================================================================

def clean_text(value: Optional[str]):
    """
    Nettoyage simple des VARCHAR Progress / TEXT :
        - trim
        - NULL si juste des espaces
        - suppression des caractères non imprimables
    """
    if value is None:
        return None

    v = str(value).strip()

    if v == "":
        return None

    # Retirer caractères de contrôle
    v = re.sub(r"[\x00-\x1F\x7F]", "", v)

    return v


# ============================================================================
# AUTO-CLEANING PAR TYPE METADATA
# ============================================================================

def clean_by_progress_type(value, progress_type: str):
    """
    Sélecteur automatique basé sur metadata.proginovcolumns.ProgressType
    """
    if progress_type is None:
        return value

    pt = str(progress_type).lower()

    if pt in ("character"):
        return clean_text(value)

    if pt in ("integer", "int", "int64"):
        return clean_numeric(value)

    if pt in ("decimal", "numeric"):
        return clean_numeric(value)

    if pt in ("date"):
        return clean_date(value)

    if pt in ("logical", "bit"):
        return clean_boolean(value)

    if pt in ("datetime", "timestamp"):
        return clean_date(value)  # Proginov → souvent mauvaise date

    # fallback
    return value
