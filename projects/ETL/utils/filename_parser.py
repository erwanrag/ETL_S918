"""
============================================================================
Utilitaire Parsing Noms Fichiers SFTP
============================================================================
Fichier : E:/Prefect/projects/ETL/utils/filename_parser.py

Parse les noms de fichiers SFTP pour extraire :
- table_name ou config_name
- timestamp/date
- Gère les cas avec configName (ex: lisval_fou_production)
============================================================================
"""

import re
from pathlib import Path
from typing import Optional, Tuple
from datetime import datetime


def parse_parquet_filename(filename: str) -> dict:
    """
    Parse un nom de fichier parquet pour extraire table/config et timestamp
    
    Formats supportés :
    - client_20241201.parquet (simple TableName + date)
    - client_20241201_143022.parquet (TableName + datetime)
    - lisval_fou_production_20241201.parquet (ConfigName + date)
    - lisval_fou_production_20241201_143022.parquet (ConfigName + datetime)
    
    Args:
        filename: Nom du fichier (avec ou sans extension)
    
    Returns:
        dict: {
            'table_identifier': str,  # Le nom complet (table ou config)
            'timestamp': Optional[datetime],
            'has_time': bool,
            'original_filename': str
        }
    
    Exemples:
        >>> parse_parquet_filename("client_20241201.parquet")
        {
            'table_identifier': 'client',
            'timestamp': datetime(2024, 12, 1),
            'has_time': False,
            'original_filename': 'client_20241201.parquet'
        }
        
        >>> parse_parquet_filename("lisval_fou_production_20241201_143022.parquet")
        {
            'table_identifier': 'lisval_fou_production',
            'timestamp': datetime(2024, 12, 1, 14, 30, 22),
            'has_time': True,
            'original_filename': 'lisval_fou_production_20241201_143022.parquet'
        }
    """
    
    # Enlever extension
    stem = Path(filename).stem
    
    # Pattern : capture tout avant le dernier groupe de chiffres
    # Les patterns possibles :
    # 1. nom_YYYYMMDD
    # 2. nom_YYYYMMDD_HHMMSS
    
    # Essayer avec datetime complet (YYYYMMDD_HHMMSS)
    pattern_datetime = r'^(.+?)_(\d{8})_(\d{6})$'
    match = re.match(pattern_datetime, stem)
    
    if match:
        table_id = match.group(1)
        date_str = match.group(2)
        time_str = match.group(3)
        
        try:
            timestamp = datetime.strptime(
                f"{date_str}{time_str}",
                "%Y%m%d%H%M%S"
            )
            
            return {
                'table_identifier': table_id,
                'timestamp': timestamp,
                'has_time': True,
                'original_filename': filename
            }
        except ValueError:
            pass  # Fallback au pattern date seule
    
    # Essayer avec date seule (YYYYMMDD)
    pattern_date = r'^(.+?)_(\d{8})$'
    match = re.match(pattern_date, stem)
    
    if match:
        table_id = match.group(1)
        date_str = match.group(2)
        
        try:
            timestamp = datetime.strptime(date_str, "%Y%m%d")
            
            return {
                'table_identifier': table_id,
                'timestamp': timestamp,
                'has_time': False,
                'original_filename': filename
            }
        except ValueError:
            pass  # Fallback au nom complet
    
    # Si aucun pattern ne match, retourner le stem complet
    return {
        'table_identifier': stem,
        'timestamp': None,
        'has_time': False,
        'original_filename': filename
    }


def resolve_table_names(
    parsed_info: dict,
    metadata: Optional[dict] = None
) -> dict:
    """
    Résout les noms de table en utilisant metadata si disponible
    
    Args:
        parsed_info: Résultat de parse_parquet_filename()
        metadata: Contenu du fichier *_metadata.json
    
    Returns:
        dict: {
            'table_name': str,        # TableName business
            'config_name': Optional[str],  # ConfigName si existe
            'physical_name': str,     # Nom physique pour RAW/STAGING (ConfigName > TableName)
            'file_identifier': str    # Ce qui était dans le nom de fichier
        }
    
    Exemples:
        >>> # Cas simple sans ConfigName
        >>> resolve_table_names(
        ...     {'table_identifier': 'client'},
        ...     {'table_name': 'client'}
        ... )
        {
            'table_name': 'client',
            'config_name': None,
            'physical_name': 'client',
            'file_identifier': 'client'
        }
        
        >>> # Cas avec ConfigName
        >>> resolve_table_names(
        ...     {'table_identifier': 'lisval_fou_production'},
        ...     {'table_name': 'lisval', 'config_name': 'lisval_fou_production'}
        ... )
        {
            'table_name': 'lisval',
            'config_name': 'lisval_fou_production',
            'physical_name': 'lisval_fou_production',
            'file_identifier': 'lisval_fou_production'
        }
    """
    
    file_id = parsed_info['table_identifier']
    
    # Si pas de metadata, utiliser file_identifier pour tout
    if not metadata:
        return {
            'table_name': file_id,
            'config_name': None,
            'physical_name': file_id,
            'file_identifier': file_id
        }
    
    # Extraire depuis metadata
    table_name = metadata.get('table_name', file_id)
    config_name = metadata.get('config_name')
    
    # Déterminer nom physique :
    # Si config_name existe et diffère de table_name, utiliser config_name
    # Sinon utiliser table_name
    if config_name and config_name.lower() != table_name.lower():
        physical_name = config_name
    else:
        physical_name = table_name
        config_name = None  # Pas vraiment de config différente
    
    return {
        'table_name': table_name,
        'config_name': config_name,
        'physical_name': physical_name,
        'file_identifier': file_id
    }


def validate_names_consistency(
    parsed_info: dict,
    resolved_names: dict,
    strict: bool = False
) -> Tuple[bool, Optional[str]]:
    """
    Valide la cohérence entre nom de fichier et metadata
    
    Args:
        parsed_info: Résultat de parse_parquet_filename()
        resolved_names: Résultat de resolve_table_names()
        strict: Si True, le nom fichier DOIT matcher exactement
    
    Returns:
        Tuple[bool, Optional[str]]: (is_valid, error_message)
    
    Exemples:
        >>> # Cas OK : fichier = config_name
        >>> validate_names_consistency(
        ...     {'table_identifier': 'lisval_fou_production'},
        ...     {'config_name': 'lisval_fou_production', 'physical_name': 'lisval_fou_production'}
        ... )
        (True, None)
        
        >>> # Cas WARNING : fichier = table_name mais config_name existe
        >>> validate_names_consistency(
        ...     {'table_identifier': 'lisval'},
        ...     {'table_name': 'lisval', 'config_name': 'lisval_fou_production'}
        ... )
        (True, 'WARNING: Filename uses TableName but ConfigName exists')
    """
    
    file_id = parsed_info['table_identifier'].lower()
    table_name = resolved_names['table_name'].lower()
    config_name = resolved_names.get('config_name')
    physical_name = resolved_names['physical_name'].lower()
    
    # Cas 1 : Match parfait avec physical_name (idéal)
    if file_id == physical_name:
        return True, None
    
    # Cas 2 : Match avec table_name mais config_name existe (warning)
    if config_name and file_id == table_name:
        msg = (
            f"WARNING: Filename uses TableName '{table_name}' "
            f"but ConfigName '{config_name}' exists. "
            f"Recommend using ConfigName in filename."
        )
        return not strict, msg
    
    # Cas 3 : Aucun match (erreur)
    msg = (
        f"ERROR: Filename identifier '{file_id}' doesn't match "
        f"TableName '{table_name}'"
    )
    if config_name:
        msg += f" or ConfigName '{config_name}'"
    
    return False, msg


# ============================================================================
# Fonction helper principale
# ============================================================================

def parse_and_resolve(
    filename: str,
    metadata: Optional[dict] = None,
    strict: bool = False
) -> dict:
    """
    Parse complet d'un fichier avec validation
    
    Args:
        filename: Nom du fichier parquet
        metadata: Metadata JSON si disponible
        strict: Validation stricte des noms
    
    Returns:
        dict: Toutes les infos parsées + validation
        {
            'table_identifier': str,
            'timestamp': Optional[datetime],
            'table_name': str,
            'config_name': Optional[str],
            'physical_name': str,
            'is_valid': bool,
            'validation_message': Optional[str]
        }
    """
    
    # 1. Parser filename
    parsed = parse_parquet_filename(filename)
    
    # 2. Résoudre noms
    resolved = resolve_table_names(parsed, metadata)
    
    # 3. Valider
    is_valid, msg = validate_names_consistency(parsed, resolved, strict)
    
    # 4. Combiner tout
    result = {
        **parsed,
        **resolved,
        'is_valid': is_valid,
        'validation_message': msg
    }
    
    return result


# ============================================================================
# Tests unitaires
# ============================================================================

if __name__ == "__main__":
    import json
    
    print("="*80)
    print("TESTS - Parsing Noms Fichiers")
    print("="*80)
    
    # Test 1 : Simple table
    print("\n[TEST 1] Simple table sans ConfigName")
    result = parse_and_resolve(
        "client_20241201.parquet",
        metadata={'table_name': 'client'}
    )
    print(json.dumps(result, indent=2, default=str))
    
    # Test 2 : Table avec ConfigName
    print("\n[TEST 2] Table avec ConfigName")
    result = parse_and_resolve(
        "lisval_fou_production_20241201_143022.parquet",
        metadata={
            'table_name': 'lisval',
            'config_name': 'lisval_fou_production'
        }
    )
    print(json.dumps(result, indent=2, default=str))
    
    # Test 3 : Fichier utilise TableName mais ConfigName existe (WARNING)
    print("\n[TEST 3] Filename avec TableName mais ConfigName existe")
    result = parse_and_resolve(
        "lisval_20241201.parquet",
        metadata={
            'table_name': 'lisval',
            'config_name': 'lisval_fou_production'
        }
    )
    print(json.dumps(result, indent=2, default=str))
    
    # Test 4 : Pas de metadata
    print("\n[TEST 4] Pas de metadata")
    result = parse_and_resolve(
        "unknown_table_20241201.parquet"
    )
    print(json.dumps(result, indent=2, default=str))
    
    print("\n" + "="*80)
    print("Tests terminés")
    print("="*80)