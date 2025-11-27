"""
============================================================================
Validation Tasks - Validation enrichie des fichiers SFTP
============================================================================
Fichier : tasks/validation_tasks.py

Tasks pour valider les fichiers avant chargement :
- Validation schéma (colonnes attendues vs réelles)
- Validation row count (metadata vs parquet)
- Validation primary keys (NOT NULL + unicité)
============================================================================
"""

from prefect import task
from prefect.logging import get_run_logger
import pandas as pd
import json
from pathlib import Path
from typing import Dict, List
import sys

sys.path.append(r'E:\Prefect\projects\ETL')
from utils.metadata_helper import get_table_columns, get_primary_keys


@task(name="✅ Valider schéma fichier", retries=1)
def validate_schema(parquet_path: str, metadata_path: str, table_name: str) -> Dict:
    """
    Valider que le schéma du fichier correspond aux métadonnées
    
    Returns:
        Dict avec validation_passed (bool) et errors (list)
    """
    logger = get_run_logger()
    
    result = {
        'validation_passed': True,
        'errors': [],
        'warnings': []
    }
    
    try:
        # 1. Lire metadata.json
        with open(metadata_path, 'r', encoding='utf-8') as f:
            metadata = json.load(f)
        
        actual_cols = set(metadata.get('columns', []))
        
        # 2. Récupérer colonnes attendues depuis ProginovColumns
        expected_cols_data = get_table_columns(table_name)
        expected_cols = set([col['column_name'] for col in expected_cols_data])
        
        # 3. Vérifier colonnes manquantes / en trop
        missing_cols = expected_cols - actual_cols
        extra_cols = actual_cols - expected_cols
        
        if missing_cols:
            result['errors'].append(f"Colonnes manquantes: {missing_cols}")
            result['validation_passed'] = False
        
        if extra_cols:
            result['warnings'].append(f"Colonnes supplémentaires: {extra_cols}")
        
        # 4. Vérifier que le parquet contient bien les colonnes
        df = pd.read_parquet(parquet_path, engine='pyarrow')
        parquet_cols = set(df.columns)
        
        cols_in_metadata_not_in_parquet = actual_cols - parquet_cols
        if cols_in_metadata_not_in_parquet:
            result['errors'].append(f"Colonnes dans metadata mais pas dans parquet: {cols_in_metadata_not_in_parquet}")
            result['validation_passed'] = False
        
        if result['validation_passed']:
            logger.info(f"✅ Schéma validé : {len(actual_cols)} colonnes")
        else:
            logger.error(f"❌ Validation schéma échouée : {result['errors']}")
        
        return result
        
    except Exception as e:
        logger.error(f"❌ Erreur validation schéma : {e}")
        return {
            'validation_passed': False,
            'errors': [str(e)],
            'warnings': []
        }


@task(name="✅ Valider row count", retries=1)
def validate_row_count(parquet_path: str, metadata_path: str) -> Dict:
    """
    Valider que le row count correspond entre metadata et parquet
    
    Returns:
        Dict avec validation_passed (bool) et row counts
    """
    logger = get_run_logger()
    
    try:
        # 1. Row count depuis metadata.json
        with open(metadata_path, 'r', encoding='utf-8') as f:
            metadata = json.load(f)
        
        metadata_row_count = metadata.get('row_count', 0)
        
        # 2. Row count depuis parquet
        df = pd.read_parquet(parquet_path, engine='pyarrow')
        parquet_row_count = len(df)
        
        # 3. Comparer
        validation_passed = (metadata_row_count == parquet_row_count)
        
        result = {
            'validation_passed': validation_passed,
            'metadata_row_count': metadata_row_count,
            'parquet_row_count': parquet_row_count,
            'difference': abs(metadata_row_count - parquet_row_count)
        }
        
        if validation_passed:
            logger.info(f"✅ Row count validé : {parquet_row_count} lignes")
        else:
            logger.error(f"❌ Row count mismatch : metadata={metadata_row_count}, parquet={parquet_row_count}")
        
        return result
        
    except Exception as e:
        logger.error(f"❌ Erreur validation row count : {e}")
        return {
            'validation_passed': False,
            'error': str(e)
        }


@task(name="✅ Valider primary keys", retries=1)
def validate_primary_keys(parquet_path: str, table_name: str) -> Dict:
    """
    Valider primary keys :
    - Pas de NULL
    - Pas de doublons
    
    Returns:
        Dict avec validation_passed (bool) et erreurs
    """
    logger = get_run_logger()
    
    try:
        # 1. Récupérer PK depuis metadata
        pk_cols = get_primary_keys(table_name)
        
        if not pk_cols:
            logger.warning(f"⚠️ Aucune PK définie pour {table_name}")
            return {
                'validation_passed': True,
                'warning': 'No PK defined'
            }
        
        # 2. Lire parquet
        df = pd.read_parquet(parquet_path, engine='pyarrow')
        
        result = {
            'validation_passed': True,
            'errors': [],
            'pk_columns': pk_cols
        }
        
        # 3. Vérifier NOT NULL sur PK
        for pk in pk_cols:
            if pk not in df.columns:
                result['errors'].append(f"PK {pk} manquante dans fichier")
                result['validation_passed'] = False
                continue
            
            null_count = df[pk].isna().sum()
            if null_count > 0:
                result['errors'].append(f"PK {pk} a {null_count} valeurs NULL")
                result['validation_passed'] = False
        
        # 4. Vérifier unicité PK
        if result['validation_passed']:
            duplicate_count = df.duplicated(subset=pk_cols).sum()
            if duplicate_count > 0:
                result['errors'].append(f"{duplicate_count} doublons sur PK {pk_cols}")
                result['validation_passed'] = False
        
        if result['validation_passed']:
            logger.info(f"✅ PK validées : {pk_cols}")
        else:
            logger.error(f"❌ Validation PK échouée : {result['errors']}")
        
        return result
        
    except Exception as e:
        logger.error(f"❌ Erreur validation PK : {e}")
        return {
            'validation_passed': False,
            'errors': [str(e)]
        }


@task(name="✅ Validation complète fichier")
def validate_file_complete(parquet_path: str, metadata_path: str, table_name: str) -> Dict:
    """
    Validation complète : schéma + row count + PK
    
    Returns:
        Dict avec validation_passed (bool) global
    """
    logger = get_run_logger()
    
    logger.info(f"🔍 Validation complète de {Path(parquet_path).name}")
    
    # 1. Validation schéma
    schema_result = validate_schema(parquet_path, metadata_path, table_name)
    
    # 2. Validation row count
    rowcount_result = validate_row_count(parquet_path, metadata_path)
    
    # 3. Validation PK
    pk_result = validate_primary_keys(parquet_path, table_name)
    
    # 4. Résultat global
    all_passed = (
        schema_result['validation_passed'] and
        rowcount_result['validation_passed'] and
        pk_result['validation_passed']
    )
    
    result = {
        'validation_passed': all_passed,
        'schema_validation': schema_result,
        'rowcount_validation': rowcount_result,
        'pk_validation': pk_result
    }
    
    if all_passed:
        logger.info("✅ Validation complète RÉUSSIE")
    else:
        logger.error("❌ Validation complète ÉCHOUÉE")
        
        # Logger tous les détails
        if not schema_result['validation_passed']:
            logger.error(f"  - Schéma : {schema_result['errors']}")
        if not rowcount_result['validation_passed']:
            logger.error(f"  - Row count : {rowcount_result}")
        if not pk_result['validation_passed']:
            logger.error(f"  - PK : {pk_result['errors']}")
    
    return result


# ============================================================================
# TEST
# ============================================================================

if __name__ == "__main__":
    # Test validation sur fichier existant
    test_parquet = r"E:\SFTP_Mirroring\Incoming\data\parquet\client_20251119_165323.parquet"
    test_metadata = r"E:\SFTP_Mirroring\Incoming\data\metadata\client_20251119_165323_metadata.json"
    
    if Path(test_parquet).exists():
        print("Test validation...")
        result = validate_file_complete(test_parquet, test_metadata, "client")
        print(f"\nRésultat : {result['validation_passed']}")
    else:
        print("Fichier test introuvable")