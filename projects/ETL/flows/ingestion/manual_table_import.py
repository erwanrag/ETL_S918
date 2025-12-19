"""
============================================================================
Flow Import Manuel Table Spécifique
============================================================================
Fichier : E:/Prefect/projects/ETL/flows/ingestion/manual_table_import.py

Permet d'importer une table spécifique avec contrôle du load_mode :
- AUTO : Détecte depuis fichier (FULL, INCREMENTAL, FULL_RESET)
- FULL : Force un FULL refresh
- INCREMENTAL : Force un INCREMENTAL
- FULL_RESET : Force un FULL avec reset metadata
============================================================================
"""

from prefect import flow, task
from prefect.logging import get_run_logger
from typing import Optional, Literal
from pathlib import Path
import json
import sys
import psycopg2
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from shared.config import config, sftp_config
from flows.ingestion.sftp_to_raw import (
    read_metadata_json,
    read_status_json, 
    log_file_to_monitoring,
    load_to_raw,
    archive_files
)
from tasks.staging_tasks import create_staging_table, load_raw_to_staging
from tasks.ods_tasks import merge_ods_auto


LoadMode = Literal["AUTO", "FULL", "INCREMENTAL", "FULL_RESET"]


@task(name="🔍 Rechercher fichier table")
def find_table_file(
    table_identifier: str,
    load_mode_override: LoadMode = "AUTO"
) -> Optional[dict]:
    """
    Recherche le fichier parquet d'une table par nom ou configName
    
    Args:
        table_identifier: Nom de table (ex: 'client', 'lisval_fou_production')
        load_mode_override: Mode à forcer ou AUTO pour détecter
    
    Returns:
        dict: {
            'parquet_path': str,
            'metadata': dict,
            'status': dict,
            'detected_load_mode': str,
            'final_load_mode': str,
            'table_name': str,
            'config_name': Optional[str]
        }
    """
    logger = get_run_logger()
    
    # Scanner répertoire parquet
    parquet_dir = sftp_config.sftp_parquet_dir
    
    if not parquet_dir.exists():
        logger.error(f"[ERROR] Répertoire parquet introuvable : {parquet_dir}")
        return None
    
    # Chercher fichier correspondant
    # Formats possibles :
    # - client_20241201_161023.parquet (TableName)
    # - lisval_fou_production_20241201_161023.parquet (ConfigName)
    
    matching_files = []
    for f in parquet_dir.glob("*.parquet"):
        # Extraire nom base (sans date/timestamp)
        stem = f.stem  # ex: client_20241201_161023
        
        # Enlever suffixe timestamp/date
        # Pattern : nomtable_YYYYMMDD_HHMMSS
        parts = stem.split('_')
        
        # Essayer de trouver l'index où commence la date
        possible_table_name = []
        for i, part in enumerate(parts):
            # Si part ressemble à une date (8 chiffres)
            if part.isdigit() and len(part) == 8:
                possible_table_name = '_'.join(parts[:i])
                break
        else:
            # Pas de date trouvée, prendre tout
            possible_table_name = stem
        
        if possible_table_name.lower() == table_identifier.lower():
            matching_files.append(f)
    
    if not matching_files:
        logger.warning(f"[WARN] Aucun fichier trouvé pour table '{table_identifier}'")
        logger.info(f"[INFO] Fichiers disponibles dans {parquet_dir}:")
        for f in parquet_dir.glob("*.parquet"):
            logger.info(f"  - {f.name}")
        return None
    
    # Prendre le plus récent si plusieurs
    parquet_file = max(matching_files, key=lambda p: p.stat().st_mtime)
    logger.info(f"[FOUND] Fichier trouvé : {parquet_file.name}")
    
    # Lire metadata et status
    metadata = read_metadata_json(str(parquet_file))
    status = read_status_json(str(parquet_file))
    
    # Déterminer load_mode
    detected_mode = "FULL"  # défaut
    
    if status and 'load_mode' in status:
        detected_mode = status['load_mode']
    elif metadata and 'load_mode' in metadata:
        detected_mode = metadata['load_mode']
    
    # Appliquer override si nécessaire
    if load_mode_override == "AUTO":
        final_mode = detected_mode
        logger.info(f"[MODE] Mode AUTO détecté : {final_mode}")
    else:
        final_mode = load_mode_override
        logger.info(f"[MODE] Mode forcé : {final_mode} (détecté: {detected_mode})")
    
    # Extraire table_name et config_name
    table_name = metadata.get('table_name', 'unknown') if metadata else 'unknown'
    config_name = metadata.get('config_name') if metadata else None
    
    return {
        'parquet_path': str(parquet_file),
        'metadata': metadata,
        'status': status,
        'detected_load_mode': detected_mode,
        'final_load_mode': final_mode,
        'table_name': table_name,
        'config_name': config_name
    }


@task(name="🔧 Mettre à jour load_mode")
def update_load_mode_in_memory(
    file_info: dict,
    new_mode: str
) -> dict:
    """
    Met à jour le load_mode dans metadata/status en mémoire
    (pas sur disque, juste pour processing)
    """
    logger = get_run_logger()
    
    # Modifier dans metadata
    if file_info['metadata']:
        file_info['metadata']['load_mode'] = new_mode
    
    # Modifier dans status
    if file_info['status']:
        file_info['status']['load_mode'] = new_mode
    
    logger.info(f"[UPDATE] Load mode mis à jour : {new_mode}")
    
    return file_info


@task(name="🎯 Vérifier table en metadata")
def verify_table_metadata(table_identifier: str) -> dict:
    """
    Vérifie que la table existe dans metadata.etl_tables
    et retourne les infos config
    """
    logger = get_run_logger()
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    try:
        # Chercher par ConfigName OU TableName (case-insensitive)
        cur.execute("""
            SELECT 
                "TableName",
                "ConfigName",
                "PrimaryKeyCols",
                "HasTimestamps",
                "IsActive"
            FROM metadata.etl_tables
            WHERE LOWER("ConfigName") = LOWER(%s) OR LOWER("TableName") = LOWER(%s)
            LIMIT 1
        """, (table_identifier, table_identifier))
        
        row = cur.fetchone()
        
        if not row:
            logger.error(f"[ERROR] Table '{table_identifier}' introuvable dans metadata.etl_tables")
            return None
        
        result = {
            'table_name': row[0],
            'config_name': row[1],
            'primary_keys': row[2],
            'has_timestamps': row[3],
            'is_active': row[4]
        }
        
        if not result['is_active']:
            logger.warning(f"[WARN] Table '{table_identifier}' est INACTIVE")
        
        logger.info(f"[METADATA] TableName={result['table_name']}, ConfigName={result['config_name']}")
        
        return result
        
    finally:
        cur.close()
        conn.close()

@flow(name="📋 Import Manuel Table Spécifique")
def manual_table_import_flow(
    table_identifier: str,
    load_mode: LoadMode = "AUTO",
    skip_raw: bool = False,
    skip_staging: bool = False,
    skip_ods: bool = False
):
    """
    Flow pour importer manuellement une table spécifique
    
    Args:
        table_identifier: Nom de table ou ConfigName (ex: 'client', 'lisval_fou_production')
        load_mode: Mode d'import
            - "AUTO" : Détecte depuis fichier
            - "FULL" : Force FULL refresh
            - "INCREMENTAL" : Force incremental
            - "FULL_RESET" : Force FULL avec reset metadata
        skip_raw: Si True, skip la phase RAW (suppose déjà chargé)
        skip_staging: Si True, skip la phase STAGING
        skip_ods: Si True, skip la phase ODS
    
    Exemple:
        # Import auto
        manual_table_import_flow("client")
        
        # Force FULL refresh
        manual_table_import_flow("lisval_fou_production", load_mode="FULL")
        
        # Rejouer seulement STAGING->ODS
        manual_table_import_flow("client", skip_raw=True)
    """
    logger = get_run_logger()
    
    logger.info("="*80)
    logger.info("📋 IMPORT MANUEL TABLE SPÉCIFIQUE")
    logger.info("="*80)
    logger.info(f"Table      : {table_identifier}")
    logger.info(f"Load Mode  : {load_mode}")
    logger.info(f"Skip RAW   : {skip_raw}")
    logger.info(f"Skip STAG  : {skip_staging}")
    logger.info(f"Skip ODS   : {skip_ods}")
    logger.info("="*80)
    
    # 1. Vérifier metadata
    table_meta = verify_table_metadata(table_identifier)
    if not table_meta:
        logger.error("[ABORT] Table introuvable dans metadata")
        return {
            'success': False,
            'error': 'Table non trouvée dans metadata.etl_tables'
        }
    
    # Déterminer le nom physique (ConfigName > TableName)
    physical_name = table_meta['config_name'] or table_meta['table_name']
    
    logger.info(f"[CONFIG] Physical name : {physical_name}")
    
    # 2. Trouver fichier (si on ne skip pas RAW)
    if not skip_raw:
        file_info = find_table_file(table_identifier, load_mode)
        if not file_info:
            logger.error("[ABORT] Fichier parquet introuvable")
            return {
                'success': False,
                'error': 'Fichier parquet non trouvé'
            }
        
        # 3. Appliquer override du load_mode si nécessaire
        if load_mode != "AUTO":
            file_info = update_load_mode_in_memory(file_info, load_mode)
        
        final_mode = file_info['final_load_mode']
    else:
        # Si skip RAW, utiliser le mode passé en paramètre
        final_mode = load_mode if load_mode != "AUTO" else "FULL"
        logger.info(f"[MODE] Mode utilisé (skip RAW) : {final_mode}")
    
    # 4. Phase RAW
    if not skip_raw:
        logger.info("\n" + "="*80)
        logger.info("📥 PHASE RAW")
        logger.info("="*80)
        
        log_id = log_file_to_monitoring(
            file_info['parquet_path'],
            file_info['metadata'],
            file_info['status']
        )
        
        result_raw = load_to_raw(
            file_info['parquet_path'],
            log_id,
            file_info['metadata']
        )
        
        logger.info(f"[OK] RAW : {result_raw['rows_loaded']:,} lignes chargées")
        logger.info(f"[OK] Physical name : {result_raw['physical_name']}")
        
        # NE PAS archiver automatiquement en mode manuel
        # L'utilisateur peut vouloir rejouer
        logger.info("[INFO] Fichier NON archivé (mode manuel)")
    else:
        logger.info("[SKIP] Phase RAW ignorée")
    
    # 5. Phase STAGING
    if not skip_staging:
        logger.info("\n" + "="*80)
        logger.info("🔄 PHASE STAGING")
        logger.info("="*80)
        
        # Créer table staging
        create_staging_table(physical_name)
        
        # Charger données
        run_id = f"manual_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        rows_stg = load_raw_to_staging(physical_name, run_id)
        
        logger.info(f"[OK] STAGING : {rows_stg:,} lignes")
    else:
        logger.info("[SKIP] Phase STAGING ignorée")
    
    # 6. Phase ODS
    if not skip_ods:
        logger.info("\n" + "="*80)
        logger.info("📊 PHASE ODS")
        logger.info("="*80)
        
        # Merge données (avec load_mode)
        run_id = f"manual_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        result_ods = merge_ods_auto(
            table_name=physical_name,
            run_id=run_id,
            load_mode=final_mode
        )
        
        rows_ods = result_ods.get('rows_affected', result_ods.get('rows_inserted', 0))
        logger.info(f"[OK] ODS : {rows_ods:,} lignes affectées")
    else:
        logger.info("[SKIP] Phase ODS ignorée")
    
    logger.info("\n" + "="*80)
    logger.info("✅ IMPORT MANUEL TERMINÉ")
    logger.info("="*80)
    
    return {
        'success': True,
        'table_name': table_meta['table_name'],
        'config_name': table_meta['config_name'],
        'physical_name': physical_name,
        'load_mode': final_mode,
        'phases_executed': {
            'raw': not skip_raw,
            'staging': not skip_staging,
            'ods': not skip_ods
        }
    }


if __name__ == "__main__":
    # Exemples d'utilisation
    
    # 1. Import auto complet
    # manual_table_import_flow("client")
    
    # 2. Force FULL refresh d'une table
    # manual_table_import_flow("lisval_fou_production", load_mode="FULL")
    
    # 3. Rejouer seulement STAGING->ODS
    # manual_table_import_flow("client", skip_raw=True, load_mode="FULL")
    
    # 4. Test avec une table spécifique
    manual_table_import_flow(
        table_identifier="lisval_slimstock",
        load_mode="AUTO"
    )