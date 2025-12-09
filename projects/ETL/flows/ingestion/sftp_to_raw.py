"""
============================================================================
Flow Prefect : SFTP → RAW
============================================================================
Responsabilité : Ingestion brute des fichiers SFTP dans PostgreSQL RAW
- Pas de transformation
- Pas de hashdiff
- DROP + CREATE + COPY
============================================================================
"""

import os
import json
from pathlib import Path
from datetime import datetime
from io import StringIO
import pandas as pd
import psycopg2
import pyarrow.parquet as pq
from psycopg2.extras import Json
from prefect import flow, task
from prefect.logging import get_run_logger
from sqlalchemy import create_engine
import sys

sys.path.append(r'E:\Prefect\projects\ETL')
from flows.config.pg_config import config
from utils.file_operations import archive_and_cleanup
from utils.filename_parser import parse_and_resolve
from utils.metadata_helper import get_table_metadata

@task(name="[OPEN] Scanner SFTP parquet")
def scan_sftp_directory():
    logger = get_run_logger()
    parquet_dir = Path(config.sftp_parquet_dir)
    files = list(parquet_dir.glob("*.parquet"))
    logger.info(f"[DATA] {len(files)} fichier(s) trouvés")
    return [str(f) for f in files]


@task(name="[FILE] Lire metadata JSON")
def read_metadata_json(parquet_path: str):
    logger = get_run_logger()
    base = Path(parquet_path).stem
    meta_path = Path(config.sftp_metadata_dir) / f"{base}_metadata.json"

    if not meta_path.exists():
        logger.warning(f"[WARN] Metadata introuvable : {meta_path}")
        return None

    with open(meta_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    logger.info(f"[FILE] Metadata lu : {meta_path.name}")
    return data


@task(name="[DATA] Lire status JSON")
def read_status_json(parquet_path: str):
    logger = get_run_logger()
    base = Path(parquet_path).stem
    status_path = Path(config.sftp_status_dir) / f"{base}_status.json"

    if not status_path.exists():
        return None

    with open(status_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data


@task(name="[NOTE] Logger dans sftp_monitoring")
def log_file_to_monitoring(file_path: str, metadata: dict, status: dict):
    """
    Logger fichier SFTP dans sftp_monitoring.sftp_file_log
    
    La fonction PostgreSQL log_new_file() extrait automatiquement :
    - load_mode (depuis status ou metadata)
    - table_name (depuis status ou metadata)
    - row_count (depuis status ou metadata)
    - run_id (depuis status ou metadata)
    
    Args:
        file_path: Chemin complet du fichier .parquet
        metadata: Contenu du fichier *_metadata.json
        status: Contenu du fichier *_status.json
    
    Returns:
        int: log_id créé dans sftp_file_log
    """
    logger = get_run_logger()
    file_name = os.path.basename(file_path)
    file_size = os.path.getsize(file_path)

    # [OK] AMÉLIORATION : Structure metadata_json claire
    full_meta = {
        "file_extension": ".parquet",
        "detected_timestamp": datetime.now().isoformat(),
        "phase1_metadata": metadata or {},  # Contenu metadata.json
        "phase1_status": status or {}       # Contenu status.json
    }

    # Log le load_mode détecté pour debug
    load_mode = None
    if status:
        load_mode = status.get('load_mode', 'UNKNOWN')
    elif metadata:
        load_mode = metadata.get('load_mode', 'UNKNOWN')
    
    logger.info(f"[LIST] {file_name} - load_mode: {load_mode}")

    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    try:
        # Appel fonction PostgreSQL (extraction automatique)
        cur.execute("""
            SELECT sftp_monitoring.log_new_file(%s, %s, %s, %s, %s)
        """, (file_name, file_path, file_size, "CBM", Json(full_meta)))

        log_id = cur.fetchone()[0]
        conn.commit()
        
        logger.info(f"[NOTE] Log ID {log_id} créé")
        
        return log_id
        
    except Exception as e:
        logger.error(f"[ERROR] Erreur log_file_to_monitoring : {e}")
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


@task(name="📥 Charger dans RAW (DROP + CREATE + COPY) [FIXED]")
def load_to_raw(parquet_path: str, log_id: int, metadata: dict):
    """
    Charge fichier parquet dans RAW avec parsing ConfigName correct
    
    Args:
        parquet_path: Chemin fichier .parquet
        log_id: ID du log sftp_monitoring
        metadata: Contenu *_metadata.json
    
    Returns:
        dict: {
            'rows_loaded': int,
            'table_name': str,        # Nom business
            'config_name': str|None,  # ConfigName si existe
            'physical_name': str,     # Nom physique table RAW
            'full_table': str         # Nom complet avec schema
        }
    """
    logger = get_run_logger()
    
    file_name = Path(parquet_path).name
    
    # ========================================================================
    # ÉTAPE 1 : Parse et résout les noms avec validation
    # ========================================================================
    
    names = parse_and_resolve(file_name, metadata, strict=False)
    
    # Log infos parsing
    logger.info("="*80)
    logger.info("📋 PARSING FICHIER")
    logger.info("="*80)
    logger.info(f"Fichier           : {file_name}")
    logger.info(f"File Identifier   : {names['table_identifier']}")
    logger.info(f"TableName         : {names['table_name']}")
    logger.info(f"ConfigName        : {names['config_name'] or 'N/A'}")
    logger.info(f"Physical Name     : {names['physical_name']}")
    logger.info(f"Valid             : {names['is_valid']}")
    
    if names['validation_message']:
        if names['is_valid']:
            logger.warning(f"⚠️  {names['validation_message']}")
        else:
            logger.error(f"❌ {names['validation_message']}")
            # En mode non-strict, on continue quand même
    
    logger.info("="*80)
    
    # ========================================================================
    # ÉTAPE 2 : Construire nom table RAW
    # ========================================================================
    
    # Utiliser physical_name (qui prend ConfigName si existe)
    table_name = f"raw_{names['physical_name'].lower()}"
    full_table = f"{config.schema_raw}.{table_name}"
    
    logger.info(f"[TARGET] Table RAW : {full_table}")
    
    # ========================================================================
    # ÉTAPE 3 : DROP table existante
    # ========================================================================
    
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    try:
        logger.info(f"[DROP] DROP TABLE IF EXISTS {full_table}")
        cur.execute(f'DROP TABLE IF EXISTS {full_table} CASCADE;')
        conn.commit()
        
        # ====================================================================
        # ÉTAPE 4 : CREATE table avec schema Parquet
        # ====================================================================
        
        logger.info(f"[SCHEMA] Lecture schema Parquet")
        parquet_file = pq.ParquetFile(parquet_path)
        # VERIFIER SI VIDE
        if parquet_file.num_row_groups == 0 or parquet_file.metadata.num_rows == 0:
            logger.warning(f"[SKIP] Fichier parquet VIDE : {parquet_path}")
            return {
                'rows_loaded': 0,
                'table_name': table_name,
                'skipped': True,
                'reason': 'empty_file'
            }

        # Sinon, continuer normalement
        df_sample = parquet_file.read_row_group(
            0, 
            columns=parquet_file.schema.names
        ).to_pandas().head(1)
        
        # Ajouter colonnes ETL
        df_sample["_loaded_at"] = datetime.now()
        df_sample["_source_file"] = file_name
        df_sample["_sftp_log_id"] = log_id
        
        logger.info(f"[CREATE] CREATE TABLE {full_table}")
        engine = create_engine(config.get_sqlalchemy_url(config.schema_raw))
        metadata_types = get_table_metadata(names['table_name'])

        if metadata_types:
            # Convertir les colonnes FLOAT qui doivent être INTEGER
            for col in df_sample.columns:
                if col in metadata_types:
                    expected_type = metadata_types[col]['data_type']
                    
                    # Si metadata dit INTEGER mais pandas a float
                    if expected_type == 'INTEGER' and df_sample[col].dtype == 'float64':
                        df_sample[col] = df_sample[col].fillna(0).astype('int64')

        df_sample.head(0).to_sql(
            name=table_name,
            con=engine,
            schema=config.schema_raw,
            if_exists="replace",
            index=False
        )
        engine.dispose()
        
        # ====================================================================
        # ÉTAPE 5 : COPY données par batches
        # ====================================================================
        
        logger.info("[COPY] Chargement donnees (streaming)")
        
        col_list = ",".join([f'"{c}"' for c in df_sample.columns])
        copy_sql = (
            f'COPY {full_table} ({col_list}) '
            f"FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', NULL '\\N');"
        )
        
        batch_size = 50000
        total_rows = 0
        
        # Identifier colonnes INTEGER depuis metadata
        integer_cols = set()
        if metadata_types:
            for col, col_info in metadata_types.items():
                if col in df_sample.columns:
                    expected_type = (col_info.get('progress_type') or col_info.get('data_type') or '').upper()
                    if expected_type in ('INTEGER', 'INT', 'INT64'):
                        integer_cols.add(col)
        
        for i, batch in enumerate(parquet_file.iter_batches(batch_size=batch_size), 1):
            chunk = batch.to_pandas()
            chunk["_loaded_at"] = datetime.now()
            chunk["_source_file"] = file_name
            chunk["_sftp_log_id"] = log_id
            
            # Convertir colonnes INTEGER pour ce batch
            for col in integer_cols:
                if col in chunk.columns and chunk[col].dtype == 'float64':
                    chunk[col] = chunk[col].fillna(0).astype('int64')
            
            output = StringIO()
            chunk.to_csv(output, sep="\t", header=False, index=False, na_rep="\\N")
            output.seek(0)
            
            cur.copy_expert(copy_sql, output)
            
            total_rows += len(chunk)
            if i % 10 == 0:
                logger.info(f"  [{total_rows:,} lignes chargees]")
        
        conn.commit()


        
        logger.info("="*80)
        logger.info(f"✅ SUCCESS")
        logger.info(f"   Table    : {full_table}")
        logger.info(f"   Lignes   : {total_rows:,}")
        logger.info(f"   Physical : {names['physical_name']}")
        logger.info("="*80)
        
        return {
            'rows_loaded': total_rows,
            'table_name': names['table_name'],
            'config_name': names['config_name'],
            'physical_name': names['physical_name'],
            'full_table': full_table
        }
        
    except Exception as e:
        conn.rollback()
        logger.error(f"[ERROR] Erreur chargement RAW : {e}")
        raise
        
    finally:
        cur.close()
        conn.close()


@task(name="[PACKAGE] Archiver + Nettoyer SFTP")
def archive_files(parquet_path: str):
    logger = get_run_logger()
    base = Path(parquet_path).stem
    
    incoming_paths = {
        'parquet': Path(parquet_path),
        'metadata': Path(config.sftp_metadata_dir) / f"{base}_metadata.json",
        'status': Path(config.sftp_status_dir) / f"{base}_status.json"
    }
    
    sftp_root = Path(r"C:\ProgramData\ssh\SFTPRoot\Incoming\data")
    sftp_server_paths = {
        'parquet': sftp_root / "parquet" / f"{base}.parquet",
        'metadata': sftp_root / "metadata" / f"{base}_metadata.json",
        'status': sftp_root / "status" / f"{base}_status.json"
    }
    
    archive_and_cleanup(
        base_filename=base,
        archive_root=config.sftp_processed_dir,
        incoming_paths=incoming_paths,
        sftp_server_paths=sftp_server_paths,
        logger=logger
    )


@flow(name="📥 SFTP → RAW (Ingestion brute)")
def sftp_to_raw_flow():
    """
    Flow d'ingestion brute : SFTP → RAW
    
    Étapes :
    1. Scanner fichiers SFTP
    2. Charger dans raw.raw_{table} (DROP + CREATE + COPY)
    3. Archiver fichiers
    
    Note : Pas de transformation, pas de hashdiff
    """
    logger = get_run_logger()

    files = scan_sftp_directory()

    if not files:
        logger.info("[INFO] Aucun fichier parquet")
        return {"tables_loaded": 0, "total_rows": 0}

    total_rows = 0
    tables_loaded = []
    
    for f in files:
        try:
            meta = read_metadata_json(f)
            status = read_status_json(f)
            
            if not meta:
                logger.warning(f"[WARN] Skip {f} - pas de metadata")
                continue
            
            table_name = meta.get('table_name', 'unknown')
            logger.info(f"[TARGET] Traitement de {table_name}")
            
            log_id = log_file_to_monitoring(f, meta, status)
            result = load_to_raw(f, log_id, meta)
            
            # ✅ GESTION DES FICHIERS VIDES/SKIPPED
            if result.get('skipped', False):
                logger.warning(f"[SKIP] {table_name} - Raison: {result.get('reason', 'unknown')}")
                
                # ✅ Mettre à jour sftp_monitoring
                conn = psycopg2.connect(config.get_connection_string())
                cur = conn.cursor()
                try:
                    cur.execute("""
                        UPDATE sftp_monitoring.sftp_file_log
                        SET processing_status = 'SKIPPED',
                            error_message = %s,
                            processed_at = NOW()
                        WHERE log_id = %s
                    """, (result.get('reason', 'Fichier vide'), log_id))
                    conn.commit()

                finally:
                    cur.close()
                    conn.close()
                
                # ✅ Archiver quand même le fichier
                archive_files(f)
                
                # ✅ NE PAS ajouter à tables_loaded
                logger.info(f"[INFO] Fichier archive mais table non creee")
                continue  # ← Passer au fichier suivant
            
            # Si pas skipped, continuer normalement
            archive_files(f)
            
            total_rows += result['rows_loaded']
            tables_loaded.append(result["physical_name"])
            
            logger.info(f"[OK] Table physique chargee : {result['physical_name']}")
            
        except Exception as e:
            logger.error(f"[ERROR] Erreur {f} : {e}")
            # Ne pas bloquer le pipeline pour une table en erreur
            continue

    logger.info(f"[TARGET] TERMINE : {len(tables_loaded)} table(s), {total_rows:,} lignes")
    
    return {
        "tables_loaded": len(tables_loaded),
        "total_rows": total_rows,
        "tables": tables_loaded
    }

if __name__ == "__main__":
    sftp_to_raw_flow()