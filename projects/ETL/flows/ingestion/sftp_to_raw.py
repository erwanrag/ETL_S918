"""
============================================================================
Flow Prefect : SFTP → RAW
============================================================================
"""

import sys
import os
import json
from pathlib import Path
from datetime import datetime
from io import StringIO
from typing import Optional, List

import pandas as pd
import psycopg2
import pyarrow.parquet as pq
import pyarrow.compute as pc
from psycopg2.extras import Json
from sqlalchemy import create_engine

from prefect import flow, task
from prefect.logging import get_run_logger

# Imports configuration
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from shared.config import config, sftp_config  # <--- IMPORT CORRECT
from shared.utils.file_operations import archive_and_cleanup
from utils.filename_parser import parse_and_resolve
from utils.metadata_helper import get_table_metadata

@task(name="[SCAN] Scanner SFTP parquet")
def scan_sftp_directory():
    logger = get_run_logger()
    # CORRECTION ICI : sftp_config
    parquet_dir = Path(sftp_config.sftp_parquet_dir)
    files = list(parquet_dir.glob("*.parquet"))
    logger.info(f"[DATA] {len(files)} fichier(s) trouves")
    return [str(f) for f in files]


@task(name="[META] Read Metadata JSON")
def read_metadata_json(parquet_path: str):
    logger = get_run_logger()
    base = Path(parquet_path).stem
    
    # CORRECTION ICI : sftp_config au lieu de config
    meta_path = Path(sftp_config.sftp_metadata_dir) / f"{base}_metadata.json"

    if not meta_path.exists():
        logger.warning(f"[WARN] Metadata introuvable : {meta_path}")
        return None

    with open(meta_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    logger.info(f"[FILE] Metadata lu : {meta_path.name}")
    return data


@task(name="[DATA] Read status JSON")
def read_status_json(parquet_path: str):
    logger = get_run_logger()
    base = Path(parquet_path).stem
    
    # CORRECTION ICI : sftp_config au lieu de config
    status_path = Path(sftp_config.sftp_status_dir) / f"{base}_status.json"

    if not status_path.exists():
        return None

    with open(status_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data


@task(name="[LOG] Logger table sftp_monitoring")
def log_file_to_monitoring(file_path: str, metadata: dict, status: dict):
    """
    Logger fichier SFTP dans sftp_monitoring.sftp_file_log
    """
    logger = get_run_logger()
    file_name = os.path.basename(file_path)
    file_size = os.path.getsize(file_path)

    full_meta = {
        "file_extension": ".parquet",
        "detected_timestamp": datetime.now().isoformat(),
        "phase1_metadata": metadata or {}, 
        "phase1_status": status or {}      
    }

    load_mode = None
    if status:
        load_mode = status.get('load_mode', 'UNKNOWN')
    elif metadata:
        load_mode = metadata.get('load_mode', 'UNKNOWN')
    
    logger.info(f"[LIST] {file_name} - load_mode: {load_mode}")

    # ICI ON GARDE 'config' car c'est la BDD (Postgres)
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    try:
        cur.execute("""
            SELECT sftp_monitoring.log_new_file(%s, %s, %s, %s, %s)
        """, (file_name, file_path, file_size, "CBM", Json(full_meta)))

        log_id = cur.fetchone()[0]
        conn.commit()
        
        logger.info(f"[NOTE] Log ID {log_id} cree")
        return log_id
        
    except Exception as e:
        logger.error(f"[ERROR] Erreur log_file_to_monitoring : {e}")
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


@task(name="[LOAD] Load in RAW (DROP + CREATE + COPY)")
def load_to_raw(parquet_path: str, log_id: int, metadata: dict):
    """
    Charge fichier parquet dans RAW avec parsing ConfigName correct
    """
    logger = get_run_logger()
    
    file_name = Path(parquet_path).name
    
    # Etape 1 : Parse
    names = parse_and_resolve(file_name, metadata, strict=False)
    
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
    
    logger.info("="*80)
    
    # Etape 2 : Nom table RAW
    table_name = f"raw_{names['physical_name'].lower()}"
    full_table = f"{config.schema_raw}.{table_name}"
    
    logger.info(f"[TARGET] Table RAW : {full_table}")
    
    # Etape 3 : DROP
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    try:
        logger.info(f"[DROP] DROP TABLE IF EXISTS {full_table}")
        cur.execute(f'DROP TABLE IF EXISTS {full_table} CASCADE;')
        conn.commit()
        
        # Etape 4 : CREATE
        logger.info(f"[SCHEMA] Lecture schema Parquet")
        parquet_file = pq.ParquetFile(parquet_path)
        
        if parquet_file.num_row_groups == 0 or parquet_file.metadata.num_rows == 0:
            logger.warning(f"[SKIP] Fichier parquet VIDE : {parquet_path}")
            return {
                'rows_loaded': 0,
                'table_name': table_name,
                'skipped': True,
                'reason': 'empty_file'
            }

        df_sample = parquet_file.read_row_group(
            0, 
            columns=parquet_file.schema.names
        ).to_pandas().head(1)
        
        df_sample["_loaded_at"] = datetime.now()
        df_sample["_source_file"] = file_name
        df_sample["_sftp_log_id"] = log_id
        
        logger.info(f"[CREATE] CREATE TABLE {full_table}")
        engine = create_engine(config.get_sqlalchemy_url(config.schema_raw))
        metadata_types = get_table_metadata(names['table_name'])

        if metadata_types:
            for col in df_sample.columns:
                if col in metadata_types:
                    expected_type = metadata_types[col]['data_type']
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
        
        # Etape 5 : COPY
        logger.info("[COPY] Chargement donnees (streaming)")
        
        col_list = ",".join([f'"{c}"' for c in df_sample.columns])
        copy_sql = (
            f'COPY {full_table} ({col_list}) '
            f"FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', NULL '\\N');"
        )
        
        batch_size = 50000
        total_rows = 0
        
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
        
        # ANALYZE
        logger.info(f"[ANALYZE] {full_table}")
        cur.execute(f"ANALYZE {full_table};")
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


@task(name="[ARCHIVE] Archive File")
def archive_files(parquet_path: str):
    logger = get_run_logger()
    base = Path(parquet_path).stem
    
    # ICI : On utilise directement sftp_config importé globalement
    
    incoming_paths = {
        'parquet': Path(parquet_path),
        'metadata': sftp_config.sftp_metadata_dir / f"{base}_metadata.json",
        'status': sftp_config.sftp_status_dir / f"{base}_status.json"
    }
    
    archive_and_cleanup(
        base_filename=base,
        archive_root=sftp_config.sftp_processed_dir,
        incoming_paths=incoming_paths,
        sftp_server_paths={},
        logger=logger
    )


@flow(name="[01] 📥 SFTP → RAW")
def sftp_to_raw_flow(table_filter=None):
    """
    Flow d'ingestion brute : SFTP → RAW
    """
    logger = get_run_logger()

    files = scan_sftp_directory()

    if not files:
        logger.info("[INFO] Aucun fichier parquet")
        return {
            "tables_loaded": 0, 
            "total_rows": 0,
            "tables": [],
            "table_sizes": {}
        }

    total_rows = 0
    tables_loaded = []
    table_sizes = {}
    
    for f in files:
        try:
            file_name = Path(f).name
            table_name_from_file = file_name.split('_')[0]
            
            if table_filter and table_name_from_file not in table_filter:
                logger.info(f"[FILTER] Skip {table_name_from_file} - non demande")
                continue
            
            meta = read_metadata_json(f)
            status = read_status_json(f)

            if not meta:
                logger.warning(f"[WARN] Skip {f} - pas de metadata")
                continue

            table_name = meta.get('table_name', 'unknown')
            row_count = status.get('row_count', 0) if status else 0
            
            logger.info(f"[TARGET] Traitement de {table_name}")
            
            log_id = log_file_to_monitoring(f, meta, status)
            result = load_to_raw(f, log_id, meta)
            
            if result.get('skipped', False):
                logger.warning(f"[SKIP] {table_name} - Raison: {result.get('reason', 'unknown')}")
                
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
                
                archive_files(f)
                logger.info(f"[INFO] Fichier archive mais table non creee")
                continue
            
            archive_files(f)
            
            total_rows += result['rows_loaded']
            
            physical_name = result["physical_name"]
            tables_loaded.append(physical_name)
            
            if physical_name in table_sizes:
                table_sizes[physical_name] = max(table_sizes[physical_name], row_count)
            else:
                table_sizes[physical_name] = row_count
            
            logger.info(f"[OK] Table physique chargee : {physical_name}")
            
        except Exception as e:
            logger.error(f"[ERROR] Erreur {f} : {e}")
            continue

    logger.info(f"[TARGET] TERMINE : {len(tables_loaded)} table(s), {total_rows:,} lignes")
    
    return {
        "tables_loaded": len(tables_loaded),
        "total_rows": total_rows,
        "tables": tables_loaded,
        "table_sizes": table_sizes
    }


if __name__ == "__main__":
    sftp_to_raw_flow()