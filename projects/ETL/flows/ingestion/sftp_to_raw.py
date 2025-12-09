"""
============================================================================
Flow Prefect : SFTP → RAW (OPTIMISÉ - FIX PYARROW)
============================================================================
"""

import os
import json
from pathlib import Path
from datetime import datetime
from io import StringIO
import pandas as pd
import psycopg2
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
from psycopg2.extras import Json
from prefect import flow, task
from prefect.logging import get_run_logger
from prefect.tasks import exponential_backoff
from sqlalchemy import create_engine
import sys
from contextlib import contextmanager
import threading

sys.path.append(r'E:\Prefect\projects\ETL')
from flows.config.pg_config import config
from utils.file_operations import archive_and_cleanup
from utils.filename_parser import parse_and_resolve
from utils.metadata_helper import get_table_metadata


@contextmanager
def timeout(seconds):
    """Timeout pour Windows"""
    def timeout_handler():
        raise TimeoutError(f"Timeout {seconds}s")
    
    timer = threading.Timer(seconds, timeout_handler)
    timer.start()
    try:
        yield
    finally:
        timer.cancel()


@task(name="[OPEN] Scanner SFTP parquet")
def scan_sftp_directory():
    logger = get_run_logger()
    parquet_dir = Path(config.sftp_parquet_dir)
    files = list(parquet_dir.glob("*.parquet"))
    logger.info(f"[DATA] {len(files)} fichier(s)")
    return [str(f) for f in files]


@task(name="[FILE] Lire metadata JSON")
def read_metadata_json(parquet_path: str):
    logger = get_run_logger()
    base = Path(parquet_path).stem
    meta_path = Path(config.sftp_metadata_dir) / f"{base}_metadata.json"

    if not meta_path.exists():
        logger.warning(f"[WARN] Metadata introuvable")
        return None

    with open(meta_path, "r", encoding="utf-8") as f:
        return json.load(f)


@task(name="[DATA] Lire status JSON")
def read_status_json(parquet_path: str):
    base = Path(parquet_path).stem
    status_path = Path(config.sftp_status_dir) / f"{base}_status.json"
    
    if not status_path.exists():
        return None
    
    with open(status_path, "r", encoding="utf-8") as f:
        return json.load(f)


@task(name="[NOTE] Logger monitoring")
def log_file_to_monitoring(file_path: str, metadata: dict, status: dict):
    logger = get_run_logger()
    file_name = os.path.basename(file_path)
    file_size = os.path.getsize(file_path)

    full_meta = {
        "file_extension": ".parquet",
        "detected_timestamp": datetime.now().isoformat(),
        "phase1_metadata": metadata or {},
        "phase1_status": status or {}
    }

    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    try:
        cur.execute("""
            SELECT sftp_monitoring.log_new_file(%s, %s, %s, %s, %s)
        """, (file_name, file_path, file_size, "CBM", Json(full_meta)))

        log_id = cur.fetchone()[0]
        conn.commit()
        logger.info(f"[NOTE] Log ID {log_id}")
        return log_id
        
    finally:
        cur.close()
        conn.close()


@task(
    name="📥 Charger RAW (OPTIMISÉ)",
    retries=3,
    retry_delay_seconds=exponential_backoff(backoff_factor=2),
    timeout_seconds=1800
)
def load_to_raw(parquet_path: str, log_id: int, metadata: dict):
    """Version optimisée avec fallback Pandas si PyArrow CSV pas disponible"""
    logger = get_run_logger()
    file_name = Path(parquet_path).name
    
    # Parse noms
    names = parse_and_resolve(file_name, metadata, strict=False)
    logger.info(f"[TARGET] {names['physical_name']}")
    
    table_name = f"raw_{names['physical_name'].lower()}"
    full_table = f"{config.schema_raw}.{table_name}"
    
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    try:
        with timeout(1800):
            # DROP
            cur.execute(f'DROP TABLE IF EXISTS {full_table} CASCADE;')
            conn.commit()
            
            # Vérifier si vide
            parquet_file = pq.ParquetFile(parquet_path)
            if parquet_file.metadata.num_rows == 0:
                logger.warning(f"[SKIP] Fichier vide")
                return {'rows_loaded': 0, 'skipped': True, 'reason': 'empty_file'}
            
            # CREATE
            df_sample = parquet_file.read_row_group(0).to_pandas().head(1)
            df_sample["_loaded_at"] = datetime.now()
            df_sample["_source_file"] = file_name
            df_sample["_sftp_log_id"] = log_id
            
            engine = create_engine(config.get_sqlalchemy_url(config.schema_raw))
            metadata_types = get_table_metadata(names['table_name'])
            
            # Fix types INTEGER
            if metadata_types:
                for col in df_sample.columns:
                    if col in metadata_types:
                        if metadata_types[col]['data_type'] == 'INTEGER' and df_sample[col].dtype == 'float64':
                            df_sample[col] = df_sample[col].fillna(0).astype('int64')
            
            df_sample.head(0).to_sql(
                name=table_name,
                con=engine,
                schema=config.schema_raw,
                if_exists="replace",
                index=False
            )
            engine.dispose()
            
            # COPY avec PyArrow + fallback Pandas
            col_list = ",".join([f'"{c}"' for c in df_sample.columns])
            copy_sql = f'COPY {full_table} ({col_list}) FROM STDIN WITH (FORMAT CSV, DELIMITER E\'\\t\', NULL \'\\N\');'
            
            # Pre-compute INTEGER colonnes
            integer_cols_set = set()
            if metadata_types:
                integer_cols_set = {
                    col for col, info in metadata_types.items()
                    if info.get('progress_type', '').upper() in ('INTEGER', 'INT', 'INT64')
                }
            
            total_rows = 0
            batch_size = 100000
            
            logger.info("[COPY] Streaming optimisé")
            
            for batch_idx, arrow_batch in enumerate(parquet_file.iter_batches(batch_size=batch_size), 1):
                
                # Cast INTEGER dans PyArrow
                for col in integer_cols_set:
                    if col in arrow_batch.schema.names:
                        col_data = arrow_batch.column(col)
                        if pa.types.is_floating(col_data.type):
                            filled = pc.fill_null(col_data, 0)
                            arrow_batch = arrow_batch.set_column(
                                arrow_batch.schema.get_field_index(col),
                                col,
                                pc.cast(filled, pa.int64())
                            )
                
                # Ajouter colonnes ETL
                arrow_batch = arrow_batch.append_column(
                    '_loaded_at',
                    pa.array([datetime.now()] * len(arrow_batch), type=pa.timestamp('us'))
                )
                arrow_batch = arrow_batch.append_column(
                    '_source_file',
                    pa.array([file_name] * len(arrow_batch), type=pa.string())
                )
                arrow_batch = arrow_batch.append_column(
                    '_sftp_log_id',
                    pa.array([log_id] * len(arrow_batch), type=pa.int64())
                )
                
                # Conversion vers CSV : Utiliser Pandas (compatible toutes versions)
                chunk = arrow_batch.to_pandas()
                
                output = StringIO()
                chunk.to_csv(output, sep="\t", header=False, index=False, na_rep="\\N")
                output.seek(0)
                
                cur.copy_expert(copy_sql, output)
                total_rows += len(chunk)
                
                if batch_idx % 5 == 0:
                    logger.info(f"  [{total_rows:,} lignes]")
            
            conn.commit()
            logger.info(f"✅ {total_rows:,} lignes")
            
            return {
                'rows_loaded': total_rows,
                'table_name': names['table_name'],
                'config_name': names['config_name'],
                'physical_name': names['physical_name'],
                'full_table': full_table
            }
    
    except TimeoutError as e:
        conn.rollback()
        logger.error(f"[TIMEOUT] {e}")
        
        cur.execute("""
            UPDATE sftp_monitoring.sftp_file_log
            SET processing_status = 'TIMEOUT',
                error_message = %s,
                processed_at = NOW()
            WHERE log_id = %s
        """, (str(e), log_id))
        conn.commit()
        raise
        
    except Exception as e:
        conn.rollback()
        logger.error(f"[ERROR] {e}")
        raise
        
    finally:
        cur.close()
        conn.close()


@task(name="[PACKAGE] Archiver")
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


@flow(name="📥 SFTP → RAW (Optimisé)")
def sftp_to_raw_flow():
    logger = get_run_logger()
    files = scan_sftp_directory()

    if not files:
        logger.info("[INFO] Aucun fichier")
        return {"tables_loaded": 0, "total_rows": 0, "tables": [], "table_sizes": {}}

    total_rows = 0
    tables_loaded = []
    table_sizes = {}
    
    for f in files:
        try:
            meta = read_metadata_json(f)
            status = read_status_json(f)
            
            if not meta:
                logger.warning(f"[SKIP] Pas de metadata")
                continue
            
            log_id = log_file_to_monitoring(f, meta, status)
            result = load_to_raw(f, log_id, meta)
            
            if result.get('skipped'):
                logger.warning(f"[SKIP] {result.get('reason')}")
                
                conn = psycopg2.connect(config.get_connection_string())
                cur = conn.cursor()
                cur.execute("""
                    UPDATE sftp_monitoring.sftp_file_log
                    SET processing_status = 'SKIPPED',
                        error_message = %s,
                        processed_at = NOW()
                    WHERE log_id = %s
                """, (result.get('reason'), log_id))
                conn.commit()
                cur.close()
                conn.close()
                
                archive_files(f)
                continue
            
            archive_files(f)
            
            total_rows += result['rows_loaded']
            physical_name = result["physical_name"]
            tables_loaded.append(physical_name)
            
            row_count = status.get('row_count', 0) if status else 0
            if physical_name in table_sizes:
                table_sizes[physical_name] = max(table_sizes[physical_name], row_count)
            else:
                table_sizes[physical_name] = row_count
            
            logger.info(f"[OK] {physical_name}")
            
        except TimeoutError:
            logger.error(f"[TIMEOUT] {f}")
            continue
            
        except Exception as e:
            logger.error(f"[ERROR] {f} : {e}")
            continue

    logger.info(f"[DONE] {len(tables_loaded)} tables, {total_rows:,} lignes")
    
    return {
        "tables_loaded": len(tables_loaded),
        "total_rows": total_rows,
        "tables": tables_loaded,
        "table_sizes": table_sizes
    }


if __name__ == "__main__":
    sftp_to_raw_flow()