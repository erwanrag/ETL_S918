"""
============================================================================
Flow Prefect : Ingestion SFTP → PostgreSQL (RAW → ODS)
============================================================================
Version PRODUCTION avec :
- Validation enrichie
- Calcul hashdiff dans RAW
- Merge ODS
- Archivage + Nettoyage SFTP
============================================================================
"""

import os
import json
from pathlib import Path
from datetime import datetime
from io import StringIO
import pandas as pd
import psycopg2
from psycopg2.extras import Json
from prefect import flow, task
from prefect.logging import get_run_logger
from sqlalchemy import create_engine
import sys

sys.path.append(r'E:\Prefect\projects\ETL')
from flows.config.pg_config import config
from flows.utils.file_operations import archive_and_cleanup

# Import des tasks métier
from tasks.validation_tasks import validate_file_complete
from tasks.ods_tasks import merge_ods_auto, verify_ods_after_merge


# ============================================================================
# TASKS INGESTION
# ============================================================================

@task(name="📂 Scanner répertoire SFTP", retries=2)
def scan_sftp_directory():
    logger = get_run_logger()
    parquet_dir = Path(config.sftp_parquet_dir)
    files = list(parquet_dir.glob("*.parquet"))
    logger.info(f"📊 {len(files)} fichier(s) trouvés dans {parquet_dir}")
    return [str(f) for f in files]


@task(name="📄 Lire metadata JSON")
def read_metadata_json(parquet_path: str):
    logger = get_run_logger()
    base = Path(parquet_path).stem
    meta_path = Path(config.sftp_metadata_dir) / f"{base}_metadata.json"

    if not meta_path.exists():
        logger.warning(f"⚠️ Metadata introuvable : {meta_path}")
        return None

    try:
        with open(meta_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        logger.info(f"📄 Metadata lu : {meta_path.name}")
        return data
    except Exception as e:
        logger.warning(f"⚠️ Erreur lecture metadata : {e}")
        return None


@task(name="📊 Lire status JSON")
def read_status_json(parquet_path: str):
    logger = get_run_logger()
    base = Path(parquet_path).stem
    status_path = Path(config.sftp_status_dir) / f"{base}_status.json"

    if not status_path.exists():
        logger.warning(f"⚠️ Status introuvable : {status_path}")
        return None

    try:
        with open(status_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        logger.info(f"📊 Status lu : {status_path.name}")
        return data
    except Exception as e:
        logger.warning(f"⚠️ Erreur lecture status : {e}")
        return None


@task(name="📝 Logger fichier dans sftp_monitoring")
def log_file_to_monitoring(file_path: str, metadata: dict, status: dict):
    logger = get_run_logger()
    file_name = os.path.basename(file_path)
    file_size = os.path.getsize(file_path)

    full_meta = {
        "file_extension": ".parquet",
        "detected_timestamp": datetime.now().isoformat(),
        "metadata": metadata or {},
        "status": status or {}
    }

    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    cur.execute("""
        SELECT sftp_monitoring.log_new_file(%s, %s, %s, %s, %s)
    """, (file_name, file_path, file_size, "CBM", Json(full_meta)))

    log_id = cur.fetchone()[0]
    conn.commit()
    cur.close()
    conn.close()

    logger.info(f"📝 Log ID {log_id} créé pour {file_name}")
    return log_id


@task(name="📥 Charger fichier dans RAW (DROP + CREATE + COPY)", retries=1)
def load_file_to_raw_optimized(file_path: str, log_id: int, metadata: dict):
    logger = get_run_logger()
    file_name = os.path.basename(file_path)

    # Déterminer nom de table
    if metadata and "table_name" in metadata:
        table_short = metadata["table_name"].lower()
    else:
        table_short = file_name.split("_")[0].lower()

    table_name = f"raw_{table_short}"
    full_table = f"{config.schema_raw}.{table_name}"

    # Lire parquet + colonnes ETL
    df = pd.read_parquet(file_path)
    df["_loaded_at"] = datetime.now()
    df["_source_file"] = file_name
    df["_sftp_log_id"] = log_id

    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()

    # DROP + CREATE
    logger.info(f"🧨 DROP TABLE si existe : {full_table}")
    cur.execute(f'DROP TABLE IF EXISTS {full_table} CASCADE;')
    conn.commit()

    logger.info(f"🛠️ CREATE TABLE {full_table}")
    engine = create_engine(config.get_sqlalchemy_url(config.schema_raw))
    df.head(0).to_sql(
        name=table_name,
        con=engine,
        schema=config.schema_raw,
        if_exists="replace",
        index=False
    )

    # COPY données
    logger.info("🚀 COPY PostgreSQL…")
    output = StringIO()
    df.to_csv(output, sep="\t", header=False, index=False, na_rep="\\N")
    output.seek(0)

    col_list = ",".join([f'"{c}"' for c in df.columns])
    copy_sql = f"""
    COPY {full_table} ({col_list})
    FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', NULL '\\N');
    """

    cur.copy_expert(copy_sql, output)
    conn.commit()

    logger.info(f"✅ {len(df):,} lignes chargées dans {full_table}")
    cur.close()
    conn.close()

    return {"rows_loaded": len(df), "table_name": table_short}


@task(name="📝 Logger étape dans load_step_log")
def log_step(run_id: str, file_log_id: int, table_name: str, step_name: str, 
             status: str, rows_processed: int = None, duration_seconds: float = None, 
             error_message: str = None, step_metadata: dict = None):
    """Logger une étape dans etl_logs.load_step_log"""
    logger = get_run_logger()
    
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    try:
        cur.execute("""
            INSERT INTO etl_logs.load_step_log 
                (run_id, file_log_id, table_name, step_name, status, 
                 rows_processed, duration_seconds, error_message, step_metadata)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (run_id, file_log_id, table_name, step_name, status, 
              rows_processed, duration_seconds, error_message, 
              Json(step_metadata) if step_metadata else None))
        
        conn.commit()
        logger.info(f"📝 Step log: {step_name} - {status}")
        
    finally:
        cur.close()
        conn.close()


@task(name="📦 Archiver + Nettoyer SFTP")
def archive_files(parquet_path: str):
    """
    Archivage complet :
    1. MOVE E:\SFTP_Mirroring\Incoming → Processed
    2. DELETE C:\ProgramData\ssh\SFTPRoot\Incoming
    """
    logger = get_run_logger()
    base = Path(parquet_path).stem
    
    # Construire chemins Incoming
    incoming_paths = {
        'parquet': Path(parquet_path),
        'metadata': Path(config.sftp_metadata_dir) / f"{base}_metadata.json",
        'status': Path(config.sftp_status_dir) / f"{base}_status.json"
    }
    
    # Construire chemins serveur SFTP
    sftp_root = Path(r"C:\ProgramData\ssh\SFTPRoot\Incoming\data")
    sftp_server_paths = {
        'parquet': sftp_root / "parquet" / f"{base}.parquet",
        'metadata': sftp_root / "metadata" / f"{base}_metadata.json",
        'status': sftp_root / "status" / f"{base}_status.json"
    }
    
    # Appel fonction mutualisée
    archive_and_cleanup(
        base_filename=base,
        archive_root=config.sftp_processed_dir,
        incoming_paths=incoming_paths,
        sftp_server_paths=sftp_server_paths,
        logger=logger
    )


# ============================================================================
# FLOW PRINCIPAL
# ============================================================================

@flow(name="🔄 Pipeline Complet : SFTP → RAW → ODS")
def sftp_to_ods_flow():
    """
    Pipeline PRODUCTION complet :
    1. Scan SFTP
    2. Validation
    3. Load RAW
    4. Hashdiff dans RAW
    5. Merge ODS
    6. Archivage + Nettoyage
    """
    logger = get_run_logger()

    files = scan_sftp_directory()

    if not files:
        logger.info("ℹ️ Aucun fichier parquet.")
        return

    total_rows = 0
    total_tables = 0
    
    for f in files:
        start_time = datetime.now()
        
        try:
            # 1. Lire métadonnées
            meta = read_metadata_json(f)
            status = read_status_json(f)
            
            if not meta:
                logger.warning(f"⚠️ Skip {f} - pas de metadata")
                continue
            
            table_name = meta.get('table_name', 'unknown')
            run_id = meta.get('run_id', str(datetime.now().timestamp()))
            load_mode = meta.get('load_mode', 'INCREMENTAL')
            
            logger.info(f"🎯 Traitement de {table_name} (mode {load_mode})")
            
            # 2. Logger fichier
            log_id = log_file_to_monitoring(f, meta, status)
            
            # 3. VALIDATION
            base = Path(f).stem
            meta_path = Path(config.sftp_metadata_dir) / f"{base}_metadata.json"
            
            validation_start = datetime.now()
            validation = validate_file_complete(f, str(meta_path), table_name)
            validation_duration = (datetime.now() - validation_start).total_seconds()
            
            log_step(run_id, log_id, table_name, 'validate', 
                    'success' if validation['validation_passed'] else 'failed',
                    duration_seconds=validation_duration,
                    step_metadata=validation)
            
            if not validation['validation_passed']:
                logger.error(f"❌ Validation échouée pour {table_name}")
                continue
            
            # 4. LOAD RAW
            raw_start = datetime.now()
            result = load_file_to_raw_optimized(f, log_id, meta)
            raw_duration = (datetime.now() - raw_start).total_seconds()
            
            log_step(run_id, log_id, table_name, 'load_raw', 'success',
                    rows_processed=result['rows_loaded'],
                    duration_seconds=raw_duration)
            
            # 5. CALCUL HASHDIFF dans RAW
            staging_start = datetime.now()
            
            from utils.hashdiff import execute_hashdiff_update
            execute_hashdiff_update(table_name.lower(), 'raw')  # ✅ CORRIGÉ
            
            staging_duration = (datetime.now() - staging_start).total_seconds()
            
            log_step(run_id, log_id, table_name, 'hashdiff', 'success',
                    rows_processed=result['rows_loaded'],
                    duration_seconds=staging_duration)
            
            # 6. MERGE ODS
            ods_start = datetime.now()
            ods_result = merge_ods_auto(table_name, run_id, load_mode)
            ods_duration = (datetime.now() - ods_start).total_seconds()
            
            log_step(run_id, log_id, table_name, 'merge_ods', 'success',
                    rows_processed=ods_result.get('rows_affected', ods_result.get('rows_inserted', 0)),
                    duration_seconds=ods_duration,
                    step_metadata=ods_result)
            
            # 7. Vérification ODS
            verify_ods_after_merge(table_name, run_id)
            
            # 8. Archivage + Nettoyage
            archive_files(f)
            
            # Stats
            total_duration = (datetime.now() - start_time).total_seconds()
            total_rows += result['rows_loaded']
            total_tables += 1
            
            logger.info(f"✅ {table_name} traité en {total_duration:.2f}s")
            
        except Exception as e:
            logger.error(f"❌ Erreur fichier : {f} — {e}")
            
            if 'log_id' in locals() and 'table_name' in locals():
                log_step(run_id, log_id, table_name, 'error', 'failed',
                        error_message=str(e))

    logger.info(f"🎯 PIPELINE TERMINÉ : {total_tables} table(s), {total_rows:,} lignes")


if __name__ == "__main__":
    sftp_to_ods_flow()