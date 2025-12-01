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
from psycopg2.extras import Json
from prefect import flow, task
from prefect.logging import get_run_logger
from sqlalchemy import create_engine
import sys

sys.path.append(r'E:\Prefect\projects\ETL')
from flows.config.pg_config import config
from flows.utils.file_operations import archive_and_cleanup


@task(name="📂 Scanner SFTP parquet")
def scan_sftp_directory():
    logger = get_run_logger()
    parquet_dir = Path(config.sftp_parquet_dir)
    files = list(parquet_dir.glob("*.parquet"))
    logger.info(f"📊 {len(files)} fichier(s) trouvés")
    return [str(f) for f in files]


@task(name="📄 Lire metadata JSON")
def read_metadata_json(parquet_path: str):
    logger = get_run_logger()
    base = Path(parquet_path).stem
    meta_path = Path(config.sftp_metadata_dir) / f"{base}_metadata.json"

    if not meta_path.exists():
        logger.warning(f"⚠️ Metadata introuvable : {meta_path}")
        return None

    with open(meta_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    logger.info(f"📄 Metadata lu : {meta_path.name}")
    return data


@task(name="📊 Lire status JSON")
def read_status_json(parquet_path: str):
    logger = get_run_logger()
    base = Path(parquet_path).stem
    status_path = Path(config.sftp_status_dir) / f"{base}_status.json"

    if not status_path.exists():
        return None

    with open(status_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data


@task(name="📝 Logger dans sftp_monitoring")
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

    logger.info(f"📝 Log ID {log_id}")
    return log_id


@task(name="📥 Charger dans RAW (DROP + CREATE + COPY)")
def load_to_raw(file_path: str, log_id: int, metadata: dict):
    """
    Charge le fichier parquet dans raw.raw_{table}
    
    IMPORTANT : 
    - DROP TABLE à chaque fois (refresh complet)
    - Colonnes business + _loaded_at, _source_file, _sftp_log_id
    - PAS de _etl_hashdiff (sera ajouté dans STAGING)
    """
    logger = get_run_logger()
    file_name = os.path.basename(file_path)

    # Déterminer nom de table
    if metadata and "table_name" in metadata:
        table_short = metadata["table_name"].lower()
    else:
        table_short = file_name.split("_")[0].lower()

    table_name = f"raw_{table_short}"
    full_table = f"{config.schema_raw}.{table_name}"

    # Lire parquet
    df = pd.read_parquet(file_path)
    
    # Ajouter colonnes ETL (pas de hashdiff ici)
    df["_loaded_at"] = datetime.now()
    df["_source_file"] = file_name
    df["_sftp_log_id"] = log_id

    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()

    # DROP + CREATE
    logger.info(f"🧨 DROP {full_table}")
    cur.execute(f'DROP TABLE IF EXISTS {full_table} CASCADE;')
    conn.commit()

    logger.info(f"🛠️ CREATE {full_table}")
    engine = create_engine(config.get_sqlalchemy_url(config.schema_raw))
    df.head(0).to_sql(
        name=table_name,
        con=engine,
        schema=config.schema_raw,
        if_exists="replace",
        index=False
    )

    # COPY
    logger.info("🚀 COPY données")
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

    logger.info(f"✅ {len(df):,} lignes chargées")
    cur.close()
    conn.close()

    return {
        "rows_loaded": len(df), 
        "table_name": table_short,
        "full_table": full_table
    }


@task(name="📦 Archiver + Nettoyer SFTP")
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
        logger.info("ℹ️ Aucun fichier parquet")
        return {"tables_loaded": 0, "total_rows": 0}

    total_rows = 0
    tables_loaded = []
    
    for f in files:
        try:
            meta = read_metadata_json(f)
            status = read_status_json(f)
            
            if not meta:
                logger.warning(f"⚠️ Skip {f} - pas de metadata")
                continue
            
            table_name = meta.get('table_name', 'unknown')
            logger.info(f"🎯 Traitement de {table_name}")
            
            log_id = log_file_to_monitoring(f, meta, status)
            result = load_to_raw(f, log_id, meta)
            archive_files(f)
            
            total_rows += result['rows_loaded']
            tables_loaded.append(table_name)
            
            logger.info(f"✅ {table_name} : {result['rows_loaded']:,} lignes")
            
        except Exception as e:
            logger.error(f"❌ Erreur {f} : {e}")

    logger.info(f"🎯 TERMINÉ : {len(tables_loaded)} table(s), {total_rows:,} lignes")
    
    return {
        "tables_loaded": len(tables_loaded),
        "total_rows": total_rows,
        "tables": tables_loaded
    }


if __name__ == "__main__":
    sftp_to_raw_flow()