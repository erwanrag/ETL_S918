"""
============================================================================
Flow Prefect : Ingestion SFTP → PostgreSQL (RAW) - OPTIMISÉ + AUTO-DROP/CREATE
============================================================================
"""

import os
import shutil
import json
import time
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


def safe_move(src: str, dst: str, retries: int = 30, delay: float = 1.0):
    """Move ultra-robuste Windows/SFTP avec fallback COPY+DELETE"""
    for i in range(retries):
        try:
            shutil.move(src, dst)
            return True
        except PermissionError:
            if i == retries - 1:
                # Fallback: COPY puis DELETE
                try:
                    shutil.copy2(src, dst)
                    time.sleep(2)
                    os.remove(src)
                    return True
                except:
                    return False
            time.sleep(delay)
        except FileNotFoundError:
            return False
    return False


# ============================================================================
# TASKS
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

    # ✅ Sécurisation si metadata est None
    if metadata and "table_name" in metadata:
        table_short = metadata["table_name"].lower()
    else:
        table_short = file_name.split("_")[0].lower()

    table_name = f"raw_{table_short}"
    full_table = f"{config.schema_raw}.{table_name}"

    # Lire le parquet
    df = pd.read_parquet(file_path)
    df["_loaded_at"] = datetime.now()
    df["_source_file"] = file_name
    df["_sftp_log_id"] = log_id

    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()

    # 🔥 AUTO-DROP TABLE RAW (toujours propre et à jour)
    logger.info(f"🧨 DROP TABLE si existe : {full_table}")
    cur.execute(f'DROP TABLE IF EXISTS {full_table} CASCADE;')
    conn.commit()

    # 🔧 Création via pandas
    logger.info(f"🛠️ CREATE TABLE {full_table} via pandas.to_sql()")
    engine = create_engine(config.get_sqlalchemy_url(config.schema_raw))
    df.head(0).to_sql(
        name=table_name,
        con=engine,
        schema=config.schema_raw,
        if_exists="replace",
        index=False
    )

    # 🚀 COPY data
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

    return {"rows_loaded": len(df)}


@task(name="📦 Archiver fichiers SFTP")
def archive_files(parquet_path: str):
    logger = get_run_logger()
    today = datetime.now().strftime("%Y-%m-%d")
    archive_dir = Path(config.sftp_processed_dir) / today / "data"
    archive_dir.mkdir(parents=True, exist_ok=True)

    base = Path(parquet_path).stem

    files = [
        (parquet_path, archive_dir / f"{base}.parquet"),
        (Path(config.sftp_metadata_dir) / f"{base}_metadata.json",
         archive_dir / f"{base}_metadata.json"),
        (Path(config.sftp_status_dir) / f"{base}_status.json",
         archive_dir / f"{base}_status.json")
    ]

    for src, dst in files:
        src = Path(src)
        if src.exists():
            ok = safe_move(str(src), str(dst))
            if ok:
                logger.info(f"📦 Archivé : {dst.name}")
            else:
                logger.warning(f"⚠️ Impossible d'archiver (fichier verrouillé) : {src}")


# ============================================================================
# FLOW PRINCIPAL
# ============================================================================

@flow(name="🔄 Ingestion SFTP → PostgreSQL RAW (OPTIMISÉ)")
def sftp_to_raw_flow():
    logger = get_run_logger()

    files = scan_sftp_directory()

    if not files:
        logger.info("ℹ️ Aucun fichier parquet.")
        return

    total = 0
    for f in files:
        try:
            meta = read_metadata_json(f)
            status = read_status_json(f)
            log_id = log_file_to_monitoring(f, meta, status)
            result = load_file_to_raw_optimized(f, log_id, meta)
            total += result["rows_loaded"]
            archive_files(f)
        except Exception as e:
            logger.error(f"❌ Erreur fichier : {f} — {e}")

    logger.info(f"🎯 TOTAL : {total:,} lignes chargées")


if __name__ == "__main__":
    sftp_to_raw_flow()
