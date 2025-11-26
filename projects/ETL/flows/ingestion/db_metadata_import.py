"""
============================================================================
Flow Prefect : Import Metadata Progress/Config → PostgreSQL
============================================================================
Fichier : E:\Prefect\postgresql\ingestion\db_metadata_import.py
Objectif : 
- Lire les fichiers JSON de /Incoming/db_metadata
- Charger dans schema metadata (ProginovTables, ETL_Tables, etc.)
- Archiver dans Processed/YYYY-MM-DD/db_metadata/
============================================================================
"""

import os
import shutil
import json
from pathlib import Path
from datetime import datetime
import psycopg2
from psycopg2.extras import Json, execute_batch
from prefect import flow, task
from prefect.logging import get_run_logger
import sys
import time

sys.path.append(r'E:\Prefect\projects/ETL')
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

@task(name="📂 Scanner metadata Progress")
def scan_db_metadata_directory():
    """Scanner le répertoire pour les fichiers metadata JSON"""
    logger = get_run_logger()
    
    metadata_dir = Path(config.sftp_db_metadata_dir)
    
    files_found = list(metadata_dir.glob('*.json'))
    
    logger.info(f"📊 {len(files_found)} fichier(s) metadata trouvé(s)")
    
    return [str(f) for f in files_found]


@task(name="📥 Charger metadata dans PostgreSQL")
def load_metadata_to_postgres(file_path: str):
    """Charger le fichier metadata JSON dans PostgreSQL"""
    logger = get_run_logger()
    
    file_name = Path(file_path).name
    
    try:
        # Lire le JSON
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        schema = data.get('schema')
        table = data.get('table')
        rows = data.get('data', [])
        
        logger.info(f"📄 {file_name} : {schema}.{table} ({len(rows)} lignes)")
        
        if not rows:
            logger.warning(f"⚠️ Aucune donnée dans {file_name}")
            return 0
        
        # Connexion PostgreSQL
        conn = psycopg2.connect(config.get_connection_string())
        cur = conn.cursor()
        
        # Créer schema metadata si nécessaire
        cur.execute("CREATE SCHEMA IF NOT EXISTS metadata")
        
        # Créer table dynamiquement
        columns = rows[0].keys()
        table_name = f"metadata.{table}"
        
        # Truncate si existe déjà (refresh complet)
        cur.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        # Créer table
        col_defs = []
        for col in columns:
            # Détecter type
            sample_value = rows[0][col]
            if isinstance(sample_value, bool):
                col_type = "BOOLEAN"
            elif isinstance(sample_value, int):
                col_type = "INTEGER"
            elif isinstance(sample_value, float):
                col_type = "NUMERIC"
            else:
                col_type = "TEXT"
            
            col_defs.append(f'"{col}" {col_type}')
        
        create_sql = f"""
            CREATE TABLE {table_name} (
                {', '.join(col_defs)},
                _imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        cur.execute(create_sql)
        
        # Insérer données
        col_names = ', '.join([f'"{col}"' for col in columns])
        placeholders = ', '.join(['%s'] * len(columns))
        
        insert_sql = f"INSERT INTO {table_name} ({col_names}) VALUES ({placeholders})"
        
        data_tuples = [tuple(row[col] for col in columns) for row in rows]
        execute_batch(cur, insert_sql, data_tuples)
        
        conn.commit()
        
        logger.info(f"✅ {len(rows)} lignes chargées dans {table_name}")
        
        cur.close()
        conn.close()
        
        return len(rows)
        
    except Exception as e:
        logger.error(f"❌ Erreur chargement {file_name}: {e}")
        if 'conn' in locals():
            conn.rollback()
            conn.close()
        raise


@task(name="📦 Archiver metadata")
def archive_metadata_file(file_path: str):
    """Archiver le fichier metadata dans Processed/YYYY-MM-DD/db_metadata/"""
    logger = get_run_logger()

    today = datetime.now().strftime('%Y-%m-%d')
    archive_dir = Path(config.sftp_processed_dir) / today / "db_metadata"
    archive_dir.mkdir(parents=True, exist_ok=True)

    file_name = Path(file_path).name
    dest_path = archive_dir / file_name

    if safe_move(file_path, dest_path):
        logger.info(f"📦 Archivé : {file_name}")
    else:
        logger.warning(f"⚠️ Impossible d'archiver (fichier verrouillé) : {file_name}")


@flow(name="🗄️ Import Metadata Progress → PostgreSQL", log_prints=True)
def db_metadata_import_flow():
    """
    Flow d'import des métadonnées Progress/Config
    
    1. Scanner /Incoming/db_metadata
    2. Charger dans schema metadata
    3. Archiver dans Processed/YYYY-MM-DD/db_metadata/
    """
    logger = get_run_logger()
    
    logger.info("=" * 60)
    logger.info("🗄️ Import Metadata Progress")
    logger.info("=" * 60)
    
    total_rows = 0
    
    try:
        # Scanner
        files = scan_db_metadata_directory()
        
        if not files:
            logger.info("ℹ️ Aucun fichier metadata à traiter")
            return
        
        # Traiter chaque fichier
        for file_path in files:
            try:
                rows = load_metadata_to_postgres(file_path)
                total_rows += rows
                
                archive_metadata_file(file_path)
                
            except Exception as e:
                logger.error(f"❌ Erreur {file_path}: {e}")
                continue
        
        logger.info("=" * 60)
        logger.info(f"✅ Import terminé : {total_rows:,} lignes metadata")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"❌ Erreur flow : {e}")
        raise


if __name__ == "__main__":
    db_metadata_import_flow()
