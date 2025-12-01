"""
============================================================================
Flow Prefect : RAW → STAGING_ETL
============================================================================
Responsabilité : Copier RAW → STAGING avec enrichissement
- Calcul _etl_hashdiff
- Ajout _etl_valid_from
- Déduplication si nécessaire
============================================================================
"""

import psycopg2
from prefect import flow, task
from prefect.logging import get_run_logger
from typing import Optional, List
import sys

sys.path.append(r'E:\Prefect\projects\ETL')
from flows.config.pg_config import config


@task(name="📋 Copier RAW → STAGING + Hashdiff")
def copy_raw_to_staging(table_name: str, run_id: str):
    """
    Copie raw.raw_{table} → staging_etl.stg_{table}
    + Calcul _etl_hashdiff (gère +100 colonnes via concaténation par blocs)
    + Ajout _etl_valid_from, _etl_run_id
    """
    logger = get_run_logger()
    
    raw_table = f"raw.raw_{table_name.lower()}"
    stg_table = f"staging_etl.stg_{table_name.lower()}"
    
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    try:
        # Vérifier que RAW existe
        cur.execute(f"""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_schema = 'raw' 
                AND table_name = 'raw_{table_name.lower()}'
            )
        """)
        
        if not cur.fetchone()[0]:
            logger.error(f"❌ Table {raw_table} introuvable")
            return {"rows_copied": 0, "error": "Table RAW inexistante"}
        
        # DROP staging si existe
        logger.info(f"🧨 DROP {stg_table} si existe")
        cur.execute(f"DROP TABLE IF EXISTS {stg_table} CASCADE")
        conn.commit()
        
        # Récupérer TOUTES les colonnes sauf les 3 colonnes ETL
        cur.execute(f"""
            SELECT column_name, data_type
            FROM information_schema.columns 
            WHERE table_schema = 'raw' 
              AND table_name = 'raw_{table_name.lower()}'
              AND column_name NOT IN ('_loaded_at', '_source_file', '_sftp_log_id')
            ORDER BY ordinal_position
        """)
        
        columns_info = cur.fetchall()
        
        if not columns_info:
            logger.error(f"❌ Aucune colonne business dans {raw_table}")
            return {"rows_copied": 0, "error": "Pas de colonnes business"}
        
        business_cols = [col[0] for col in columns_info]
        
        logger.info(f"📊 {len(business_cols)} colonnes business détectées")
        
        # Guillemets doubles pour tous les noms de colonnes
        business_cols_quoted = ', '.join([f'"{col}"' for col in business_cols])
        
        # ✅ SOLUTION : Concaténation par groupes de 90 colonnes (marge de sécurité)
        CHUNK_SIZE = 90
        hash_chunks = []
        
        for i in range(0, len(business_cols), CHUNK_SIZE):
            chunk = business_cols[i:i + CHUNK_SIZE]
            chunk_cols = ', '.join([
                f"COALESCE(CAST(\"{col}\" AS TEXT), '')" 
                for col in chunk
            ])
            hash_chunks.append(f"CONCAT_WS('|', {chunk_cols})")
        
        # Si un seul chunk, pas besoin de concaténer
        if len(hash_chunks) == 1:
            hash_expression = f"MD5({hash_chunks[0]})"
        else:
            # Concaténer tous les chunks
            all_chunks = ' || \'|\' || '.join(hash_chunks)
            hash_expression = f"MD5({all_chunks})"
        
        logger.info(f"📋 CREATE {stg_table} avec hashdiff ({len(hash_chunks)} bloc(s))")
        
        # CREATE TABLE avec hashdiff
        create_sql = f"""
            CREATE TABLE {stg_table} AS
            SELECT 
                {business_cols_quoted},
                {hash_expression} AS _etl_hashdiff,
                CURRENT_TIMESTAMP AS _etl_valid_from,
                '{run_id}' AS _etl_run_id
            FROM {raw_table}
        """
        
        cur.execute(create_sql)
        conn.commit()
        
        # Compter lignes
        cur.execute(f"SELECT COUNT(*) FROM {stg_table}")
        row_count = cur.fetchone()[0]
        
        logger.info(f"✅ {row_count:,} lignes copiées dans {stg_table}")
        
        return {
            "rows_copied": row_count,
            "source_table": raw_table,
            "target_table": stg_table,
            "columns_count": len(business_cols),
            "hash_chunks": len(hash_chunks)
        }
        
    except Exception as e:
        logger.error(f"❌ Erreur : {e}")
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

@task(name="📊 Lister tables RAW")
def list_raw_tables():
    """Liste toutes les tables raw.raw_*"""
    logger = get_run_logger()
    
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    cur.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'raw' 
          AND table_name LIKE 'raw_%'
        ORDER BY table_name
    """)
    
    tables = [row[0].replace('raw_', '') for row in cur.fetchall()]
    
    cur.close()
    conn.close()
    
    logger.info(f"📊 {len(tables)} table(s) RAW trouvée(s)")
    return tables


@flow(name="📋 RAW → STAGING_ETL (avec hashdiff)")
def raw_to_staging_flow(
    table_names: Optional[List[str]] = None,  # ✅ Type explicite
    run_id: Optional[str] = None              # ✅ Type explicite
):
    """
    Flow de copie RAW → STAGING avec enrichissement
    
    Args:
        table_names: Liste des tables à traiter (None = toutes)
        run_id: ID du run (auto-généré si None)
    
    Étapes :
    1. Lister tables RAW (si table_names=None)
    2. Pour chaque table : copier + hashdiff
    """
    logger = get_run_logger()
    
    if run_id is None:
        from datetime import datetime
        run_id = f"raw_to_staging_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    # Déterminer tables à traiter
    if table_names is None:
        tables = list_raw_tables()
    else:
        tables = table_names
    
    if not tables:
        logger.info("ℹ️ Aucune table à traiter")
        return {"tables_processed": 0}
    
    total_rows = 0
    tables_processed = []
    
    for table in tables:
        try:
            logger.info(f"🎯 Traitement de {table}")
            result = copy_raw_to_staging(table, run_id)
            
            total_rows += result['rows_copied']
            tables_processed.append(table)
            
            logger.info(f"✅ {table} : {result['rows_copied']:,} lignes")
            
        except Exception as e:
            logger.error(f"❌ Erreur {table} : {e}")
    
    logger.info(f"🎯 TERMINÉ : {len(tables_processed)} table(s), {total_rows:,} lignes")
    
    return {
        "tables_processed": len(tables_processed),
        "total_rows": total_rows,
        "tables": tables_processed,
        "run_id": run_id
    }


if __name__ == "__main__":
    raw_to_staging_flow()