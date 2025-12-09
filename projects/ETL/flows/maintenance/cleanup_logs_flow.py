"""
============================================================================
Flow Prefect : Nettoyage Logs PostgreSQL
============================================================================
Responsabilité : Supprimer les anciennes entrées dans etl_logs.* et sftp_monitoring.*
                 pour éviter la croissance infinie des tables de logs
============================================================================
"""

from datetime import datetime, timedelta
from prefect import flow, task
from prefect.logging import get_run_logger
import psycopg2
import sys

sys.path.append(r'E:\Prefect\projects\ETL')
from flows.config.pg_config import config


@task(name="🧹 Nettoyer etl_logs")
def cleanup_etl_logs(retention_days: int = 30):
    """
    Supprimer les logs ETL > retention_days
    
    Tables concernées :
    - etl_logs.flow_runs (ou équivalent)
    - etl_logs.task_logs (si existe)
    """
    logger = get_run_logger()
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    cutoff_date = datetime.now() - timedelta(days=retention_days)
    logger.info(f"🧹 Nettoyage logs < {cutoff_date.strftime('%Y-%m-%d')}")
    
    try:
        # Vérifier quelles TABLES (pas les vues) existent dans etl_logs
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'etl_logs'
              AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """)
        tables = [row[0] for row in cur.fetchall()]
        
        if not tables:
            logger.warning("[WARN] Aucune table dans etl_logs")
            return {"tables_cleaned": 0, "rows_deleted": 0}
        
        logger.info(f"[DATA] Tables trouvées : {', '.join(tables)}")
        
        total_deleted = 0
        tables_cleaned = 0
        
        for table in tables:
            # Chercher colonne timestamp (peut varier : created_at, timestamp, run_date...)
            cur.execute(f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = 'etl_logs' 
                  AND table_name = %s
                  AND data_type IN ('timestamp', 'timestamp without time zone', 'timestamp with time zone')
                LIMIT 1
            """, (table,))
            
            result = cur.fetchone()
            if not result:
                logger.warning(f"[SKIP] {table} - Pas de colonne timestamp")
                continue
            
            time_col = result[0]
            logger.info(f"[TARGET] {table}.{time_col}")
            
            # DELETE
            delete_sql = f"""
                DELETE FROM etl_logs.{table}
                WHERE "{time_col}" < %s
            """
            cur.execute(delete_sql, (cutoff_date,))
            deleted = cur.rowcount
            
            conn.commit()
            total_deleted += deleted
            tables_cleaned += 1
            
            logger.info(f"[OK] {table} : {deleted:,} lignes supprimées")
        
        logger.info(f"[OK] Total : {total_deleted:,} lignes supprimées sur {tables_cleaned} table(s)")
        
        return {
            "tables_cleaned": tables_cleaned,
            "rows_deleted": total_deleted,
            "cutoff_date": cutoff_date.isoformat()
        }
        
    except Exception as e:
        logger.error(f"[ERROR] Erreur cleanup etl_logs : {e}")
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


@task(name="🧹 Nettoyer sftp_monitoring")
def cleanup_sftp_monitoring(retention_days: int = 90):
    """
    Supprimer les logs SFTP > retention_days
    
    Table : sftp_monitoring.sftp_file_log
    """
    logger = get_run_logger()
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    cutoff_date = datetime.now() - timedelta(days=retention_days)
    logger.info(f"🧹 Nettoyage SFTP logs < {cutoff_date.strftime('%Y-%m-%d')}")
    
    try:
        # Vérifier existence table
        cur.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'sftp_monitoring'
                  AND table_name = 'sftp_file_log'
            )
        """)
        
        if not cur.fetchone()[0]:
            logger.warning("[WARN] Table sftp_monitoring.sftp_file_log inexistante")
            return {"rows_deleted": 0}
        
        # DELETE (sur detected_at ou équivalent)
        cur.execute("""
            DELETE FROM sftp_monitoring.sftp_file_log
            WHERE detected_at < %s
        """, (cutoff_date,))
        
        deleted = cur.rowcount
        conn.commit()
        
        logger.info(f"[OK] {deleted:,} entrées SFTP supprimées")
        
        return {
            "rows_deleted": deleted,
            "cutoff_date": cutoff_date.isoformat()
        }
        
    except Exception as e:
        logger.error(f"[ERROR] Erreur cleanup SFTP : {e}")
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


@task(name="📊 Statistiques logs")
def get_logs_stats():
    """Récupère stats sur taille des tables de logs"""
    logger = get_run_logger()
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    try:
        stats = {}
        
        # Stats etl_logs et sftp_monitoring
        cur.execute("""
            SELECT 
                schemaname || '.' || relname AS table_name,
                n_live_tup AS row_count,
                pg_size_pretty(pg_total_relation_size(schemaname||'.'||relname)) AS total_size
            FROM pg_stat_user_tables
            WHERE schemaname IN ('etl_logs', 'sftp_monitoring')
            ORDER BY schemaname, relname
        """)
        
        for table, rows, size in cur.fetchall():
            stats[table] = {
                'rows': rows,
                'size': size
            }
            logger.info(f"📊 {table} : {rows:,} lignes, {size}")
        
        return stats
        
    except Exception as e:
        logger.error(f"[ERROR] Erreur stats : {e}")
        return {}
    finally:
        cur.close()
        conn.close()


@flow(name="🧹 Cleanup Logs PostgreSQL", log_prints=True)
def cleanup_logs_flow(
    etl_retention_days: int = 30,
    sftp_retention_days: int = 90,
    show_stats: bool = True
):
    """
    Flow principal de nettoyage des logs
    
    Args:
        etl_retention_days: Rétention logs ETL (défaut: 30j)
        sftp_retention_days: Rétention logs SFTP (défaut: 90j)
        show_stats: Afficher stats avant/après (défaut: True)
    """
    logger = get_run_logger()
    
    logger.info("=" * 70)
    logger.info("🧹 NETTOYAGE LOGS POSTGRESQL")
    logger.info("=" * 70)
    
    # Stats AVANT
    if show_stats:
        logger.info("\n📊 STATISTIQUES AVANT NETTOYAGE")
        get_logs_stats()
    
    # Nettoyage
    logger.info(f"\n🧹 Nettoyage etl_logs (> {etl_retention_days}j)")
    etl_result = cleanup_etl_logs(retention_days=etl_retention_days)
    
    logger.info(f"\n🧹 Nettoyage sftp_monitoring (> {sftp_retention_days}j)")
    sftp_result = cleanup_sftp_monitoring(retention_days=sftp_retention_days)
    
    # Stats APRÈS
    if show_stats:
        logger.info("\n📊 STATISTIQUES APRÈS NETTOYAGE")
        get_logs_stats()
    
    # Résumé
    logger.info("\n" + "=" * 70)
    logger.info("✅ NETTOYAGE TERMINÉ")
    logger.info("=" * 70)
    logger.info(f"📥 ETL : {etl_result['rows_deleted']:,} lignes supprimées")
    logger.info(f"📥 SFTP : {sftp_result['rows_deleted']:,} lignes supprimées")
    logger.info("=" * 70 + "\n")
    
    return {
        "etl_logs": etl_result,
        "sftp_monitoring": sftp_result
    }


if __name__ == "__main__":
    # Test du flow
    cleanup_logs_flow(
        etl_retention_days=30,
        sftp_retention_days=90,
        show_stats=True
    )