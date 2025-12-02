import psycopg2
from prefect import task
from prefect.logging import get_run_logger
import sys

sys.path.append(r'E:\Prefect\projects\ETL')
from flows.config.pg_config import config
from flows.config.table_metadata import (
    get_primary_keys, 
    has_primary_key, 
    should_force_full,
    get_optimal_load_mode,
    get_table_description
)

from utils.extent_handler import (
    has_extent_columns,
    build_ods_select_with_extent,
    get_extent_mapping
)


def get_load_mode_from_monitoring(table_name: str) -> str:
    """
    Récupérer le load_mode du dernier fichier traité pour cette table
    
    Args:
        table_name: Nom de la table (ex: 'client', 'produit')
    
    Returns:
        str: 'INCREMENTAL', 'FULL', 'FULL_RESET', ou 'AUTO'
    
    Exemples:
        >>> get_load_mode_from_monitoring('client')
        'INCREMENTAL'
        
        >>> get_load_mode_from_monitoring('stock')
        'FULL'
    """
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    try:
        # Récupérer le load_mode du fichier le plus récent
        cur.execute("""
            SELECT load_mode
            FROM sftp_monitoring.sftp_file_log
            WHERE table_name = %s
              AND processing_status IN ('PENDING', 'COMPLETED')
              AND load_mode IS NOT NULL
              AND load_mode != ''
            ORDER BY detected_at DESC
            LIMIT 1
        """, (table_name,))
        
        result = cur.fetchone()
        
        if result and result[0]:
            return result[0]
        else:
            # Fallback : détection automatique
            return "AUTO"
            
    except Exception as e:
        print(f"⚠️ Erreur lecture load_mode pour {table_name}: {e}")
        return "AUTO"
    finally:
        cur.close()
        conn.close()


@task(name="💾 Merge ODS (FULL)", retries=1)
def merge_ods_full(table_name: str, run_id: str):
    logger = get_run_logger()
    logger.info(f"💾 Merge ODS FULL pour {table_name}")
    
    src_table = f"staging_etl.stg_{table_name.lower()}"
    ods_table = f"ods.{table_name.lower()}"
    
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    try:
        # Compter source
        cur.execute(f"SELECT COUNT(*) FROM {src_table}")
        src_count = cur.fetchone()[0]
        logger.info(f"📊 Source : {src_count:,} lignes")
        
        # Vérifier si ODS existe
        cur.execute(f"""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_schema = 'ods' 
                AND table_name = '{table_name.lower()}'
            )
        """)
        
        ods_exists = cur.fetchone()[0]
        
        # Récupérer colonnes staging
        cur.execute(f"""
            SELECT column_name FROM information_schema.columns 
            WHERE table_schema = 'staging_etl' 
              AND table_name = 'stg_{table_name.lower()}'
              AND column_name NOT IN ('_etl_run_id')
            ORDER BY ordinal_position
        """)
        
        staging_columns = [row[0] for row in cur.fetchall()]
        
        # ✅ NOUVEAU : Vérifier si la table a des colonnes extent
        if has_extent_columns(table_name):
            logger.info(f"🔀 Table avec colonnes EXTENT détectée")
            
            # Construire SELECT avec éclatement extent
            select_clause, ods_columns = build_ods_select_with_extent(
                table_name, 
                staging_columns
            )
            
            logger.info(f"📊 {len(staging_columns)} colonnes staging → {len(ods_columns)} colonnes ODS")
            
            # DROP et CREATE ODS avec structure éclatée
            if ods_exists:
                logger.info(f"🧨 DROP {ods_table}")
                cur.execute(f"DROP TABLE {ods_table} CASCADE")
                conn.commit()
            
            logger.info(f"🛠️ CREATE {ods_table} avec extent éclaté")
            create_sql = f"""
                CREATE TABLE {ods_table} AS
                SELECT 
                    {select_clause}
                FROM {src_table}
            """
            cur.execute(create_sql)
            conn.commit()
            
        else:
            # Pas de colonnes extent, traitement normal
            logger.info(f"📋 Table sans colonnes EXTENT")
            
            if not ods_exists:
                logger.info(f"🆕 Création de {ods_table}")
                cur.execute(f"CREATE TABLE {ods_table} AS SELECT * FROM {src_table} WHERE 1=0")
                conn.commit()
            
            logger.info(f"🧨 TRUNCATE {ods_table}")
            cur.execute(f"TRUNCATE TABLE {ods_table}")
            conn.commit()
            
            columns = [col for col in staging_columns if col != '_etl_run_id']
            columns_str = ', '.join([f'"{col}"' for col in columns])
            
            cur.execute(f"INSERT INTO {ods_table} ({columns_str}) SELECT {columns_str} FROM {src_table}")
            conn.commit()
        
        # Compter résultat
        cur.execute(f"SELECT COUNT(*) FROM {ods_table}")
        inserted_count = cur.fetchone()[0]
        
        logger.info(f"✅ {inserted_count:,} lignes insérées")
        
        return {
            'rows_inserted': inserted_count,
            'mode': 'FULL',
            'table': ods_table,
            'run_id': run_id,
            'extent_expanded': has_extent_columns(table_name)
        }
        
    except Exception as e:
        logger.error(f"❌ Erreur : {e}")
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


@task(name="💾 Merge ODS (INCREMENTAL)", retries=1)
def merge_ods_incremental(table_name: str, run_id: str):
    logger = get_run_logger()
    logger.info(f"💾 Merge ODS INCREMENTAL pour {table_name}")
    
    pk_columns = get_primary_keys(table_name)
    
    if not pk_columns:
        raise ValueError(f"Aucune PK pour {table_name}")
    
    logger.info(f"🔑 PK : {', '.join(pk_columns)}")
    
    src_table = f"staging_etl.stg_{table_name.lower()}"
    ods_table = f"ods.{table_name.lower()}"
    
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    try:
        # Vérifier si ODS existe
        cur.execute(f"""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_schema = 'ods' 
                AND table_name = '{table_name.lower()}'
            )
        """)
        
        ods_exists = cur.fetchone()[0]
        
        # Récupérer colonnes staging
        cur.execute(f"""
            SELECT column_name FROM information_schema.columns 
            WHERE table_schema = 'staging_etl' 
              AND table_name = 'stg_{table_name.lower()}'
              AND column_name NOT IN ('_etl_run_id')
            ORDER BY ordinal_position
        """)
        
        staging_columns = [row[0] for row in cur.fetchall()]
        
        # ✅ NOUVEAU : Gérer extent
        if has_extent_columns(table_name):
            logger.info(f"🔀 Table avec colonnes EXTENT")
            
            if not ods_exists:
                # Première création : éclater extent
                logger.info(f"🆕 Création initiale {ods_table} avec extent éclaté")
                
                select_clause, ods_columns = build_ods_select_with_extent(
                    table_name,
                    staging_columns
                )
                
                create_sql = f"""
                    CREATE TABLE {ods_table} AS
                    SELECT {select_clause}
                    FROM {src_table}
                    WHERE 1=0
                """
                cur.execute(create_sql)
                conn.commit()
            
            # Pour INCREMENTAL avec extent, on fait FULL car impossible de merger
            # colonnes éclatées facilement
            logger.warning(f"⚠️ INCREMENTAL avec extent non supporté → Fallback FULL")
            return merge_ods_full(table_name, run_id)
        
        else:
            # Pas de extent, traitement INCREMENTAL normal
            logger.info(f"📋 INCREMENTAL standard (sans extent)")
            
            if not ods_exists:
                logger.info(f"🆕 Création de {ods_table}")
                cur.execute(f"CREATE TABLE {ods_table} AS SELECT * FROM {src_table} WHERE 1=0")
                conn.commit()
            
            columns = [col for col in staging_columns if col != '_etl_run_id']
            columns_str = ', '.join([f'"{col}"' for col in columns])
            
            pk_join = ' AND '.join([f'target."{pk}" = source."{pk}"' for pk in pk_columns])
            
            # INSERT nouveaux
            logger.info("🔄 INSERT nouveaux")
            cur.execute(f"""
                INSERT INTO {ods_table} ({columns_str})
                SELECT {columns_str} FROM {src_table} AS source
                WHERE NOT EXISTS (
                    SELECT 1 FROM {ods_table} AS target
                    WHERE {pk_join}
                )
            """)
            inserted_count = cur.rowcount
            conn.commit()
            logger.info(f"➕ {inserted_count:,} lignes insérées")
            
            # UPDATE modifiés (si hashdiff présent)
            if '_etl_hashdiff' in columns:
                update_cols = [
                    col for col in columns 
                    if col not in pk_columns 
                    and col not in ['_etl_hashdiff', '_etl_valid_from']
                ]
                
                if update_cols:
                    set_clause = ', '.join([f'"{col}" = source."{col}"' for col in update_cols])
                    full_set = f'{set_clause}, "_etl_hashdiff" = source."_etl_hashdiff", "_etl_valid_from" = source."_etl_valid_from"'
                else:
                    full_set = '"_etl_hashdiff" = source."_etl_hashdiff", "_etl_valid_from" = source."_etl_valid_from"'
                
                cur.execute(f"""
                    UPDATE {ods_table} AS target
                    SET {full_set}
                    FROM {src_table} AS source
                    WHERE {pk_join}
                      AND target."_etl_hashdiff" != source."_etl_hashdiff"
                """)
                updated_count = cur.rowcount
                conn.commit()
                logger.info(f"🔄 {updated_count:,} lignes mises à jour")
            else:
                updated_count = 0
            
            total = inserted_count + updated_count
            logger.info(f"✅ {total:,} lignes affectées")
            
            return {
                'rows_inserted': inserted_count,
                'rows_updated': updated_count,
                'rows_affected': total,
                'mode': 'INCREMENTAL',
                'table': ods_table,
                'run_id': run_id,
                'extent_expanded': False
            }
        
    except Exception as e:
        logger.error(f"❌ Erreur : {e}")
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

@task(name="💾 Merge ODS auto")
def merge_ods_auto(table_name: str, run_id: str, load_mode: str = "AUTO"):
    """
    Merger STAGING → ODS avec mode automatique ou forcé
    
    Priorité de détermination du mode :
    1. load_mode passé en paramètre (si != "AUTO")
    2. load_mode depuis sftp_monitoring (dernier fichier)
    3. Détection automatique (PK + timestamps)
    
    Args:
        table_name: Nom de la table (ex: 'client')
        run_id: ID du run ETL
        load_mode: Mode forcé ou "AUTO" pour détection
    
    Returns:
        dict: Résultat du merge avec mode utilisé
    
    Modes disponibles:
        - INCREMENTAL: UPSERT (INSERT nouveaux + UPDATE modifiés)
        - FULL: TRUNCATE + INSERT ALL
        - FULL_RESET: DROP + CREATE + INSERT ALL
        - AUTO: Détection automatique
    """
    logger = get_run_logger()
    
    # ✅ Si mode AUTO, lire depuis sftp_monitoring
    if load_mode == "AUTO":
        load_mode = get_load_mode_from_monitoring(table_name)
        logger.info(f"🤖 Mode depuis sftp_monitoring : {load_mode}")
    else:
        logger.info(f"🎯 Mode forcé : {load_mode}")
    
    # Description table
    table_desc = get_table_description(table_name)
    logger.info(f"📋 {table_desc}")
    
    # ============================================================
    # ROUTER selon load_mode
    # ============================================================
    
    if load_mode == "FULL_RESET":
        logger.info(f"💾 Mode FULL_RESET - Réinitialisation complète")
        return merge_ods_full_reset(table_name, run_id)
        
    elif load_mode == "FULL":
        logger.info(f"💾 Mode FULL - TRUNCATE + INSERT")
        return merge_ods_full(table_name, run_id)
        
    elif load_mode == "INCREMENTAL":
        # Vérifier qu'on a bien une PK
        pk = get_primary_keys(table_name)
        if not pk:
            logger.warning(f"⚠️ Mode INCREMENTAL demandé mais pas de PK → Fallback sur FULL")
            return merge_ods_full(table_name, run_id)
        else:
            logger.info(f"💾 Mode INCREMENTAL - UPSERT (PK: {pk})")
            return merge_ods_incremental(table_name, run_id)
    
    else:  # AUTO ou mode inconnu
        logger.info(f"🤖 Détection automatique (mode={load_mode})")
        
        has_pk = has_primary_key(table_name)
        force_full = should_force_full(table_name)
        
        if has_pk and not force_full:
            pk = get_primary_keys(table_name)
            logger.info(f"🤖 AUTO → INCREMENTAL (PK: {pk})")
            return merge_ods_incremental(table_name, run_id)
        else:
            logger.info(f"🤖 AUTO → FULL")
            return merge_ods_full(table_name, run_id)
        

@task(name="💾 Merge ODS (FULL RESET)", retries=1)
def merge_ods_full_reset(table_name: str, run_id: str):
    """
    Mode FULL RESET : DROP + CREATE + INSERT
    
    ✅ GÈRE EXTENT : Éclate les colonnes extent en colonnes individuelles
    """
    logger = get_run_logger()
    logger.info(f"💾 Merge ODS FULL RESET pour {table_name}")
    
    src_table = f"staging_etl.stg_{table_name.lower()}"
    ods_table = f"ods.{table_name.lower()}"
    
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    try:
        # Compter source
        cur.execute(f"SELECT COUNT(*) FROM {src_table}")
        source_count = cur.fetchone()[0]
        logger.info(f"📊 Source : {source_count:,} lignes")
        
        if source_count == 0:
            logger.warning(f"⚠️ Aucune ligne dans {src_table}")
            return {
                "mode": "FULL_RESET",
                "rows_inserted": 0,
                "rows_affected": 0,
                "source_count": 0,
                "extent_expanded": False
            }
        
        # DROP table ODS
        logger.info(f"🧨 DROP {ods_table} CASCADE")
        cur.execute(f"DROP TABLE IF EXISTS {ods_table} CASCADE")
        conn.commit()
        
        # ✅ NOUVEAU : Vérifier extent
        from utils.extent_handler import has_extent_columns, build_ods_select_with_extent
        
        if has_extent_columns(table_name):
            logger.info(f"🔀 Table avec colonnes EXTENT détectée")
            
            # Récupérer colonnes staging
            cur.execute(f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = 'staging_etl' 
                  AND table_name = 'stg_{table_name.lower()}'
                ORDER BY ordinal_position
            """)
            staging_columns = [row[0] for row in cur.fetchall()]
            
            # Construire SELECT avec éclatement extent
            select_clause, ods_columns = build_ods_select_with_extent(
                table_name,
                staging_columns
            )
            
            logger.info(f"📊 {len(staging_columns)} colonnes staging → {len(ods_columns)} colonnes ODS")
            
            # CREATE avec extent éclaté
            logger.info(f"🛠️ CREATE {ods_table} avec extent éclaté")
            create_sql = f"""
                CREATE TABLE {ods_table} AS
                SELECT {select_clause}
                FROM {src_table}
            """
            cur.execute(create_sql)
            conn.commit()
            
            extent_expanded = True
            
        else:
            # Pas d'extent, traitement standard
            logger.info(f"📋 CREATE {ods_table} (sans extent)")
            cur.execute(f"""
                CREATE TABLE {ods_table} AS 
                SELECT * FROM {src_table}
            """)
            conn.commit()
            
            extent_expanded = False
        
        # Compter résultat
        cur.execute(f"SELECT COUNT(*) FROM {ods_table}")
        rows_inserted = cur.fetchone()[0]
        
        logger.info(f"✅ FULL RESET terminé : {rows_inserted:,} lignes insérées")
        
        return {
            "mode": "FULL_RESET",
            "rows_inserted": rows_inserted,
            "rows_affected": rows_inserted,
            "source_count": source_count,
            "extent_expanded": extent_expanded
        }
        
    except Exception as e:
        logger.error(f"❌ Erreur FULL RESET : {e}")
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


@task(name="✅ Vérifier ODS")
def verify_ods_after_merge(table_name: str, run_id: str):
    logger = get_run_logger()
    
    ods_table = f"ods.{table_name.lower()}"
    
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    try:
        cur.execute(f"SELECT COUNT(*) FROM {ods_table}")
        total = cur.fetchone()[0]
        
        logger.info(f"✅ ODS : {total:,} lignes")
        
        return {'exists': True, 'total_rows': total}
        
    finally:
        cur.close()
        conn.close()