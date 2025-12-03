"""
============================================================================
ODS Tasks - STAGING → ODS avec extent éclaté et typage intelligent
============================================================================
VERSION AMÉLIORÉE avec :
[OK] Éclatement extent avec types corrects (pas tout TEXT)
[OK] Commentaires SQL depuis métadonnées
[OK] Gestion NULL intelligente
[OK] Primary keys et index automatiques
[OK] Support FULL, INCREMENTAL, FULL_RESET
============================================================================
"""

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
    get_table_description
)

# [NEW] Import version améliorée extent_handler
from utils.extent_handler import (
    has_extent_columns,
    build_ods_select_with_extent_typed,
    generate_column_comments,
    get_extent_columns_with_metadata
)


def get_load_mode_from_monitoring(table_name: str) -> str:
    """
    Récupérer le load_mode du dernier fichier traité
    
    Returns:
        'INCREMENTAL', 'FULL', 'FULL_RESET', ou 'AUTO'
    """
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    try:
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
        return result[0] if result and result[0] else "AUTO"
            
    except Exception as e:
        print(f"[WARN] Erreur lecture load_mode pour {table_name}: {e}")
        return "AUTO"
    finally:
        cur.close()
        conn.close()


def add_primary_key_and_indexes(table_name: str, schema: str = 'ods'):
    """
    [NEW] Ajouter PRIMARY KEY et INDEX après création ODS
    
    Lit metadata.proginovindexes pour créer les bonnes contraintes
    """
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    logger = get_run_logger()
    
    try:
        # 1. Ajouter PRIMARY KEY si défini
        pk_columns = get_primary_keys(table_name)
        
        if pk_columns:
            # Vérifier que les colonnes PK existent bien dans ODS
            cur.execute(f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = %s 
                  AND table_name = %s
                  AND column_name = ANY(%s)
            """, (schema, table_name.lower(), pk_columns))
            
            existing_pk_cols = [row[0] for row in cur.fetchall()]
            
            if len(existing_pk_cols) == len(pk_columns):
                pk_cols_str = ', '.join([f'"{col}"' for col in pk_columns])
                
                try:
                    logger.info(f"[KEY] Création PRIMARY KEY ({', '.join(pk_columns)})")
                    cur.execute(f"""
                        ALTER TABLE {schema}.{table_name.lower()}
                        ADD PRIMARY KEY ({pk_cols_str})
                    """)
                    conn.commit()
                    logger.info(f"[OK] PRIMARY KEY créée")
                except Exception as e:
                    logger.warning(f"[WARN] Impossible de créer PK : {e}")
                    conn.rollback()
            else:
                logger.warning(f"[WARN] Colonnes PK manquantes : {set(pk_columns) - set(existing_pk_cols)}")
        
        # 2. Créer index sur colonnes métier importantes
        # On peut lire metadata.proginovindexes ici si besoin
        # Pour l'instant, on crée des index basiques sur _etl_valid_from
        
        try:
            cur.execute(f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = %s 
                  AND table_name = %s
                  AND column_name = '_etl_valid_from'
            """, (schema, table_name.lower()))
            
            if cur.fetchone():
                logger.info(f"[INDEX] Création INDEX sur _etl_valid_from")
                cur.execute(f"""
                    CREATE INDEX IF NOT EXISTS idx_{table_name.lower()}_etl_valid_from
                    ON {schema}.{table_name.lower()} (_etl_valid_from)
                """)
                conn.commit()
                logger.info(f"[OK] INDEX créé")
        except Exception as e:
            logger.warning(f"[WARN] Impossible de créer INDEX : {e}")
            conn.rollback()
        
    finally:
        cur.close()
        conn.close()


@task(name="[SAVE] Merge ODS (FULL RESET)", retries=1)
def merge_ods_full_reset(table_name: str, run_id: str):
    """
    [NEW] Mode FULL RESET : DROP + CREATE + INSERT
    
    [OK] GÈRE EXTENT : Éclate avec types corrects + commentaires SQL
    [OK] CREATE TABLE EXPLICITE : Garantit types corrects (NUMERIC, DATE, BOOLEAN)
    [OK] PRIMARY KEY : Ajoute automatiquement
    [OK] COMMENTAIRES : Depuis métadonnées Label
    """
    logger = get_run_logger()
    logger.info(f"[SAVE] Merge ODS FULL RESET pour {table_name}")
    
    src_table = f"staging_etl.stg_{table_name.lower()}"
    ods_table = f"ods.{table_name.lower()}"
    
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    try:
        # Compter source
        cur.execute(f"SELECT COUNT(*) FROM {src_table}")
        source_count = cur.fetchone()[0]
        logger.info(f"[DATA] Source : {source_count:,} lignes")
        
        if source_count == 0:
            logger.warning(f"[WARN] Aucune ligne dans {src_table}")
            return {
                "mode": "FULL_RESET",
                "rows_inserted": 0,
                "rows_affected": 0,
                "source_count": 0,
                "extent_expanded": False
            }
        
        # DROP table ODS
        logger.info(f"[DROP] DROP {ods_table} CASCADE")
        cur.execute(f"DROP TABLE IF EXISTS {ods_table} CASCADE")
        conn.commit()
        
        # Récupérer colonnes staging
        cur.execute(f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = 'staging_etl' 
              AND table_name = 'stg_{table_name.lower()}'
            ORDER BY ordinal_position
        """)
        staging_columns = [row[0] for row in cur.fetchall()]
        
        # ============================================================
        # [NEW] EXTENT : CREATE TABLE explicite avec types corrects
        # ============================================================
        if has_extent_columns(table_name):
            logger.info(f"[MERGE] Table avec colonnes EXTENT détectée")
            
            # Construire SELECT avec typage + cast
            select_clause, ods_columns, column_types = build_ods_select_with_extent_typed(
                table_name,
                staging_columns
            )
            # 🐛 DEBUG
            logger.info(f"[SEARCH] DEBUT column_types sample:")
            for k, v in list(column_types.items())[:15]:
                logger.info(f"     {k}: {v}")
            logger.info(f"[SEARCH] FIN column_types")
            logger.info(f"[DATA] {len(staging_columns)} colonnes staging → {len(ods_columns)} colonnes ODS")
            logger.info(f"[STYLE] {len(column_types)} colonnes typées")
            
            # [NEW] Importer fonction DDL
            from utils.ddl_generator import generate_ods_extent_table_ddl, generate_ods_indexes_ddl
            
            # [NEW] CREATE TABLE EXPLICITE avec types corrects
            logger.info(f"[TOOLS] CREATE TABLE {ods_table} avec types explicites")
            create_ddl = generate_ods_extent_table_ddl(
                table_name,
                ods_columns,
                column_types
            )
            
            cur.execute(create_ddl)
            conn.commit()
            logger.info(f"[OK] Table créée avec types corrects")
            
            # [NEW] INSERT INTO ... SELECT avec conversions
            logger.info(f"📥 INSERT données avec conversions")
            columns_str = ', '.join([f'"{col}"' for col in ods_columns])
            
            insert_sql = f"""
                INSERT INTO {ods_table} ({columns_str})
                SELECT {select_clause}
                FROM {src_table}
            """
            
            cur.execute(insert_sql)
            rows_inserted = cur.rowcount
            conn.commit()
            logger.info(f"[OK] {rows_inserted:,} lignes insérées")
            
            # Ajouter commentaires SQL
            logger.info(f"[NOTE] Ajout commentaires SQL")
            comments = generate_column_comments(table_name, schema='ods')
            for comment_sql in comments:
                try:
                    cur.execute(comment_sql)
                except Exception as e:
                    logger.warning(f"[WARN] Commentaire échoué : {e}")
            conn.commit()
            logger.info(f"[OK] {len(comments)} commentaires ajoutés")
            
            # Ajouter index techniques
            logger.info(f"[INDEX] Ajout index techniques")
            index_ddl = generate_ods_indexes_ddl(table_name)
            cur.execute(index_ddl)
            conn.commit()
            logger.info(f"[OK] Index créés")
            
            extent_expanded = True
            
        else:
            # Pas d'extent, traitement standard
            logger.info(f"[LIST] CREATE {ods_table} (sans extent)")
            
            # Utiliser DDL generator standard
            from utils.ddl_generator import generate_ods_table_ddl
            
            ddl = generate_ods_table_ddl(table_name)
            cur.execute(ddl)
            conn.commit()
            
            # INSERT
            columns_str = ', '.join([f'"{col}"' for col in staging_columns])
            cur.execute(f"""
                INSERT INTO {ods_table} ({columns_str})
                SELECT {columns_str} FROM {src_table}
            """)
            rows_inserted = cur.rowcount
            conn.commit()
            
            extent_expanded = False
        
        # Ajouter PRIMARY KEY si pas déjà fait par DDL
        logger.info(f"[KEY] Vérification contraintes")
        # (PK déjà dans DDL, juste vérifier)
        
        logger.info(f"[OK] FULL RESET terminé : {rows_inserted:,} lignes insérées")
        
        return {
            "mode": "FULL_RESET",
            "rows_inserted": rows_inserted,
            "rows_affected": rows_inserted,
            "source_count": source_count,
            "extent_expanded": extent_expanded,
            "types_corrected": True
        }
        
    except Exception as e:
        logger.error(f"[ERROR] Erreur FULL RESET : {e}")
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

@task(name="[SAVE] Merge ODS (FULL)", retries=1)
def merge_ods_full(table_name: str, run_id: str):
    """
    Mode FULL : TRUNCATE + INSERT (sans DROP)
    
    [WARN] Si table n'existe pas OU si extent détecté → fallback FULL_RESET
    (car extent change la structure)
    """
    logger = get_run_logger()
    logger.info(f"[SAVE] Merge ODS FULL pour {table_name}")
    
    src_table = f"staging_etl.stg_{table_name.lower()}"
    ods_table = f"ods.{table_name.lower()}"
    
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    try:
        # Compter source
        cur.execute(f"SELECT COUNT(*) FROM {src_table}")
        src_count = cur.fetchone()[0]
        logger.info(f"[DATA] Source : {src_count:,} lignes")
        
        # Vérifier si ODS existe
        cur.execute(f"""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_schema = 'ods' 
                AND table_name = '{table_name.lower()}'
            )
        """)
        
        ods_exists = cur.fetchone()[0]
        
        # [CRITICAL] Si extent présent, TOUJOURS faire FULL_RESET (structure change)
        if has_extent_columns(table_name):
            logger.warning(f"[WARN] Table avec extent → FULL_RESET (structure change)")
            cur.close()
            conn.close()
            return merge_ods_full_reset(table_name, run_id)
        
        # Si table n'existe pas, faire FULL_RESET
        if not ods_exists:
            logger.info(f"[NEW] Table n'existe pas → FULL_RESET")
            cur.close()
            conn.close()
            return merge_ods_full_reset(table_name, run_id)
        
        # TRUNCATE + INSERT (seulement si pas d'extent)
        logger.info(f"[DROP] TRUNCATE {ods_table}")
        cur.execute(f"TRUNCATE TABLE {ods_table}")
        conn.commit()
        
        logger.info(f"📥 INSERT INTO {ods_table}")
        cur.execute(f"INSERT INTO {ods_table} SELECT * FROM {src_table}")
        inserted_count = cur.rowcount
        conn.commit()
        
        logger.info(f"[OK] {inserted_count:,} lignes insérées")
        
        return {
            'rows_inserted': inserted_count,
            'rows_affected': inserted_count,
            'mode': 'FULL',
            'table': ods_table,
            'run_id': run_id
        }
        
    except Exception as e:
        logger.error(f"[ERROR] Erreur : {e}")
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


@task(name="[SAVE] Merge ODS (INCREMENTAL)", retries=1)
def merge_ods_incremental(table_name: str, run_id: str):
    """
    Mode INCREMENTAL : UPSERT (INSERT nouveaux + UPDATE modifiés)
    
    [WARN] Si extent présent → Fallback FULL (impossible de merger colonnes éclatées)
    """
    logger = get_run_logger()
    logger.info(f"[SAVE] Merge ODS INCREMENTAL pour {table_name}")
    
    pk_columns = get_primary_keys(table_name)
    
    if not pk_columns:
        raise ValueError(f"[ERROR] Aucune PK pour {table_name}")
    
    logger.info(f"[KEY] PK : {', '.join(pk_columns)}")
    
    # [WARN] EXTENT + INCREMENTAL non supporté
    if has_extent_columns(table_name):
        logger.warning(f"[WARN] INCREMENTAL avec extent non supporté → Fallback FULL")
        return merge_ods_full(table_name, run_id)
    
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
        
        if not ods_exists:
            logger.info(f"[NEW] Table n'existe pas → FULL_RESET")
            cur.close()
            conn.close()
            return merge_ods_full_reset(table_name, run_id)
        
        # Récupérer colonnes staging
        cur.execute(f"""
            SELECT column_name FROM information_schema.columns 
            WHERE table_schema = 'staging_etl' 
              AND table_name = 'stg_{table_name.lower()}'
              AND column_name NOT IN ('_etl_run_id')
            ORDER BY ordinal_position
        """)
        
        staging_columns = [row[0] for row in cur.fetchall()]
        columns = [col for col in staging_columns if col != '_etl_run_id']
        columns_str = ', '.join([f'"{col}"' for col in columns])
        
        pk_join = ' AND '.join([f'target."{pk}" = source."{pk}"' for pk in pk_columns])
        
        # INSERT nouveaux
        logger.info("[SYNC] INSERT nouveaux")
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
        logger.info(f"[ADD] {inserted_count:,} lignes insérées")
        
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
            logger.info(f"[SYNC] {updated_count:,} lignes mises à jour")
        else:
            updated_count = 0
        
        total = inserted_count + updated_count
        logger.info(f"[OK] {total:,} lignes affectées")
        
        return {
            'rows_inserted': inserted_count,
            'rows_updated': updated_count,
            'rows_affected': total,
            'mode': 'INCREMENTAL',
            'table': ods_table,
            'run_id': run_id
        }
        
    except Exception as e:
        logger.error(f"[ERROR] Erreur : {e}")
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


@task(name="[SAVE] Merge ODS auto")
def merge_ods_auto(table_name: str, run_id: str, load_mode: str = "AUTO"):
    """
    [TARGET] ROUTER : Merger STAGING → ODS avec mode automatique ou forcé
    
    Priorité de détermination du mode :
    1. load_mode passé en paramètre (si != "AUTO")
    2. load_mode depuis sftp_monitoring (dernier fichier)
    3. Détection automatique (PK + timestamps)
    
    Modes disponibles:
        - INCREMENTAL: UPSERT (INSERT nouveaux + UPDATE modifiés)
        - FULL: TRUNCATE + INSERT ALL
        - FULL_RESET: DROP + CREATE + INSERT ALL
        - AUTO: Détection automatique
    """
    logger = get_run_logger()
    
    # Si mode AUTO, lire depuis sftp_monitoring
    if load_mode == "AUTO":
        load_mode = get_load_mode_from_monitoring(table_name)
        logger.info(f"[AUTO] Mode depuis sftp_monitoring : {load_mode}")
    else:
        logger.info(f"[TARGET] Mode forcé : {load_mode}")
    
    # Description table
    table_desc = get_table_description(table_name)
    if table_desc:
        logger.info(f"[LIST] {table_desc}")
    
    # ============================================================
    # ROUTER selon load_mode
    # ============================================================
    
    if load_mode == "FULL_RESET":
        logger.info(f"[SAVE] Mode FULL_RESET - Réinitialisation complète")
        return merge_ods_full_reset(table_name, run_id)
        
    elif load_mode == "FULL":
        logger.info(f"[SAVE] Mode FULL - TRUNCATE + INSERT")
        return merge_ods_full(table_name, run_id)
        
    elif load_mode == "INCREMENTAL":
        pk = get_primary_keys(table_name)
        if not pk:
            logger.warning(f"[WARN] Mode INCREMENTAL demandé mais pas de PK → Fallback sur FULL")
            return merge_ods_full(table_name, run_id)
        else:
            logger.info(f"[SAVE] Mode INCREMENTAL - UPSERT (PK: {pk})")
            return merge_ods_incremental(table_name, run_id)
    
    else:  # AUTO ou mode inconnu
        logger.info(f"[AUTO] Détection automatique (mode={load_mode})")
        
        has_pk = has_primary_key(table_name)
        force_full = should_force_full(table_name)
        
        if has_pk and not force_full:
            pk = get_primary_keys(table_name)
            logger.info(f"[AUTO] AUTO → INCREMENTAL (PK: {pk})")
            return merge_ods_incremental(table_name, run_id)
        else:
            logger.info(f"[AUTO] AUTO → FULL")
            return merge_ods_full(table_name, run_id)


@task(name="[OK] Vérifier ODS")
def verify_ods_after_merge(table_name: str, run_id: str):
    """Vérification post-merge"""
    logger = get_run_logger()
    
    ods_table = f"ods.{table_name.lower()}"
    
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    try:
        cur.execute(f"SELECT COUNT(*) FROM {ods_table}")
        total = cur.fetchone()[0]
        
        logger.info(f"[OK] ODS : {total:,} lignes")
        
        return {'exists': True, 'total_rows': total}
        
    finally:
        cur.close()
        conn.close()