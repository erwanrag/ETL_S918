"""
============================================================================
Nettoyage des commentaires ODS (Fix Encoding Error)
============================================================================
"""
import psycopg2
import sys
from pathlib import Path

# Setup path pour importer la config
sys.path.append(str(Path(__file__).parent.parent))
from flows.config.pg_config import config

def clean_comments():
    print("=" * 60)
    print("🧹 NETTOYAGE DES COMMENTAIRES ODS (Fix Encoding)")
    print("=" * 60)
    
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    try:
        # 1. Récupérer toutes les colonnes des tables ODS
        cur.execute("""
            SELECT table_name, column_name
            FROM information_schema.columns
            WHERE table_schema = 'ods'
        """)
        columns = cur.fetchall()
        
        print(f"[INFO] {len(columns)} colonnes trouvées dans ODS.")
        print("[PROCESS] Suppression des commentaires...")
        
        count = 0
        for table, col in columns:
            # Supprimer le commentaire
            sql = f'COMMENT ON COLUMN ods."{table}"."{col}" IS NULL'
            cur.execute(sql)
            count += 1
            
        conn.commit()
        print(f"[OK] {count} commentaires supprimés avec succès.")
        print("[INFO] Vous pouvez maintenant relancer 'dbt compile'")
        
    except Exception as e:
        print(f"[ERROR] Erreur : {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    clean_comments()
    