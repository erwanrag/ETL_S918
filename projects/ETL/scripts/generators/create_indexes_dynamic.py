"""
Création dynamique index ODS
"""

import psycopg2
import sys
from pathlib import Path

# Ajouter au path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from shared.config import config

conn = psycopg2.connect(config.get_connection_string())
conn.autocommit = True  # CONCURRENTLY nécessite autocommit
cur = conn.cursor()

# Récupérer les tables avec PK
cur.execute("""
    SELECT t."TableName", t."PrimaryKeyCols"
    FROM metadata.etl_tables t
    WHERE t."PrimaryKeyCols" IS NOT NULL 
      AND t."PrimaryKeyCols" != ''
""")

tables = cur.fetchall()

for table_name, pk_str in tables:
    # Vérifier si table existe
    cur.execute("""
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'ods' AND table_name = %s
        )
    """, (table_name.lower(),))
    
    if not cur.fetchone()[0]:
        print(f"[SKIP] {table_name} - Table n'existe pas")
        continue
    
    pk_cols = [pk.strip() for pk in pk_str.split(',')]
    pk_list = ', '.join(pk_cols)
    
    index_name = f"idx_ods_{table_name.lower()}_upsert"
    
    sql = f"""
    CREATE INDEX CONCURRENTLY IF NOT EXISTS {index_name}
    ON ods.{table_name.lower()} ({pk_list}, _etl_hashdiff)
    """
    
    print(f"[CREATE] {index_name}")
    try:
        cur.execute(sql)
        print(f"[OK] Index créé")
    except Exception as e:
        print(f"[SKIP] {e}")

# Analyser seulement les tables qui existent
print("\n[ANALYZE] Tables ODS...")
cur.execute("""
    SELECT table_name FROM information_schema.tables
    WHERE table_schema = 'ods'
""")
existing_tables = [row[0] for row in cur.fetchall()]

for table_name in existing_tables:
    cur.execute(f"ANALYZE ods.{table_name}")
    print(f"[OK] {table_name}")

cur.close()
conn.close()

print("\n[DONE] Index créés")