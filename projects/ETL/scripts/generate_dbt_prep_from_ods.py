"""
============================================================================
Générateur de modèles dbt PREP depuis ODS physique
============================================================================
Objectif : Synchroniser les modèles dbt avec la structure réelle des tables ODS
           générées par le pipeline Python (qui éclate les colonnes Extent).
============================================================================
"""

import psycopg2
from pathlib import Path
import sys
import os

# Ajout du chemin projet pour récupérer la config
sys.path.append(str(Path(__file__).parent.parent))
from flows.config.pg_config import config

def get_ods_tables_structure():
    """Récupère la structure exacte des tables présentes dans ODS"""
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    # Récupérer la liste des tables ODS
    cur.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'ods'
        ORDER BY table_name
    """)
    tables = [row[0] for row in cur.fetchall()]
    
    structure = {}
    
    for table in tables:
        # Récupérer les colonnes pour chaque table
        cur.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_schema = 'ods' 
              AND table_name = %s
            ORDER BY ordinal_position
        """, (table,))
        
        structure[table] = [row[0] for row in cur.fetchall()]
        
    cur.close()
    conn.close()
    return structure

def generate_prep_model_content(table_name, columns):
    """Génère le contenu SQL pour un modèle PREP"""
    
    # Exclure les colonnes techniques purement ETL si nécessaire, 
    # mais garder celles utiles pour le lineage (_etl_run_id, etc.)
    
    cols_list = []
    for col in columns:
        # On quote les colonnes pour gérer les majuscules/caractères spéciaux éventuels
        cols_list.append(f'    "{col}" AS {col.lower()}')

    select_block = ",\n".join(cols_list)
    
    sql = f"""{{{{ config(materialized='view') }}}}

/*
    Modèle PREP pour {table_name}
    Généré automatiquement depuis ODS pour inclure les colonnes éclataées (Extent)
*/

SELECT
{select_block}
FROM {{{{ source('ods', '{table_name}') }}}}
"""
    return sql

def main():
    print("=" * 70)
    print("🔄 SYNC DBT PREP <-> ODS STRUCTURE")
    print("=" * 70)
    
    dbt_prep_dir = Path(config.dbt_project_dir) / "models" / "prep"
    dbt_prep_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"[INFO] Lecture de la structure ODS dans PostgreSQL...")
    try:
        structure = get_ods_tables_structure()
    except Exception as e:
        print(f"[ERROR] Impossible de se connecter à la DB : {e}")
        print("Assurez-vous que le pipeline Python a déjà tourné au moins une fois pour créer les tables ODS.")
        return

    if not structure:
        print("[WARN] Aucune table trouvée dans le schéma 'ods'. Lancez d'abord le pipeline d'ingestion.")
        return

    print(f"[DATA] {len(structure)} tables trouvées.")
    
    count = 0
    for table_name, columns in structure.items():
        # Générer le SQL
        sql_content = generate_prep_model_content(table_name, columns)
        
        # Nom du fichier : prep_{table}.sql
        file_path = dbt_prep_dir / f"prep_{table_name}.sql"
        
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(sql_content)
            
        print(f"  ✅ Généré : prep_{table_name}.sql ({len(columns)} colonnes)")
        count += 1

    print("=" * 70)
    print(f"[OK] Terminé. {count} modèles mis à jour.")
    print("Prochaine étape : Lancer 'dbt run --models prep' ou le pipeline complet.")

if __name__ == "__main__":
    main()