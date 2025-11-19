"""
============================================================================
Générateur automatique de modèles dbt STAGING depuis métadonnées
============================================================================
Fichier : E:\Prefect\projects\ETL\scripts\generate_dbt_staging.py

Objectif :
- Lire metadata.ProginovTables (structure colonnes)
- Lire metadata.ETL_Tables (config load_mode)
- Générer stg_*.sql pour CHAQUE table
- Générer _staging.yml avec tests
============================================================================
"""

import psycopg2
from pathlib import Path
import sys

# Ajouter le chemin config
sys.path.append(r'E:\Prefect\projects\ETL')
from flows.config.pg_config import config


def normalize_column_name(col_name: str) -> str:
    """
    Normaliser nom de colonne SQL → dbt
    - Supprimer guillemets
    - Convertir en snake_case si besoin
    - Remplacer tirets par underscore
    """
    col_name = col_name.strip('"').strip()
    
    # Remplacer tirets et espaces par underscore
    col_name = col_name.replace('-', '_').replace(' ', '_')
    
    # Si colonne est en UPPERCASE, convertir en lowercase
    if col_name.isupper():
        return col_name.lower()
    
    # Si commence par majuscule (PascalCase comme TabPart_client), convertir en lowercase
    if col_name[0].isupper():
        return col_name.lower()
    
    # Sinon garder le format original
    return col_name


def get_table_metadata():
    """Récupérer métadonnées depuis PostgreSQL"""
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    # 1. Récupérer config chargement (tables à traiter)
    cur.execute("""
        SELECT 
            "TableName",
            "Frequency",
            "PrimaryKeyCols"
        FROM metadata.etl_tables
        WHERE "IsActive" = true
    """)
    
    load_config = {}
    tables_to_process = []
    
    for table, frequency, primary_keys in cur.fetchall():
        load_config[table] = {
            'frequency': frequency,
            'primary_keys': primary_keys
        }
        tables_to_process.append(table)
    
    print(f"  📋 {len(tables_to_process)} table(s) à traiter : {', '.join(tables_to_process)}")
    
    # 2. Récupérer structure colonnes pour ces tables uniquement
    tables_structure = {}
    
    for table_name in tables_to_process:
        # Récupérer les colonnes
        cur.execute("""
            SELECT 
                "ColumnName",
                "DataType"
            FROM metadata.proginovcolumns
            WHERE "TableName" = %s
            ORDER BY "ColumnName"
        """, (table_name,))
        
        tables_structure[table_name] = {
            'columns': [],
            'primary_keys': []
        }
        
        # Parser les primary keys depuis etl_tables (format: "col1, col2")
        pk_string = load_config[table_name].get('primary_keys', '')
        if pk_string:
            primary_keys_raw = [pk.strip() for pk in pk_string.split(',')]
        else:
            primary_keys_raw = []
        
        for col, dtype in cur.fetchall():
            col_normalized = normalize_column_name(col)
            tables_structure[table_name]['columns'].append({
                'name': col_normalized,
                'original_name': col,
                'type': dtype
            })
            
            # Vérifier si c'est une PK
            if col in primary_keys_raw or col.lower() in [pk.lower() for pk in primary_keys_raw]:
                tables_structure[table_name]['primary_keys'].append(col_normalized)
        
        print(f"    ✅ {table_name} : {len(tables_structure[table_name]['columns'])} colonnes, PK: {tables_structure[table_name]['primary_keys']}")
    
    cur.close()
    conn.close()
    
    return tables_structure, load_config


def generate_staging_model(table_name: str, structure: dict, load_config: dict) -> str:
    """Générer le SQL pour un modèle staging"""
    
    table_lower = table_name.lower()
    raw_table = f"raw_{table_lower}"
    
    # Config dbt
    config_block = "{{ config(materialized='view') }}"
    
    # Description
    description = f"-- Staging model for {table_name}"
    
    # SELECT columns
    columns = structure['columns']
    col_list = []
    
    for col in columns:
        col_original = col['original_name']
        col_normalized = col['name']
        
        # TOUJOURS quoter les colonnes RAW pour préserver la casse
        col_list.append(f'    "{col_original}" AS {col_normalized}')
    
    # Colonnes metadata
    col_list.extend([
        '    _loaded_at',
        '    _source_file',
        '    _sftp_log_id'
    ])
    
    select_cols = ',\n'.join(col_list)
    
    # SQL final
    sql = f"""{config_block}

{description}

SELECT
{select_cols}
FROM {{{{ source('raw', '{raw_table}') }}}}
"""
    
    return sql


def generate_staging_yml(tables_structure: dict, load_config: dict) -> str:
    """Générer le fichier _staging.yml avec tous les modèles"""
    
    yml_header = """version: 2

models:
"""
    
    models_yml = []
    
    for table_name, structure in tables_structure.items():
        table_lower = table_name.lower()
        pks = structure['primary_keys']
        
        model_block = f"""  - name: stg_{table_lower}
    description: "Staging model for {table_name}"
    columns:
"""
        
        # Tests sur primary keys
        for col in structure['columns']:
            col_name = col['name']
            tests = []
            
            if col_name in pks:
                tests = ['not_null', 'unique']
            
            if tests:
                model_block += f"""      - name: {col_name}
        tests:
"""
                for test in tests:
                    model_block += f"""          - {test}
"""
            else:
                model_block += f"""      - name: {col_name}
"""
        
        models_yml.append(model_block)
    
    return yml_header + '\n'.join(models_yml)


def main():
    """Fonction principale"""
    print("=" * 70)
    print("🔨 Génération automatique des modèles dbt STAGING")
    print("=" * 70)
    
    # Récupérer métadonnées
    print("\n📊 Lecture des métadonnées PostgreSQL...")
    tables_structure, load_config = get_table_metadata()
    
    print(f"✅ {len(tables_structure)} table(s) trouvée(s)")
    
    # Dossier destination
    staging_dir = Path(config.dbt_project_dir) / "models" / "staging"
    staging_dir.mkdir(parents=True, exist_ok=True)
    
    # Générer chaque modèle
    print(f"\n🔨 Génération des modèles dans {staging_dir}...")
    
    for table_name, structure in tables_structure.items():
        table_lower = table_name.lower()
        model_file = staging_dir / f"stg_{table_lower}.sql"
        
        sql_content = generate_staging_model(table_name, structure, load_config.get(table_name, {}))
        
        with open(model_file, 'w', encoding='utf-8') as f:
            f.write(sql_content)
        
        print(f"  ✅ {model_file.name}")
    
    # Générer _staging.yml
    print(f"\n📝 Génération de _staging.yml...")
    yml_content = generate_staging_yml(tables_structure, load_config)
    yml_file = staging_dir / "_staging.yml"
    
    with open(yml_file, 'w', encoding='utf-8') as f:
        f.write(yml_content)
    
    print(f"  ✅ {yml_file.name}")
    
    print("\n" + "=" * 70)
    print(f"✅ Génération terminée : {len(tables_structure)} modèle(s) staging créé(s)")
    print("=" * 70)
    print("\n📋 Prochaines étapes :")
    print("  1. cd E:\\Prefect\\projects\\ETL\\dbt\\cbm_analytics")
    print("  2. dbt compile  # Vérifier la syntaxe")
    print("  3. dbt run --models staging.*")
    print("  4. dbt test --models staging.*")


if __name__ == "__main__":
    main()