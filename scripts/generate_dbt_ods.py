"""
============================================================================
Générateur automatique de modèles dbt ODS depuis métadonnées - CORRIGÉ
============================================================================
"""

import psycopg2
from pathlib import Path
import sys

sys.path.append(r'E:\Prefect\projects\ETL')
from flows.config.pg_config import config


def normalize_column_name(col_name: str) -> str:
    """Normaliser nom de colonne"""
    col_name = col_name.strip('"').strip()
    col_name = col_name.replace('-', '_').replace(' ', '_')
    if col_name[0].isupper():
        return col_name.lower()
    return col_name


def get_table_metadata():
    """Récupérer métadonnées depuis PostgreSQL"""
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    cur.execute("""
        SELECT 
            "TableName",
            "Frequency",
            "PrimaryKeyCols"
        FROM metadata.etl_tables
        WHERE "IsActive" = true
    """)
    
    tables_config = {}
    
    for table, frequency, primary_keys in cur.fetchall():
        tables_config[table] = {
            'frequency': frequency,
            'primary_keys': primary_keys or '',
            'load_mode': 'FULL'
        }
    
    print(f"  📋 {len(tables_config)} table(s) actives")
    
    tables_structure = {}
    
    for table_name in tables_config.keys():
        cur.execute("""
            SELECT "ColumnName", "DataType"
            FROM metadata.proginovcolumns
            WHERE "TableName" = %s
            ORDER BY "ColumnName"
        """, (table_name,))
        
        tables_structure[table_name] = {
            'columns': [],
            'primary_keys': [],
            'config': tables_config[table_name]
        }
        
        pk_string = tables_config[table_name]['primary_keys']
        primary_keys_raw = [pk.strip() for pk in pk_string.split(',')] if pk_string else []
        
        for col, dtype in cur.fetchall():
            col_normalized = normalize_column_name(col)
            tables_structure[table_name]['columns'].append({
                'name': col_normalized,
                'original_name': col,
                'type': dtype
            })
            
            if col in primary_keys_raw or col.lower() in [pk.lower() for pk in primary_keys_raw]:
                tables_structure[table_name]['primary_keys'].append(col_normalized)
        
        print(f"    ✅ {table_name} : {len(tables_structure[table_name]['columns'])} cols")
    
    cur.close()
    conn.close()
    
    return tables_structure


def detect_materialization(table_name: str, config: dict) -> str:
    """Déterminer stratégie"""
    transactional_patterns = ['ligne', 'entet', 'histo']
    snapshot_patterns = ['stock', 'deppro']
    
    table_lower = table_name.lower()
    
    for pattern in transactional_patterns:
        if pattern in table_lower:
            return 'incremental'
    
    for pattern in snapshot_patterns:
        if pattern in table_lower:
            return 'table'
    
    return 'table'


def generate_column_mapping(columns: list, table_name: str) -> dict:
    """Mapping intelligent"""
    mappings = {}
    
    for col in columns:
        col_name = col['name']
        
        if col_name.startswith('cod_'):
            business_name = col_name.replace('cod_', 'code_')
        elif col_name.startswith('dat_'):
            business_name = col_name.replace('dat_', 'date_')
        elif col_name.startswith('mt_'):
            business_name = col_name.replace('mt_', 'montant_')
        elif col_name.startswith('qte_'):
            business_name = col_name.replace('qte_', 'quantite_')
        elif col_name.startswith('px_'):
            business_name = col_name.replace('px_', 'prix_')
        elif col_name.startswith('no_'):
            business_name = col_name.replace('no_', 'numero_')
        else:
            business_name = col_name
        
        mappings[col_name] = business_name
    
    return mappings


def generate_ods_model(table_name: str, structure: dict) -> str:
    """Générer SQL ODS - AVEC VIRGULES"""
    
    table_lower = table_name.lower()
    materialization = detect_materialization(table_name, structure['config'])
    pks = structure['primary_keys']
    columns = structure['columns']
    
    unique_key = pks[0] if len(pks) == 1 else pks if len(pks) > 1 else 'uniq_id'
    
    config_block = f"""{{{{
    config(
        materialized='{materialization}',
        schema='ods',"""
    
    if materialization == 'incremental':
        if isinstance(unique_key, list):
            config_block += f"""
        unique_key={unique_key},"""
        else:
            config_block += f"""
        unique_key='{unique_key}',"""
        config_block += """
        on_schema_change='sync_all_columns',"""
    
    config_block += f"""
        tags=['ods', '{table_lower}']
    )
}}}}"""
    
    staging_cte = f"""
WITH staging AS (
    SELECT * FROM {{{{ ref('stg_{table_lower}') }}}}"""
    
    if materialization == 'incremental':
        staging_cte += """
    
    {% if is_incremental() %}
    WHERE _loaded_at > (SELECT MAX(source_loaded_at) FROM {{ this }})
    {% endif %}"""
    
    staging_cte += """
),"""
    
    col_mappings = generate_column_mapping(columns, table_name)
    
    # Colonnes importantes
    important_cols = [c for c in columns if any(x in c['name'] for x in 
                     ['cod_', 'no_', 'dat_', 'qte_', 'px_', 'mt_', 'statut', 'depot'])][:20]
    
    select_lines = []
    
    # ✅ FIX : Ajouter les colonnes AVEC virgules
    for col in important_cols:
        col_name = col['name']
        business_name = col_mappings[col_name]
        
        if col_name != business_name:
            select_lines.append(f"        {col_name} AS {business_name},")
        else:
            select_lines.append(f"        {col_name},")
    
    # Metadata (dernière ligne SANS virgule)
    select_lines.extend([
        "",
        "        -- Metadata",
        "        _loaded_at AS source_loaded_at,",
        "        _source_file AS source_file,",
        "        _sftp_log_id AS sftp_log_id,",
        "        CURRENT_TIMESTAMP AS ods_updated_at"  # ← Pas de virgule
    ])
    
    select_block = f"""
final AS (
    SELECT
{chr(10).join(select_lines)}
        
    FROM staging
)

SELECT * FROM final"""
    
    sql = f"""{config_block}

/*
=================================================================
Modèle : ods_{table_lower}
Description : Modèle ODS auto-généré
Source : staging.stg_{table_lower}
Stratégie : {materialization.upper()}
=================================================================
*/

{staging_cte}
{select_block}"""
    
    return sql


def main():
    """Fonction principale"""
    print("=" * 70)
    print("🔨 Génération automatique des modèles dbt ODS (CORRIGÉ)")
    print("=" * 70)
    
    print("\n📊 Lecture métadonnées...")
    tables_structure = get_table_metadata()
    
    ods_dir = Path(config.dbt_project_dir) / "models" / "ods"
    ods_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"\n🔨 Génération des modèles ODS dans {ods_dir}...")
    
    for table_name, structure in tables_structure.items():
        table_lower = table_name.lower()
        model_file = ods_dir / f"ods_{table_lower}.sql"
        
        sql_content = generate_ods_model(table_name, structure)
        
        with open(model_file, 'w', encoding='utf-8') as f:
            f.write(sql_content)
        
        mat = detect_materialization(table_name, structure['config'])
        print(f"  ✅ ods_{table_lower}.sql ({mat.upper()})")
    
    print("\n" + "=" * 70)
    print(f"✅ {len(tables_structure)} modèle(s) ODS générés (CORRIGÉS)")
    print("=" * 70)
    print("\n📋 Prochaines étapes :")
    print("  1. dbt compile --select ods.*")
    print("  2. dbt run --select ods.*")


if __name__ == "__main__":
    main()