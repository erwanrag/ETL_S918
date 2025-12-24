"""
Générateur modèles dbt ODS - Version simplifiée SQL direct
"""

import psycopg2
from pathlib import Path
import sys
import argparse

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from shared.config import config
from ETL.flows.config.table_metadata import get_primary_keys
from typing import List

class ODSGeneratorDBT:
    
    def __init__(self):
        self.dbt_models_dir = Path(__file__).parent.parent / 'dbt' / 'etl_db' / 'models' / 'ods'
        self.dbt_models_dir.mkdir(parents=True, exist_ok=True)
    
    def get_all_active_tables(self):
        conn = psycopg2.connect(config.get_connection_string())
        cur = conn.cursor()
        try:
            cur.execute("""
                SELECT "TableName"
                FROM metadata.etl_tables
                WHERE "IsActive" = TRUE
                ORDER BY "TableName"
            """)
            return [row[0] for row in cur.fetchall()]
        finally:
            cur.close()
            conn.close()
    
    def get_table_metadata(self, table_name: str):
        conn = psycopg2.connect(config.get_connection_string())
        cur = conn.cursor()
        
        try:
            # Colonnes STAGING
            cur.execute(f"""
                SELECT column_name 
                FROM information_schema.columns
                WHERE table_schema = 'staging_etl'
                  AND table_name = 'stg_{table_name.lower()}'
                  AND column_name NOT IN ('_etl_run_id', '_etl_hashdiff', '_etl_valid_from')
                ORDER BY ordinal_position
            """)
            
            all_columns = [row[0] for row in cur.fetchall()]
            pk_columns = get_primary_keys(table_name)
            business_columns = [col for col in all_columns if col not in pk_columns]
            
            # Config SCD2
            cur.execute("""
                SELECT 
                    COALESCE("full_schedule", 'weekly') AS full_schedule,
                    COALESCE("retention_days", 365) AS retention_days,
                    COALESCE("Notes", '') AS description
                FROM metadata.etl_tables
                WHERE LOWER("TableName") = LOWER(%s)
            """, (table_name,))
            
            result = cur.fetchone()
            full_schedule, retention_days, description = result if result else ('weekly', 365, '')
            detect_deletes = full_schedule in ['weekly', 'daily']
            
            return {
                'table_name': table_name.lower(),
                'pk_columns': pk_columns,
                'business_columns': business_columns,
                'detect_deletes': detect_deletes,
                'retention_days': retention_days,
                'description': description or f"Table {table_name}"
            }
        
        finally:
            cur.close()
            conn.close()
    
    def generate_sql_model(self, table_name: str):
        """Générer SQL dbt directement (sans Jinja2)"""
        
        meta = self.get_table_metadata(table_name)
        pk_cols = meta['pk_columns']
        bus_cols = meta['business_columns']
        detect_del = meta['detect_deletes']
        retention = meta['retention_days']
        desc = meta['description']
        
        pk_list = ', '.join([f'"{pk}"' for pk in pk_cols])
        pk_py_list = str(pk_cols)  # ['uniq_id']
        
        sql = f"""{{{{
    config(
        materialized='incremental',
        unique_key={pk_py_list},
        incremental_strategy='merge',
        on_schema_change='fail',
        schema='ods_dbt'
    )
}}}}

/*
============================================================================
ODS Model: {meta['table_name']}
============================================================================
Description : {desc}
Primary Key : {', '.join(pk_cols)}
Detect Del. : {'Yes' if detect_del else 'No'}
Retention   : {retention} days
============================================================================
*/

WITH source_data AS (
    SELECT
        {', '.join([f'"{pk}"' for pk in pk_cols])},
        {', '.join([f'"{col}"' for col in bus_cols])},
        "_etl_hashdiff",
        "_etl_valid_from"
    FROM {{{{ source('staging', 'stg_{meta['table_name']}') }}}}
    
    {{% if is_incremental() %}}
    WHERE (_etl_valid_from, _etl_hashdiff) NOT IN (
        SELECT _etl_valid_from, _etl_hashdiff
        FROM {{{{ this }}}}
    )
    {{% endif %}}
),

{{% if is_incremental() %}}

current_records AS (
    SELECT
        {pk_list},
        "_etl_hashdiff",
        "_etl_valid_from",
        "last_seen_date"
    FROM {{{{ this }}}}
    WHERE "_etl_is_current" = TRUE
      AND "_etl_is_deleted" = FALSE
),

new_records AS (
    SELECT stg.*
    FROM source_data stg
    LEFT JOIN current_records ods USING ({pk_list})
    WHERE ods."{pk_cols[0]}" IS NULL
),

changed_records AS (
    SELECT 
        stg.*,
        ods."_etl_valid_from" AS previous_valid_from
    FROM source_data stg
    INNER JOIN current_records ods USING ({pk_list})
    WHERE stg."_etl_hashdiff" != ods."_etl_hashdiff"
),

unchanged_records AS (
    SELECT
        {pk_list},
        stg."_etl_valid_from" AS new_last_seen
    FROM current_records ods
    INNER JOIN source_data stg USING ({pk_list})
    WHERE stg."_etl_hashdiff" = ods."_etl_hashdiff"
),

"""
        
        # Détection suppressions conditionnelle
        if detect_del:
            sql += f"""{{% if var('load_mode', 'INCREMENTAL') == 'FULL' %}}
deleted_records AS (
    SELECT {pk_list}
    FROM current_records ods
    LEFT JOIN source_data stg USING ({pk_list})
    WHERE stg.{pk_cols[0]} IS NULL
),
{{% endif %}}

"""
        
        # Assemblage final
        all_cols_select = ', '.join([f'"{pk}"' for pk in pk_cols] + [f'"{col}"' for col in bus_cols])
        ods_cols_select = ', '.join([f'ods."{pk}"' for pk in pk_cols] + [f'ods."{col}"' for col in bus_cols])
        
        sql += f"""final AS (
    
    -- Clôturer anciennes versions
    SELECT
        ods.*,
        cr."_etl_valid_from" AS "_etl_valid_to",
        FALSE AS "_etl_is_current",
        ods."last_seen_date"
    FROM {{{{ this }}}} ods
    INNER JOIN changed_records cr USING ({pk_list})
    WHERE ods."_etl_valid_from" = cr.previous_valid_from
      AND ods."_etl_is_current" = TRUE
    
    UNION ALL
    
    -- Nouvelles versions
    SELECT
        {all_cols_select},
        "_etl_hashdiff",
        "_etl_valid_from" AS "_etl_valid_from",
        NULL::TIMESTAMP AS "_etl_valid_to",
        TRUE AS "_etl_is_current",
        FALSE AS "_etl_is_deleted",
        "_etl_valid_from" AS "last_seen_date"
    FROM changed_records
    
    UNION ALL
    
    -- Nouveaux enregistrements
    SELECT
        {all_cols_select},
        "_etl_hashdiff",
        "_etl_valid_from" AS "_etl_valid_from",
        NULL::TIMESTAMP AS "_etl_valid_to",
        TRUE AS "_etl_is_current",
        FALSE AS "_etl_is_deleted",
        "_etl_valid_from" AS "last_seen_date"
    FROM new_records
    
    UNION ALL
    
    -- Mise à jour last_seen_date
    SELECT
        {ods_cols_select},
        ods."_etl_hashdiff",
        ods."_etl_valid_from",
        ods."_etl_valid_to",
        ods."_etl_is_current",
        ods."_etl_is_deleted",
        ur."new_last_seen" AS "last_seen_date"
    FROM {{{{ this }}}} ods
    INNER JOIN unchanged_records ur USING ({pk_list})
    WHERE ods."_etl_is_current" = TRUE
    
"""
        
        # Suppressions conditionnelles
        if detect_del:
            sql += f"""    {{% if var('load_mode', 'INCREMENTAL') == 'FULL' %}}
    UNION ALL
    
    -- Marquer suppressions
    SELECT
        {ods_cols_select},
        ods."_etl_hashdiff",
        ods."_etl_valid_from",
        CURRENT_TIMESTAMP AS "_etl_valid_to",
        FALSE AS "_etl_is_current",
        TRUE AS "_etl_is_deleted",
        ods."last_seen_date"
    FROM {{{{ this }}}} ods
    INNER JOIN deleted_records dr USING ({pk_list})
    WHERE ods."_etl_is_current" = TRUE
    {{% endif %}}
    
"""
        
        sql += f"""    UNION ALL
    
    -- Historique ancien
    SELECT * FROM {{{{ this }}}}
    WHERE "_etl_is_current" = FALSE
)

{{% else %}}

-- Initial load
final AS (
    SELECT
        {all_cols_select},
        "_etl_hashdiff",
        "_etl_valid_from" AS "_etl_valid_from",
        NULL::TIMESTAMP AS "_etl_valid_to",
        TRUE AS "_etl_is_current",
        FALSE AS "_etl_is_deleted",
        "_etl_valid_from" AS "last_seen_date"
    FROM source_data
)

{{% endif %}}

SELECT * FROM final

{{% if is_incremental() %}}
WHERE "_etl_valid_to" IS NULL 
   OR "_etl_valid_to" >= CURRENT_DATE - INTERVAL '{retention} days'
{{% endif %}}
"""
        
        # Écrire fichier
        output_file = self.dbt_models_dir / f"ods_{table_name.lower()}.sql"
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(sql)
        
        print(f"  ✅ {output_file.name}")
        return output_file
    
    def generate_sources_yml(self, tables: List[str]):
        """Générer sources.yml dynamiquement"""
        
        sources_content = """# ============================================================================
# Sources STAGING - Généré automatiquement
# ============================================================================
# Ne pas éditer manuellement - Regénéré par ods_generator_dbt.py

version: 2

sources:
  - name: staging
    description: "Tables staging avec typage, extent éclaté, hashdiff"
    database: etl_db
    schema: staging_etl
    
    tables:
"""
        
        for table in sorted(tables):
            metadata = self.get_table_metadata(table)
            
            sources_content += f"""      - name: stg_{table.lower()}
        description: "{metadata['description']}"
        columns:
"""
            
            # Colonnes clés
            for pk in metadata['pk_columns']:
                sources_content += f"""          - name: {pk}
            description: "Clé primaire"
            tests:
              - not_null
"""
            
            # Colonnes techniques
            sources_content += """          - name: _etl_hashdiff
            description: "Hash MD5 colonnes métier"
            tests:
              - not_null
          
          - name: _etl_valid_from
            description: "Timestamp chargement STAGING"
            tests:
              - not_null

"""
        
        # Écrire fichier
        output_file = self.dbt_models_dir / 'sources.yml'
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(sources_content)
        
        print(f"  ✅ sources.yml ({len(tables)} tables)")
        
        return output_file
    
    def generate_schema_yml(self, tables: List[str]):
        """Générer schema.yml avec tests SCD2"""
        
        schema_content = """# ============================================================================
# Schema ODS - Tests SCD2 - Généré automatiquement
# ============================================================================
# Ne pas éditer manuellement - Regénéré par ods_generator_dbt.py

version: 2

models:
"""
        
        for table in sorted(tables):
            metadata = self.get_table_metadata(table)
            
            schema_content += f"""  - name: ods_{table.lower()}
    description: "{metadata['description']} - SCD2"
    config:
      tags: ["ods", "scd2"]
    
    columns:
"""
            
            # Primary keys
            for pk in metadata['pk_columns']:
                schema_content += f"""      - name: {pk}
        description: "Clé primaire"
        tests:
          - not_null
      
"""
            
            # Colonnes SCD2 standardisées
            schema_content += """      - name: _etl_valid_from
        description: "Date début validité version"
        tests:
          - not_null
      
      - name: _etl_valid_to
        description: "Date fin validité (NULL si version courante)"
      
      - name: _etl_is_current
        description: "Version courante (TRUE/FALSE)"
        tests:
          - not_null
          - accepted_values:
              values: [true, false]
      
      - name: _etl_is_deleted
        description: "Enregistrement supprimé de la source"
        tests:
          - not_null
          - accepted_values:
              values: [true, false]
      
      - name: last_seen_date
        description: "Dernière apparition dans STAGING"
        tests:
          - not_null
      
      - name: _etl_hashdiff
        description: "Hash MD5 colonnes métier"
        tests:
          - not_null

"""
        
        # Écrire fichier
        output_file = self.dbt_models_dir / 'schema.yml'
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(schema_content)
        
        print(f"  ✅ schema.yml ({len(tables)} tables)")
        
        return output_file
    
    
    def generate_all(self, table_name=None):
        print("=" * 70)
        print("🔨 Génération modèles dbt ODS")
        print("=" * 70)
        
        tables = [table_name] if table_name else self.get_all_active_tables()
        print(f"[MODE] {len(tables)} table(s)")
        print()
        
        print("[1/1] Génération modèles SQL...")
        for table in tables:
            try:
                self.generate_sql_model(table)
            except Exception as e:
                print(f"  ❌ {table}: {e}")
        
        print()
        print("=" * 70)
        print(f"✅ Terminé : {len(tables)} table(s)")
        print("=" * 70)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--table', type=str)
    args = parser.parse_args()
    
    generator = ODSGeneratorDBT()
    generator.generate_all(table_name=args.table)


if __name__ == "__main__":
    main()