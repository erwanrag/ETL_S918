{{ config(
    materialized='incremental',
    unique_key='cod_pro',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    post_hook=[
        "CREATE UNIQUE INDEX IF NOT EXISTS lldespro_pkey ON {{ this }} USING btree (cod_pro, langue)",
        "ANALYZE {{ this }}",
        "DELETE FROM {{ this }} WHERE cod_pro NOT IN (SELECT cod_pro FROM {{ source('ods', 'lldespro') }})"
    ]
) }}

/*
    ============================================================================
    Modèle PREP : lldespro
    ============================================================================
    Généré automatiquement le 2025-12-12 16:41:52
    
    Source       : ods.lldespro
    Lignes       : 551,592
    Colonnes ODS : 9
    Colonnes PREP: 7  (+ _prep_loaded_at)
    Exclues      : 3 (33.3%)
    
    Stratégie    : INCREMENTAL
    Unique Key  : cod_pro
    Merge        : INSERT/UPDATE + DELETE orphans
    Incremental  : Enabled (_etl_valid_from)
    Index        : 2 répliqué(s) + ANALYZE
    
    Exclusions:
      - Techniques ETL  : 1
      - 100% NULL       : 1
      - Constantes      : 1
      - Faible valeur   : 0
    ============================================================================
*/

SELECT
    "cod_pro" AS cod_pro,
    "langue" AS langue,
    "des_lan" AS des_lan,
    "des_lan2" AS des_lan2,
    "_etl_valid_from" AS _etl_source_timestamp,
    "_etl_run_id" AS _etl_run_id,
    CURRENT_TIMESTAMP AS _prep_loaded_at
FROM {{ source('ods', 'lldespro') }}

{% if is_incremental() %}
    WHERE "_etl_valid_from" > (
        SELECT COALESCE(MAX(_etl_source_timestamp), '1900-01-01'::timestamp) 
        FROM {{ this }}
    )
{% endif %}
