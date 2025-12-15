{{ config(
    materialized='incremental',
    unique_key='cod_pro',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    post_hook=[
        "CREATE UNIQUE INDEX IF NOT EXISTS deppro_pkey ON {{ this }} USING btree (cod_fou, cod_pro, depot)",
        "ANALYZE {{ this }}",
        "DELETE FROM {{ this }} WHERE cod_pro NOT IN (SELECT cod_pro FROM {{ source('ods', 'deppro') }})"
    ]
) }}

/*
    ============================================================================
    Modèle PREP : deppro
    ============================================================================
    Généré automatiquement le 2025-12-12 16:44:56
    
    Source       : ods.deppro
    Lignes       : 2,804,387
    Colonnes ODS : 101
    Colonnes PREP: 20  (+ _prep_loaded_at)
    Exclues      : 82 (81.2%)
    
    Stratégie    : INCREMENTAL
    Unique Key  : cod_pro
    Merge        : INSERT/UPDATE + DELETE orphans
    Incremental  : Enabled (_etl_valid_from)
    Index        : 2 répliqué(s) + ANALYZE
    
    Exclusions:
      - Techniques ETL  : 1
      - 100% NULL       : 17
      - Constantes      : 64
      - Faible valeur   : 0
    ============================================================================
*/

SELECT
    "cod_pro" AS cod_pro,
    "depot" AS depot,
    "cod_fou" AS cod_fou,
    "dat_prin" AS dat_prin,
    "principa" AS principa,
    "px_mini" AS px_mini,
    "dat_ent" AS dat_ent,
    "px_rvt" AS px_rvt,
    "ach_ctr" AS ach_ctr,
    "px_std" AS px_std,
    "px_refv" AS px_refv,
    "dat_fpxv" AS dat_fpxv,
    "px_std2" AS px_std2,
    "px_std3" AS px_std3,
    "px_sim" AS px_sim,
    "px_ttc" AS px_ttc,
    "dat_remp" AS dat_remp,
    "_etl_valid_from" AS _etl_source_timestamp,
    "_etl_run_id" AS _etl_run_id,
    CURRENT_TIMESTAMP AS _prep_loaded_at
FROM {{ source('ods', 'deppro') }}

{% if is_incremental() %}
    WHERE "_etl_valid_from" > (
        SELECT COALESCE(MAX(_etl_source_timestamp), '1900-01-01'::timestamp) 
        FROM {{ this }}
    )
{% endif %}
