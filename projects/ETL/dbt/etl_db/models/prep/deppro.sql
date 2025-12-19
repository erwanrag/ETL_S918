{{ config(
    materialized='incremental',
    unique_key=['cod_fou', 'cod_pro', 'depot'],
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    post_hook=[
        "{% if is_incremental() %}DELETE FROM {{ this }} t WHERE NOT EXISTS (SELECT 1 FROM {{ source('ods', 'deppro') }} s WHERE s.cod_fou = t.cod_fou AND s.cod_pro = t.cod_pro AND s.depot = t.depot){% endif %}",
        "CREATE UNIQUE INDEX IF NOT EXISTS deppro_pkey ON {{ this }} USING btree (cod_fou, cod_pro, depot)",
        "CREATE INDEX IF NOT EXISTS idx_deppro_etl_source_timestamp ON {{ this }} USING btree (_etl_source_timestamp)",
        "ANALYZE {{ this }}"
    ]
) }}

/*
============================================================================
PREP MODEL : deppro
============================================================================
Generated : 2025-12-15 16:41:58
Source    : ods.deppro
Rows ODS  : 2,804,831
Cols ODS  : 101
Cols PREP : 19 (+ _prep_loaded_at)
Strategy  : INCREMENTAL
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
