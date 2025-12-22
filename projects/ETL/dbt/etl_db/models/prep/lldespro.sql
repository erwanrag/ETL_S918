{{ config(
    materialized='incremental',
    unique_key=['cod_pro', 'langue'],
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    post_hook=[
        "{% if is_incremental() %}DELETE FROM {{ this }} t WHERE NOT EXISTS (SELECT 1 FROM {{ source('ods', 'lldespro') }} s WHERE s.cod_pro = t.cod_pro AND s.langue = t.langue){% endif %}",
        "CREATE INDEX IF NOT EXISTS idx_lldespro_etl_source_timestamp ON {{ this }} USING btree (_etl_source_timestamp)",
        "CREATE UNIQUE INDEX IF NOT EXISTS lldespro_pkey ON {{ this }} USING btree (cod_pro, langue)",
        "ANALYZE {{ this }}"
    ]
) }}

/*
============================================================================
PREP MODEL : lldespro
============================================================================
Generated : 2025-12-21 05:00:35
Source    : ods.lldespro
Rows ODS  : 551,623
Cols ODS  : 9
Cols PREP : 7 (+ _prep_loaded_at)
Strategy  : INCREMENTAL
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
