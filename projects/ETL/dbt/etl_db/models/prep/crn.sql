{{ config(
    materialized='incremental',
    unique_key='cod_crn',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    post_hook=[
        "{% if is_incremental() %}DELETE FROM {{ this }} t WHERE NOT EXISTS (SELECT 1 FROM {{ source('ods', 'crn') }} s WHERE s.cod_crn = t.cod_crn){% endif %}",
        "CREATE UNIQUE INDEX IF NOT EXISTS crn_pkey ON {{ this }} USING btree (cod_crn)",
        "CREATE INDEX IF NOT EXISTS idx_crn_etl_source_timestamp ON {{ this }} USING btree (_etl_source_timestamp)",
        "ANALYZE {{ this }}"
    ]
) }}

/*
============================================================================
PREP MODEL : crn
============================================================================
Generated : 2025-12-21 04:59:59
Source    : ods.crn
Rows ODS  : 836
Cols ODS  : 56
Cols PREP : 15 (+ _prep_loaded_at)
Strategy  : INCREMENTAL
============================================================================
*/

SELECT
    "cod_crn" AS cod_crn,
    "nom_crn" AS nom_crn,
    "usr_crt" AS usr_crt,
    "dat_crt" AS dat_crt,
    "usr_mod" AS usr_mod,
    "dat_mod" AS dat_mod,
    "zta_1" AS zta_1,
    "zta_2" AS zta_2,
    "zta_3" AS zta_3,
    "num_tel" AS num_tel,
    "num_fax" AS num_fax,
    "no_info" AS no_info,
    "_etl_valid_from" AS _etl_source_timestamp,
    "_etl_run_id" AS _etl_run_id,
    CURRENT_TIMESTAMP AS _prep_loaded_at
FROM {{ source('ods', 'crn') }}
{% if is_incremental() %}
WHERE "_etl_valid_from" > (
    SELECT COALESCE(MAX(_etl_source_timestamp), '1900-01-01'::timestamp)
    FROM {{ this }}
)
{% endif %}
