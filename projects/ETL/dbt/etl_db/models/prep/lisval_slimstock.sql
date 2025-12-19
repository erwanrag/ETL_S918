{{ config(
    materialized='incremental',
    unique_key='uniq_id',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    post_hook=[
        "{% if is_incremental() %}DELETE FROM {{ this }} t WHERE NOT EXISTS (SELECT 1 FROM {{ source('ods', 'lisval_slimstock') }} s WHERE s.uniq_id = t.uniq_id){% endif %}",
        "CREATE INDEX IF NOT EXISTS idx_lisval_slimstock_etl_source_timestamp ON {{ this }} USING btree (_etl_source_timestamp)",
        "CREATE UNIQUE INDEX IF NOT EXISTS lisval_slimstock_pkey ON {{ this }} USING btree (uniq_id)",
        "ANALYZE {{ this }}"
    ]
) }}

/*
============================================================================
PREP MODEL : lisval_slimstock
============================================================================
Generated : 2025-12-15 16:42:00
Source    : ods.lisval_slimstock
Rows ODS  : 195,900
Cols ODS  : 80
Cols PREP : 10 (+ _prep_loaded_at)
Strategy  : INCREMENTAL
============================================================================
*/

SELECT
    "cod_tiers" AS cod_tiers,
    "za0" AS za0,
    "zl0" AS zl0,
    "zt0" AS zt0,
    "no_ordre" AS no_ordre,
    "cod_autre" AS cod_autre,
    "uniq_id" AS uniq_id,
    "_etl_valid_from" AS _etl_source_timestamp,
    "_etl_run_id" AS _etl_run_id,
    CURRENT_TIMESTAMP AS _prep_loaded_at
FROM {{ source('ods', 'lisval_slimstock') }}
{% if is_incremental() %}
WHERE "_etl_valid_from" > (
    SELECT COALESCE(MAX(_etl_source_timestamp), '1900-01-01'::timestamp)
    FROM {{ this }}
)
{% endif %}
