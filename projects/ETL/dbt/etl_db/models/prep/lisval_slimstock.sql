{{ config(
    materialized='table',
) }}

/*
============================================================================
PREP MODEL : lisval_slimstock
============================================================================
Generated : 2025-12-21 05:00:35
Source    : ods.lisval_slimstock
Rows ODS  : 197,231
Cols ODS  : 80
Cols PREP : 10 (+ _prep_loaded_at)
Strategy  : TABLE
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
