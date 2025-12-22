{{ config(
    materialized='table',
) }}

/*
============================================================================
PREP MODEL : lisval_produits_lies
============================================================================
Generated : 2025-12-21 05:00:31
Source    : ods.lisval_produits_lies
Rows ODS  : 4,487
Cols ODS  : 80
Cols PREP : 9 (+ _prep_loaded_at)
Strategy  : TABLE
============================================================================
*/

SELECT
    "cod_tiers" AS cod_tiers,
    "zt0" AS zt0,
    "ze0" AS ze0,
    "no_ordre" AS no_ordre,
    "cod_autre" AS cod_autre,
    "uniq_id" AS uniq_id,
    "_etl_valid_from" AS _etl_source_timestamp,
    "_etl_run_id" AS _etl_run_id,
    CURRENT_TIMESTAMP AS _prep_loaded_at
FROM {{ source('ods', 'lisval_produits_lies') }}
