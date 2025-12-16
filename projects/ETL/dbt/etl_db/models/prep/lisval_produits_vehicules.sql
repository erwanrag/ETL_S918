{{ config(
    materialized='incremental',
    unique_key='uniq_id',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    post_hook=[
        "{% if is_incremental() %}DELETE FROM {{ this }} t WHERE NOT EXISTS (SELECT 1 FROM {{ source('ods', 'lisval_produits_vehicules') }} s WHERE s.uniq_id = t.uniq_id){% endif %}",
        "CREATE INDEX IF NOT EXISTS idx_lisval_produits_vehicules_etl_source_timestamp ON {{ this }} USING btree (_etl_source_timestamp)",
        "CREATE UNIQUE INDEX IF NOT EXISTS lisval_produits_vehicules_pkey ON {{ this }} USING btree (uniq_id)",
        "ANALYZE {{ this }}"
    ]
) }}

/*
============================================================================
PREP MODEL : lisval_produits_vehicules
============================================================================
Generated : 2025-12-15 16:41:55
Source    : ods.lisval_produits_vehicules
Rows ODS  : 180,752
Cols ODS  : 80
Cols PREP : 12 (+ _prep_loaded_at)
Strategy  : INCREMENTAL
============================================================================
*/

SELECT
    "typ_fich" AS typ_fich,
    "liste" AS liste,
    "cod_tiers" AS cod_tiers,
    "zt0" AS zt0,
    "zt1" AS zt1,
    "ze0" AS ze0,
    "no_ordre" AS no_ordre,
    "cod_autre" AS cod_autre,
    "uniq_id" AS uniq_id,
    "_etl_valid_from" AS _etl_source_timestamp,
    "_etl_run_id" AS _etl_run_id,
    CURRENT_TIMESTAMP AS _prep_loaded_at
FROM {{ source('ods', 'lisval_produits_vehicules') }}
{% if is_incremental() %}
WHERE "_etl_valid_from" > (
    SELECT COALESCE(MAX(_etl_source_timestamp), '1900-01-01'::timestamp)
    FROM {{ this }}
)
{% endif %}
