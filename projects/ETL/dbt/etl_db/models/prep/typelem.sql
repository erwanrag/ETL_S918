{{ config(
    materialized='incremental',
    unique_key=['ndos', 'typ_fich', 'typ_elem'],
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    post_hook=[
        "{% if is_incremental() %}DELETE FROM {{ this }} t WHERE NOT EXISTS (SELECT 1 FROM {{ source('ods', 'typelem') }} s WHERE s.ndos = t.ndos AND s.typ_fich = t.typ_fich AND s.typ_elem = t.typ_elem){% endif %}",
        "CREATE UNIQUE INDEX IF NOT EXISTS typelem_pkey ON {{ this }} USING btree (ndos, typ_fich, typ_elem)",
        "ANALYZE {{ this }}"
    ]
) }}

/*
============================================================================
PREP MODEL : typelem
============================================================================
Generated : 2025-12-15 16:44:02
Source    : ods.typelem
Rows ODS  : 43
Cols ODS  : 153
Cols PREP : 33 (+ _prep_loaded_at)
Strategy  : INCREMENTAL
============================================================================
*/

SELECT
    "ndos" AS ndos,
    "typ_fich" AS typ_fich,
    "typ_elem" AS typ_elem,
    "sous_type" AS sous_type,
    "lib_elem" AS lib_elem,
    "chap_util_1" AS chap_util_1,
    "chap_util_2" AS chap_util_2,
    "chap_util_3" AS chap_util_3,
    "chap_util_4" AS chap_util_4,
    "chap_util_5" AS chap_util_5,
    "chap_util_6" AS chap_util_6,
    "chap_util_7" AS chap_util_7,
    "chap_util_8" AS chap_util_8,
    "chap_util_9" AS chap_util_9,
    "chap_util_10" AS chap_util_10,
    "chap_util_11" AS chap_util_11,
    "chap_util_12" AS chap_util_12,
    "chap_util_13" AS chap_util_13,
    "chap_util_14" AS chap_util_14,
    "chap_util_15" AS chap_util_15,
    "chap_util_16" AS chap_util_16,
    "chap_util_17" AS chap_util_17,
    "stock" AS stock,
    "stat" AS stat,
    "nmc_com" AS nmc_com,
    "nmc_pro" AS nmc_pro,
    "commande" AS commande,
    "mt_mini" AS mt_mini,
    "contenant" AS contenant,
    "_etl_loaded_at" AS _etl_loaded_at,
    "_etl_run_id" AS _etl_run_id,
    "_etl_valid_from" AS _etl_source_timestamp,
    CURRENT_TIMESTAMP AS _prep_loaded_at
FROM {{ source('ods', 'typelem') }}
{% if is_incremental() %}
WHERE "_etl_valid_from" > (
    SELECT COALESCE(MAX(_etl_source_timestamp), '1900-01-01'::timestamp)
    FROM {{ this }}
)
{% endif %}
