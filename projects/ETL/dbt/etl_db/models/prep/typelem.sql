{{ config(
    materialized='table',
) }}

/*
============================================================================
PREP MODEL : typelem
============================================================================
Generated : 2025-12-21 05:01:29
Source    : ods.typelem
Rows ODS  : 43
Cols ODS  : 153
Cols PREP : 32 (+ _prep_loaded_at)
Strategy  : TABLE
============================================================================
*/

SELECT
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
