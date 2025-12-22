{{ config(
    materialized='incremental',
    unique_key='_etl_hashdiff',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    post_hook=[
        "{% if is_incremental() %}DELETE FROM {{ this }} t WHERE NOT EXISTS (SELECT 1 FROM {{ source('ods', 'focondi') }} s WHERE s._etl_hashdiff = t._etl_hashdiff){% endif %}",
        "CREATE UNIQUE INDEX IF NOT EXISTS focondi_pkey ON {{ this }} USING btree (_etl_hashdiff)",
        "CREATE INDEX IF NOT EXISTS idx_focondi_etl_source_timestamp ON {{ this }} USING btree (_etl_source_timestamp)",
        "ANALYZE {{ this }}"
    ]
) }}

/*
============================================================================
PREP MODEL : focondi
============================================================================
Generated : 2025-12-21 05:00:09
Source    : ods.focondi
Rows ODS  : 103,999
Cols ODS  : 122
Cols PREP : 76 (+ _prep_loaded_at)
Strategy  : INCREMENTAL
============================================================================
*/

SELECT
    "promo" AS promo,
    "cod_fou" AS cod_fou,
    "cod_pro" AS cod_pro,
    "dat_deb" AS dat_deb,
    "dat_fin" AS dat_fin,
    "coef_vte_1" AS coef_vte_1,
    "coef_vte_2" AS coef_vte_2,
    "coef_vte_3" AS coef_vte_3,
    "coef_vte_4" AS coef_vte_4,
    "px_refv_1" AS px_refv_1,
    "px_refv_2" AS px_refv_2,
    "px_refv_3" AS px_refv_3,
    "px_refv_4" AS px_refv_4,
    "px_refa_1" AS px_refa_1,
    "px_refa_2" AS px_refa_2,
    "px_refa_3" AS px_refa_3,
    "px_refa_4" AS px_refa_4,
    "condi_a" AS condi_a,
    "c_promo" AS c_promo,
    "sf_tar" AS sf_tar,
    "s3_tar" AS s3_tar,
    "s4_tar" AS s4_tar,
    "remise1_1" AS remise1_1,
    "remise1_2" AS remise1_2,
    "remise1_3" AS remise1_3,
    "remise1_4" AS remise1_4,
    "remise2_1" AS remise2_1,
    "depot" AS depot,
    "qte_1" AS qte_1,
    "qte_2" AS qte_2,
    "qte_3" AS qte_3,
    "qte_4" AS qte_4,
    "qte_5" AS qte_5,
    "qte_6" AS qte_6,
    "qte_7" AS qte_7,
    "qte_8" AS qte_8,
    "qte_9" AS qte_9,
    "qte_10" AS qte_10,
    "px_ach_1" AS px_ach_1,
    "px_ach_2" AS px_ach_2,
    "px_ach_3" AS px_ach_3,
    "px_ach_4" AS px_ach_4,
    "px_ach_5" AS px_ach_5,
    "px_ach_6" AS px_ach_6,
    "px_ach_7" AS px_ach_7,
    "px_ach_8" AS px_ach_8,
    "px_ach_9" AS px_ach_9,
    "px_ach_10" AS px_ach_10,
    "usr_crt" AS usr_crt,
    "dat_crt" AS dat_crt,
    "usr_mod" AS usr_mod,
    "dat_mod" AS dat_mod,
    "no_cond" AS no_cond,
    "cod_dec1" AS cod_dec1,
    "cod_dec2" AS cod_dec2,
    "cod_dec3" AS cod_dec3,
    "cod_dec4" AS cod_dec4,
    "cod_dec5" AS cod_dec5,
    "rem_app_1" AS rem_app_1,
    "rem_app_2" AS rem_app_2,
    "rem_app_3" AS rem_app_3,
    "rem_app_4" AS rem_app_4,
    "qte_rq_1" AS qte_rq_1,
    "qte_rq_2" AS qte_rq_2,
    "qte_rq_3" AS qte_rq_3,
    "qte_rq_4" AS qte_rq_4,
    "qte_rq_5" AS qte_rq_5,
    "qte_rq_6" AS qte_rq_6,
    "qte_rq_7" AS qte_rq_7,
    "qte_rq_8" AS qte_rq_8,
    "qte_rq_9" AS qte_rq_9,
    "qte_rq_10" AS qte_rq_10,
    "_etl_hashdiff" AS _etl_hashdiff,
    "_etl_valid_from" AS _etl_source_timestamp,
    "_etl_run_id" AS _etl_run_id,
    CURRENT_TIMESTAMP AS _prep_loaded_at
FROM {{ source('ods', 'focondi') }}
{% if is_incremental() %}
WHERE "_etl_valid_from" > (
    SELECT COALESCE(MAX(_etl_source_timestamp), '1900-01-01'::timestamp)
    FROM {{ this }}
)
{% endif %}
