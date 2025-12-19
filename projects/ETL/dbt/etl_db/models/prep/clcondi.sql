{{ config(
    materialized='incremental',
    unique_key='uniq_id',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    post_hook=[
        "{% if is_incremental() %}DELETE FROM {{ this }} t WHERE NOT EXISTS (SELECT 1 FROM {{ source('ods', 'clcondi') }} s WHERE s.uniq_id = t.uniq_id){% endif %}",
        "CREATE UNIQUE INDEX IF NOT EXISTS clcondi_pkey ON {{ this }} USING btree (uniq_id)",
        "CREATE INDEX IF NOT EXISTS idx_clcondi_etl_source_timestamp ON {{ this }} USING btree (_etl_source_timestamp)",
        "ANALYZE {{ this }}"
    ]
) }}

/*
============================================================================
PREP MODEL : clcondi
============================================================================
Generated : 2025-12-15 16:41:17
Source    : ods.clcondi
Rows ODS  : 429,978
Cols ODS  : 161
Cols PREP : 64 (+ _prep_loaded_at)
Strategy  : INCREMENTAL
============================================================================
*/

SELECT
    "promo" AS promo,
    "cat_tar" AS cat_tar,
    "cod_cli" AS cod_cli,
    "cod_fou" AS cod_fou,
    "cod_pro" AS cod_pro,
    "dat_deb" AS dat_deb,
    "dat_fin" AS dat_fin,
    "no_tarif" AS no_tarif,
    "rem_app_1" AS rem_app_1,
    "rem_app_2" AS rem_app_2,
    "px_net_1" AS px_net_1,
    "px_net_2" AS px_net_2,
    "px_net_3" AS px_net_3,
    "px_net_4" AS px_net_4,
    "cod_dec1" AS cod_dec1,
    "cod_dec2" AS cod_dec2,
    "cod_dec3" AS cod_dec3,
    "cod_dec4" AS cod_dec4,
    "cod_dec5" AS cod_dec5,
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
    "px_vte_1" AS px_vte_1,
    "px_vte_2" AS px_vte_2,
    "px_vte_3" AS px_vte_3,
    "px_vte_4" AS px_vte_4,
    "px_vte_5" AS px_vte_5,
    "px_vte_6" AS px_vte_6,
    "px_vte_7" AS px_vte_7,
    "px_vte_8" AS px_vte_8,
    "px_vte_9" AS px_vte_9,
    "px_vte_10" AS px_vte_10,
    "px_poi" AS px_poi,
    "px_cli" AS px_cli,
    "depot" AS depot,
    "no_cond" AS no_cond,
    "usr_crt" AS usr_crt,
    "dat_crt" AS dat_crt,
    "usr_mod" AS usr_mod,
    "dat_mod" AS dat_mod,
    "typ_gsd" AS typ_gsd,
    "cod_dec" AS cod_dec,
    "px_brut" AS px_brut,
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
    "uniq_id" AS uniq_id,
    "_etl_valid_from" AS _etl_source_timestamp,
    "_etl_run_id" AS _etl_run_id,
    CURRENT_TIMESTAMP AS _prep_loaded_at
FROM {{ source('ods', 'clcondi') }}
{% if is_incremental() %}
WHERE "_etl_valid_from" > (
    SELECT COALESCE(MAX(_etl_source_timestamp), '1900-01-01'::timestamp)
    FROM {{ this }}
)
{% endif %}
