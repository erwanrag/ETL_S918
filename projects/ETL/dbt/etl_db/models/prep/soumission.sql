{{ config(
    materialized='incremental',
    unique_key='uniq_id',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    post_hook=[
        "{% if is_incremental() %}DELETE FROM {{ this }} t WHERE NOT EXISTS (SELECT 1 FROM {{ source('ods', 'soumission') }} s WHERE s.uniq_id = t.uniq_id){% endif %}",
        "CREATE UNIQUE INDEX IF NOT EXISTS soumission_pkey ON {{ this }} USING btree (uniq_id)",
        "ANALYZE {{ this }}"
    ]
) }}

/*
============================================================================
PREP MODEL : soumission
============================================================================
Generated : 2025-12-15 16:43:55
Source    : ods.soumission
Rows ODS  : 176,290
Cols ODS  : 154
Cols PREP : 30 (+ _prep_loaded_at)
Strategy  : INCREMENTAL
============================================================================
*/

SELECT
    "no_contrat" AS no_contrat,
    "lib_contrat" AS lib_contrat,
    "vente" AS vente,
    "cod_tiers" AS cod_tiers,
    "dat_deb" AS dat_deb,
    "dat_fin" AS dat_fin,
    "cod_pro" AS cod_pro,
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
    "qte" AS qte,
    "typ_con" AS typ_con,
    "qte_his" AS qte_his,
    "cat_tar" AS cat_tar,
    "no_lot" AS no_lot,
    "uniq_id" AS uniq_id,
    "dat_liv" AS dat_liv,
    "no_cde" AS no_cde,
    "_etl_loaded_at" AS _etl_loaded_at,
    "_etl_run_id" AS _etl_run_id,
    "_etl_valid_from" AS _etl_source_timestamp,
    CURRENT_TIMESTAMP AS _prep_loaded_at
FROM {{ source('ods', 'soumission') }}
{% if is_incremental() %}
WHERE "_etl_valid_from" > (
    SELECT COALESCE(MAX(_etl_source_timestamp), '1900-01-01'::timestamp)
    FROM {{ this }}
)
{% endif %}
