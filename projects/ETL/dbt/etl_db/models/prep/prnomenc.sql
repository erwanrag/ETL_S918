{{ config(
    materialized='incremental',
    unique_key=['cod_nmc', 'cod_pro', 'depot', 'ordre', 'type_nmc'],
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    post_hook=[
        "{% if is_incremental() %}DELETE FROM {{ this }} t WHERE NOT EXISTS (SELECT 1 FROM {{ source('ods', 'prnomenc') }} s WHERE s.cod_nmc = t.cod_nmc AND s.cod_pro = t.cod_pro AND s.depot = t.depot AND s.ordre = t.ordre AND s.type_nmc = t.type_nmc){% endif %}",
        "CREATE UNIQUE INDEX IF NOT EXISTS prnomenc_pkey ON {{ this }} USING btree (cod_nmc, cod_pro, depot, ordre, type_nmc)",
        "ANALYZE {{ this }}"
    ]
) }}

/*
============================================================================
PREP MODEL : prnomenc
============================================================================
Generated : 2025-12-15 16:42:02
Source    : ods.prnomenc
Rows ODS  : 18,944
Cols ODS  : 137
Cols PREP : 29 (+ _prep_loaded_at)
Strategy  : INCREMENTAL
============================================================================
*/

SELECT
    "cod_pro" AS cod_pro,
    "type_nmc" AS type_nmc,
    "ordre" AS ordre,
    "quantite" AS quantite,
    "cod_nmc" AS cod_nmc,
    "niveau" AS niveau,
    "editer" AS editer,
    "sor_comp" AS sor_comp,
    "cod_dec1" AS cod_dec1,
    "cod_dec2" AS cod_dec2,
    "cod_dec3" AS cod_dec3,
    "cod_dec4" AS cod_dec4,
    "cod_dec5" AS cod_dec5,
    "depot" AS depot,
    "condition" AS condition,
    "dat_app" AS dat_app,
    "dat_fin" AS dat_fin,
    "qte_avc" AS qte_avc,
    "qte_pf" AS qte_pf,
    "no_page" AS no_page,
    "date_deb" AS date_deb,
    "date_fin" AS date_fin,
    "no_info" AS no_info,
    "nb_cs3" AS nb_cs3,
    "cod_fou" AS cod_fou,
    "_etl_loaded_at" AS _etl_loaded_at,
    "_etl_run_id" AS _etl_run_id,
    "_etl_valid_from" AS _etl_source_timestamp,
    CURRENT_TIMESTAMP AS _prep_loaded_at
FROM {{ source('ods', 'prnomenc') }}
{% if is_incremental() %}
WHERE "_etl_valid_from" > (
    SELECT COALESCE(MAX(_etl_source_timestamp), '1900-01-01'::timestamp)
    FROM {{ this }}
)
{% endif %}
