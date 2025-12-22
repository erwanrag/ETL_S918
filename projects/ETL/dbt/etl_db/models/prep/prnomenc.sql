{{ config(
    materialized='table',
) }}

/*
============================================================================
PREP MODEL : prnomenc
============================================================================
Generated : 2025-12-21 05:00:36
Source    : ods.prnomenc
Rows ODS  : 18,964
Cols ODS  : 137
Cols PREP : 29 (+ _prep_loaded_at)
Strategy  : TABLE
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
