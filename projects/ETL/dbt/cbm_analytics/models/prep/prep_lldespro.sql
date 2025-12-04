{{ config(materialized='view') }}

/*
    Modèle PREP pour lldespro
    Généré automatiquement depuis ODS pour inclure les colonnes éclataées (Extent)
*/

SELECT
    "cod_pro" AS cod_pro,
    "langue" AS langue,
    "des_lan" AS des_lan,
    "TabPart_lldespro" AS tabpart_lldespro,
    "flag_repli" AS flag_repli,
    "des_lan2" AS des_lan2,
    "_etl_loaded_at" AS _etl_loaded_at,
    "_etl_updated_at" AS _etl_updated_at,
    "_etl_run_id" AS _etl_run_id,
    "_etl_source" AS _etl_source,
    "_etl_hashdiff" AS _etl_hashdiff,
    "_etl_is_active" AS _etl_is_active,
    "_etl_valid_from" AS _etl_valid_from,
    "_etl_valid_to" AS _etl_valid_to
FROM {{ source('ods', 'lldespro') }}
