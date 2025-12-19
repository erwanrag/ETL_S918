{{ config(
    materialized='incremental',
    unique_key=['cod_pro', 'depot'],
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    post_hook=[
        "{% if is_incremental() %}DELETE FROM {{ this }} t WHERE NOT EXISTS (SELECT 1 FROM {{ source('ods', 'stock') }} s WHERE s.cod_pro = t.cod_pro AND s.depot = t.depot){% endif %}",
        "CREATE UNIQUE INDEX IF NOT EXISTS stock_pkey ON {{ this }} USING btree (cod_pro, depot)",
        "ANALYZE {{ this }}"
    ]
) }}

/*
============================================================================
PREP MODEL : stock
============================================================================
Generated : 2025-12-15 16:44:00
Source    : ods.stock
Rows ODS  : 279,198
Cols ODS  : 45
Cols PREP : 32 (+ _prep_loaded_at)
Strategy  : INCREMENTAL
============================================================================
*/

SELECT
    "cod_pro" AS cod_pro,
    "depot" AS depot,
    "magasin" AS magasin,
    "emplac" AS emplac,
    "stock" AS stock,
    "pmp" AS pmp,
    "pmp_sit" AS pmp_sit,
    "qte_sit" AS qte_sit,
    "cum_ent" AS cum_ent,
    "cum_sor" AS cum_sor,
    "dat_ent" AS dat_ent,
    "dat_sor" AS dat_sor,
    "val_ent" AS val_ent,
    "val_sor" AS val_sor,
    "poids" AS poids,
    "poi_sit" AS poi_sit,
    "px_rvt" AS px_rvt,
    "nb_mvt" AS nb_mvt,
    "px_std" AS px_std,
    "poids_net" AS poids_net,
    "poi_net_sit" AS poi_net_sit,
    "px_std2" AS px_std2,
    "px_std3" AS px_std3,
    "px_sim" AS px_sim,
    "hr_ent" AS hr_ent,
    "hr_sor" AS hr_sor,
    "stk_mini" AS stk_mini,
    "stk_maxi" AS stk_maxi,
    "dat_cal" AS dat_cal,
    "_etl_valid_from" AS _etl_source_timestamp,
    "_etl_run_id" AS _etl_run_id,
    CURRENT_TIMESTAMP AS _prep_loaded_at
FROM {{ source('ods', 'stock') }}
{% if is_incremental() %}
WHERE "_etl_valid_from" > (
    SELECT COALESCE(MAX(_etl_source_timestamp), '1900-01-01'::timestamp)
    FROM {{ this }}
)
{% endif %}
