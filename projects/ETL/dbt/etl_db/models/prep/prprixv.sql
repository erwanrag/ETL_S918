{{ config(
    materialized='incremental',
    unique_key=['cod_pro', 'no_tarif'],
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    post_hook=[
        "{% if is_incremental() %}DELETE FROM {{ this }} t WHERE NOT EXISTS (SELECT 1 FROM {{ source('ods', 'prprixv') }} s WHERE s.cod_pro = t.cod_pro AND s.no_tarif = t.no_tarif){% endif %}",
        "CREATE UNIQUE INDEX IF NOT EXISTS prprixv_pkey ON {{ this }} USING btree (cod_pro, no_tarif)",
        "ANALYZE {{ this }}"
    ]
) }}

/*
============================================================================
PREP MODEL : prprixv
============================================================================
Generated : 2025-12-15 16:44:14
Source    : ods.prprixv
Rows ODS  : 5,020,141
Cols ODS  : 121
Cols PREP : 43 (+ _prep_loaded_at)
Strategy  : INCREMENTAL
============================================================================
*/

SELECT
    "cod_pro" AS cod_pro,
    "no_tarif" AS no_tarif,
    "px_refv" AS px_refv,
    "coef_t2" AS coef_t2,
    "fpx_refv" AS fpx_refv,
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
    "px_mini" AS px_mini,
    "dat_fpxv" AS dat_fpxv,
    "cod_cli" AS cod_cli,
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
    "cod_rvt_vte" AS cod_rvt_vte,
    "_etl_loaded_at" AS _etl_loaded_at,
    "_etl_run_id" AS _etl_run_id,
    "_etl_valid_from" AS _etl_source_timestamp,
    CURRENT_TIMESTAMP AS _prep_loaded_at
FROM {{ source('ods', 'prprixv') }}
{% if is_incremental() %}
WHERE "_etl_valid_from" > (
    SELECT COALESCE(MAX(_etl_source_timestamp), '1900-01-01'::timestamp)
    FROM {{ this }}
)
{% endif %}
