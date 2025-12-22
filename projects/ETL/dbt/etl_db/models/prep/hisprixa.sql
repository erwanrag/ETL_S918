{{ config(
    materialized='incremental',
    unique_key=['cod_fou', 'cod_pro', 'dat_px'],
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    post_hook=[
        "{% if is_incremental() %}DELETE FROM {{ this }} t WHERE NOT EXISTS (SELECT 1 FROM {{ source('ods', 'hisprixa') }} s WHERE s.cod_fou = t.cod_fou AND s.cod_pro = t.cod_pro AND s.dat_px = t.dat_px){% endif %}",
        "CREATE UNIQUE INDEX IF NOT EXISTS hisprixa_pkey ON {{ this }} USING btree (cod_fou, cod_pro, dat_px)",
        "CREATE INDEX IF NOT EXISTS idx_hisprixa_etl_source_timestamp ON {{ this }} USING btree (_etl_source_timestamp)",
        "ANALYZE {{ this }}"
    ]
) }}

/*
============================================================================
PREP MODEL : hisprixa
============================================================================
Generated : 2025-12-21 05:00:11
Source    : ods.hisprixa
Rows ODS  : 116,026
Cols ODS  : 48
Cols PREP : 41 (+ _prep_loaded_at)
Strategy  : INCREMENTAL
============================================================================
*/

SELECT
    "cod_pro" AS cod_pro,
    "cod_fou" AS cod_fou,
    "dat_px" AS dat_px,
    "px_refa" AS px_refa,
    "qte_1" AS qte_1,
    "qte_2" AS qte_2,
    "qte_3" AS qte_3,
    "qte_4" AS qte_4,
    "qte_5" AS qte_5,
    "px_ach_1" AS px_ach_1,
    "px_ach_2" AS px_ach_2,
    "px_ach_3" AS px_ach_3,
    "px_ach_4" AS px_ach_4,
    "px_ach_5" AS px_ach_5,
    "usr_mod" AS usr_mod,
    "dat_mod" AS dat_mod,
    "depot" AS depot,
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
    "px_ach_rq_1" AS px_ach_rq_1,
    "px_ach_rq_2" AS px_ach_rq_2,
    "px_ach_rq_3" AS px_ach_rq_3,
    "px_ach_rq_4" AS px_ach_rq_4,
    "px_ach_rq_5" AS px_ach_rq_5,
    "px_ach_rq_6" AS px_ach_rq_6,
    "px_ach_rq_7" AS px_ach_rq_7,
    "px_ach_rq_8" AS px_ach_rq_8,
    "px_ach_rq_9" AS px_ach_rq_9,
    "px_ach_rq_10" AS px_ach_rq_10,
    "hr_mod" AS hr_mod,
    "_etl_valid_from" AS _etl_source_timestamp,
    "_etl_run_id" AS _etl_run_id,
    CURRENT_TIMESTAMP AS _prep_loaded_at
FROM {{ source('ods', 'hisprixa') }}
{% if is_incremental() %}
WHERE "_etl_valid_from" > (
    SELECT COALESCE(MAX(_etl_source_timestamp), '1900-01-01'::timestamp)
    FROM {{ this }}
)
{% endif %}
