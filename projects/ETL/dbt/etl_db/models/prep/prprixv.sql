{{ config(
    materialized='table',
) }}

/*
    ============================================================================
    Modèle PREP : prprixv
    ============================================================================
    Généré automatiquement le 2025-12-12 17:03:47
    
    Source       : ods.prprixv
    Lignes       : 5,013,186
    Colonnes ODS : 121
    Colonnes PREP: 48  (+ _prep_loaded_at)
    Exclues      : 74 (61.2%)
    
    Stratégie    : TABLE
    Full Refresh: Oui
    Merge        : N/A
    Incremental  : Enabled (_etl_valid_from)
    Index        : 0 répliqué(s)
    
    Exclusions:
      - Techniques ETL  : 5
      - 100% NULL       : 11
      - Constantes      : 58
      - Faible valeur   : 0
    ============================================================================
*/

SELECT
    "cod_pro" AS cod_pro,
    "no_tarif" AS no_tarif,
    "px_refv" AS px_refv,
    "coef_t2" AS coef_t2,
    "coef_t3" AS coef_t3,
    "coef_t4" AS coef_t4,
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
    "coef_rq_1" AS coef_rq_1,
    "coef_rq_2" AS coef_rq_2,
    "coef_rq_3" AS coef_rq_3,
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
