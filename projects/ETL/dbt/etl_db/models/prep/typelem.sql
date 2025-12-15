{{ config(
    materialized='table',
) }}

/*
    ============================================================================
    Modèle PREP : typelem
    ============================================================================
    Généré automatiquement le 2025-12-12 16:58:09
    
    Source       : ods.typelem
    Lignes       : 43
    Colonnes ODS : 153
    Colonnes PREP: 32  (+ _prep_loaded_at)
    Exclues      : 122 (79.7%)
    
    Stratégie    : TABLE
    Full Refresh: Oui
    Merge        : N/A
    Incremental  : Enabled (_etl_valid_from)
    Index        : 0 répliqué(s)
    
    Exclusions:
      - Techniques ETL  : 5
      - 100% NULL       : 8
      - Constantes      : 109
      - Faible valeur   : 0
    ============================================================================
*/

SELECT
    "typ_fich" AS typ_fich,
    "typ_elem" AS typ_elem,
    "sous_type" AS sous_type,
    "lib_elem" AS lib_elem,
    "chap_util_1" AS chap_util_1,
    "chap_util_2" AS chap_util_2,
    "chap_util_3" AS chap_util_3,
    "chap_util_4" AS chap_util_4,
    "chap_util_5" AS chap_util_5,
    "chap_util_6" AS chap_util_6,
    "chap_util_7" AS chap_util_7,
    "chap_util_8" AS chap_util_8,
    "chap_util_9" AS chap_util_9,
    "chap_util_10" AS chap_util_10,
    "chap_util_11" AS chap_util_11,
    "chap_util_12" AS chap_util_12,
    "chap_util_13" AS chap_util_13,
    "chap_util_14" AS chap_util_14,
    "chap_util_15" AS chap_util_15,
    "chap_util_16" AS chap_util_16,
    "chap_util_17" AS chap_util_17,
    "stock" AS stock,
    "stat" AS stat,
    "nmc_com" AS nmc_com,
    "nmc_pro" AS nmc_pro,
    "commande" AS commande,
    "mt_mini" AS mt_mini,
    "contenant" AS contenant,
    "_etl_loaded_at" AS _etl_loaded_at,
    "_etl_run_id" AS _etl_run_id,
    "_etl_valid_from" AS _etl_source_timestamp,
    CURRENT_TIMESTAMP AS _prep_loaded_at
FROM {{ source('ods', 'typelem') }}
