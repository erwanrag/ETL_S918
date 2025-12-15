{{ config(
    materialized='table',
) }}

/*
    ============================================================================
    Modèle PREP : soumission
    ============================================================================
    Généré automatiquement le 2025-12-12 16:57:54
    
    Source       : ods.soumission
    Lignes       : 176,280
    Colonnes ODS : 154
    Colonnes PREP: 31  (+ _prep_loaded_at)
    Exclues      : 124 (80.5%)
    
    Stratégie    : TABLE
    Full Refresh: Oui
    Merge        : N/A
    Incremental  : Enabled (_etl_valid_from)
    Index        : 0 répliqué(s)
    
    Exclusions:
      - Techniques ETL  : 5
      - 100% NULL       : 47
      - Constantes      : 70
      - Faible valeur   : 2
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
    "depot" AS depot,
    "no_lot" AS no_lot,
    "uniq_id" AS uniq_id,
    "dat_liv" AS dat_liv,
    "no_cde" AS no_cde,
    "_etl_loaded_at" AS _etl_loaded_at,
    "_etl_run_id" AS _etl_run_id,
    "_etl_valid_from" AS _etl_source_timestamp,
    CURRENT_TIMESTAMP AS _prep_loaded_at
FROM {{ source('ods', 'soumission') }}
