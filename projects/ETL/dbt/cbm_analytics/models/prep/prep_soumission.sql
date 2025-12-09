{{ config(materialized='view') }}

/*
    ============================================================================
    Modèle PREP : soumission
    ============================================================================
    Généré automatiquement le 2025-12-05 15:38:14
    
    Source       : ods.soumission
    Lignes       : 176,053
    Colonnes ODS : 154
    Colonnes PREP: 16
    Exclues      : 138 (89.6%)
    
    Exclusions:
      - Techniques ETL  : 7
      - 100% NULL       : 118
      - Constantes      : 12
      - Faible valeur   : 1
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
    "qte" AS qte,
    "typ_con" AS typ_con,
    "qte_his" AS qte_his,
    "cat_tar" AS cat_tar,
    "no_lot" AS no_lot,
    "uniq_id" AS uniq_id,
    "dat_liv" AS dat_liv,
    "no_cde" AS no_cde,
    "_etl_run_id" AS _etl_run_id
FROM {{ source('ods', 'soumission') }}
