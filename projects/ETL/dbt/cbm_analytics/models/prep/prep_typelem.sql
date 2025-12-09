{{ config(materialized='view') }}

/*
    ============================================================================
    Modèle PREP : typelem
    ============================================================================
    Généré automatiquement le 2025-12-05 15:38:21
    
    Source       : ods.typelem
    Lignes       : 43
    Colonnes ODS : 153
    Colonnes PREP: 11
    Exclues      : 142 (92.8%)
    
    Exclusions:
      - Techniques ETL  : 7
      - 100% NULL       : 107
      - Constantes      : 28
      - Faible valeur   : 0
    ============================================================================
*/

SELECT
    "typ_fich" AS typ_fich,
    "typ_elem" AS typ_elem,
    "sous_type" AS sous_type,
    "lib_elem" AS lib_elem,
    "stock" AS stock,
    "stat" AS stat,
    "nmc_com" AS nmc_com,
    "nmc_pro" AS nmc_pro,
    "commande" AS commande,
    "contenant" AS contenant,
    "_etl_run_id" AS _etl_run_id
FROM {{ source('ods', 'typelem') }}
