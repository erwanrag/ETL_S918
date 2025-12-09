{{ config(materialized='view') }}

/*
    ============================================================================
    Modèle PREP : lisval_produits_lies
    ============================================================================
    Généré automatiquement le 2025-12-05 15:33:58
    
    Source       : ods.lisval_produits_lies
    Lignes       : 4,452
    Colonnes ODS : 80
    Colonnes PREP: 5
    Exclues      : 75 (93.8%)
    
    Exclusions:
      - Techniques ETL  : 2
      - 100% NULL       : 35
      - Constantes      : 38
      - Faible valeur   : 0
    ============================================================================
*/

SELECT
    "cod_tiers" AS cod_tiers,
    "zt0" AS zt0,
    "ze0" AS ze0,
    "uniq_id" AS uniq_id,
    "_etl_run_id" AS _etl_run_id
FROM {{ source('ods', 'lisval_produits_lies') }}
