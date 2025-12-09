{{ config(materialized='view') }}

/*
    ============================================================================
    Modèle PREP : lisval_slimstock
    ============================================================================
    Généré automatiquement le 2025-12-05 15:34:11
    
    Source       : ods.lisval_slimstock
    Lignes       : 194,493
    Colonnes ODS : 80
    Colonnes PREP: 6
    Exclues      : 74 (92.5%)
    
    Exclusions:
      - Techniques ETL  : 2
      - 100% NULL       : 34
      - Constantes      : 38
      - Faible valeur   : 0
    ============================================================================
*/

SELECT
    "cod_tiers" AS cod_tiers,
    "za0" AS za0,
    "zl0" AS zl0,
    "zt0" AS zt0,
    "uniq_id" AS uniq_id,
    "_etl_run_id" AS _etl_run_id
FROM {{ source('ods', 'lisval_slimstock') }}
