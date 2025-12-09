{{ config(materialized='view') }}

/*
    ============================================================================
    Modèle PREP : lisval_fou_production
    ============================================================================
    Généré automatiquement le 2025-12-05 15:33:58
    
    Source       : ods.lisval_fou_production
    Lignes       : 5
    Colonnes ODS : 80
    Colonnes PREP: 4
    Exclues      : 76 (95.0%)
    
    Exclusions:
      - Techniques ETL  : 2
      - 100% NULL       : 35
      - Constantes      : 39
      - Faible valeur   : 0
    ============================================================================
*/

SELECT
    "cod_tiers" AS cod_tiers,
    "zt0" AS zt0,
    "uniq_id" AS uniq_id,
    "_etl_run_id" AS _etl_run_id
FROM {{ source('ods', 'lisval_fou_production') }}
