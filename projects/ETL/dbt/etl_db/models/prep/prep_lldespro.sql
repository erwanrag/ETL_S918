{{ config(materialized='view') }}

/*
    ============================================================================
    Modèle PREP : lldespro
    ============================================================================
    Généré automatiquement le 2025-12-05 15:34:14
    
    Source       : ods.lldespro
    Lignes       : 551,535
    Colonnes ODS : 9
    Colonnes PREP: 5
    Exclues      : 4 (44.4%)
    
    Exclusions:
      - Techniques ETL  : 2
      - 100% NULL       : 1
      - Constantes      : 1
      - Faible valeur   : 0
    ============================================================================
*/

SELECT
    "cod_pro" AS cod_pro,
    "langue" AS langue,
    "des_lan" AS des_lan,
    "des_lan2" AS des_lan2,
    "_etl_run_id" AS _etl_run_id
FROM {{ source('ods', 'lldespro') }}
