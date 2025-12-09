{{ config(materialized='view') }}

/*
    ============================================================================
    Modèle PREP : deppro
    ============================================================================
    Généré automatiquement le 2025-12-05 15:23:36
    
    Source       : ods.deppro
    Lignes       : 2,803,213
    Colonnes ODS : 106
    Colonnes PREP: 6
    Exclues      : 100 (94.3%)
    
    Exclusions:
      - Techniques ETL  : 7
      - 100% NULL       : 40
      - Constantes      : 53
      - Faible valeur   : 0
    ============================================================================
*/

SELECT
    "cod_pro" AS cod_pro,
    "depot" AS depot,
    "cod_fou" AS cod_fou,
    "principa" AS principa,
    "ach_ctr" AS ach_ctr,
    "_etl_run_id" AS _etl_run_id
FROM {{ source('ods', 'deppro') }}
