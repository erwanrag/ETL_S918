{{ config(materialized='view') }}

/*
    ============================================================================
    Modèle PREP : prprixv
    ============================================================================
    Généré automatiquement le 2025-12-05 15:38:03
    
    Source       : ods.prprixv
    Lignes       : 4,994,280
    Colonnes ODS : 121
    Colonnes PREP: 8
    Exclues      : 113 (93.4%)
    
    Exclusions:
      - Techniques ETL  : 7
      - 100% NULL       : 91
      - Constantes      : 12
      - Faible valeur   : 3
    ============================================================================
*/

SELECT
    "cod_pro" AS cod_pro,
    "no_tarif" AS no_tarif,
    "px_refv" AS px_refv,
    "fpx_refv" AS fpx_refv,
    "px_mini" AS px_mini,
    "dat_fpxv" AS dat_fpxv,
    "cod_cli" AS cod_cli,
    "_etl_run_id" AS _etl_run_id
FROM {{ source('ods', 'prprixv') }}
