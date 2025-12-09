{{ config(materialized='view') }}

/*
    ============================================================================
    Modèle PREP : lisval_produits_vehicules
    ============================================================================
    Généré automatiquement le 2025-12-05 15:34:04
    
    Source       : ods.lisval_produits_vehicules
    Lignes       : 180,609
    Colonnes ODS : 80
    Colonnes PREP: 7
    Exclues      : 73 (91.2%)
    
    Exclusions:
      - Techniques ETL  : 2
      - 100% NULL       : 33
      - Constantes      : 36
      - Faible valeur   : 2
    ============================================================================
*/

SELECT
    "cod_tiers" AS cod_tiers,
    "zd0" AS zd0,
    "zt0" AS zt0,
    "zt1" AS zt1,
    "ze0" AS ze0,
    "uniq_id" AS uniq_id,
    "_etl_run_id" AS _etl_run_id
FROM {{ source('ods', 'lisval_produits_vehicules') }}
