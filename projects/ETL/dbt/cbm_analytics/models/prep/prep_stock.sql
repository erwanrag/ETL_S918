{{ config(materialized='view') }}

/*
    ============================================================================
    Modèle PREP : stock
    ============================================================================
    Généré automatiquement le 2025-12-05 15:38:20
    
    Source       : ods.stock
    Lignes       : 278,916
    Colonnes ODS : 45
    Colonnes PREP: 26
    Exclues      : 19 (42.2%)
    
    Exclusions:
      - Techniques ETL  : 2
      - 100% NULL       : 2
      - Constantes      : 15
      - Faible valeur   : 0
    ============================================================================
*/

SELECT
    "cod_pro" AS cod_pro,
    "depot" AS depot,
    "magasin" AS magasin,
    "emplac" AS emplac,
    "stock" AS stock,
    "pmp" AS pmp,
    "arr_sit" AS arr_sit,
    "pmp_sit" AS pmp_sit,
    "qte_sit" AS qte_sit,
    "cum_ent" AS cum_ent,
    "cum_sor" AS cum_sor,
    "dat_ent" AS dat_ent,
    "dat_sor" AS dat_sor,
    "val_ent" AS val_ent,
    "val_sor" AS val_sor,
    "poids" AS poids,
    "poi_sit" AS poi_sit,
    "px_rvt" AS px_rvt,
    "nb_mvt" AS nb_mvt,
    "poids_net" AS poids_net,
    "poi_net_sit" AS poi_net_sit,
    "hr_ent" AS hr_ent,
    "hr_sor" AS hr_sor,
    "stk_mini" AS stk_mini,
    "stk_maxi" AS stk_maxi,
    "_etl_run_id" AS _etl_run_id
FROM {{ source('ods', 'stock') }}
