{{ config(materialized='view') }}

/*
    ============================================================================
    Modèle PREP : prnomenc
    ============================================================================
    Généré automatiquement le 2025-12-05 15:34:15
    
    Source       : ods.prnomenc
    Lignes       : 18,877
    Colonnes ODS : 137
    Colonnes PREP: 13
    Exclues      : 124 (90.5%)
    
    Exclusions:
      - Techniques ETL  : 7
      - 100% NULL       : 71
      - Constantes      : 44
      - Faible valeur   : 2
    ============================================================================
*/

SELECT
    "cod_pro" AS cod_pro,
    "type_nmc" AS type_nmc,
    "ordre" AS ordre,
    "quantite" AS quantite,
    "cod_nmc" AS cod_nmc,
    "niveau" AS niveau,
    "editer" AS editer,
    "sor_comp" AS sor_comp,
    "depot" AS depot,
    "dat_fin" AS dat_fin,
    "qte_pf" AS qte_pf,
    "cod_fou" AS cod_fou,
    "_etl_run_id" AS _etl_run_id
FROM {{ source('ods', 'prnomenc') }}
