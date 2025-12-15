{{ config(
    materialized='table',
) }}

/*
    ============================================================================
    Modèle PREP : stock
    ============================================================================
    Généré automatiquement le 2025-12-12 16:58:06
    
    Source       : ods.stock
    Lignes       : 279,154
    Colonnes ODS : 45
    Colonnes PREP: 32  (+ _prep_loaded_at)
    Exclues      : 14 (31.1%)
    
    Stratégie    : TABLE
    Full Refresh: Oui
    Merge        : N/A
    Incremental  : Enabled (_etl_valid_from)
    Index        : 0 répliqué(s)
    
    Exclusions:
      - Techniques ETL  : 1
      - 100% NULL       : 2
      - Constantes      : 11
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
    "px_std" AS px_std,
    "poids_net" AS poids_net,
    "poi_net_sit" AS poi_net_sit,
    "px_std2" AS px_std2,
    "px_std3" AS px_std3,
    "px_sim" AS px_sim,
    "hr_ent" AS hr_ent,
    "hr_sor" AS hr_sor,
    "stk_mini" AS stk_mini,
    "stk_maxi" AS stk_maxi,
    "dat_cal" AS dat_cal,
    "_etl_valid_from" AS _etl_source_timestamp,
    "_etl_run_id" AS _etl_run_id,
    CURRENT_TIMESTAMP AS _prep_loaded_at
FROM {{ source('ods', 'stock') }}
