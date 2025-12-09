{{ config(materialized='view') }}

/*
    ============================================================================
    Modèle PREP : hisprixa
    ============================================================================
    Généré automatiquement le 2025-12-05 15:23:49
    
    Source       : ods.hisprixa
    Lignes       : 114,751
    Colonnes ODS : 53
    Colonnes PREP: 8
    Exclues      : 45 (84.9%)
    
    Exclusions:
      - Techniques ETL  : 7
      - 100% NULL       : 36
      - Constantes      : 1
      - Faible valeur   : 1
    ============================================================================
*/

SELECT
    "cod_pro" AS cod_pro,
    "cod_fou" AS cod_fou,
    "dat_px" AS dat_px,
    "px_refa" AS px_refa,
    "usr_mod" AS usr_mod,
    "dat_mod" AS dat_mod,
    "hr_mod" AS hr_mod,
    "_etl_run_id" AS _etl_run_id
FROM {{ source('ods', 'hisprixa') }}
