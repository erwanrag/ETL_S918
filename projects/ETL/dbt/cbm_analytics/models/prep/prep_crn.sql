{{ config(materialized='view') }}

/*
    ============================================================================
    Modèle PREP : crn
    ============================================================================
    Généré automatiquement le 2025-12-05 15:19:49
    
    Source       : ods.crn
    Lignes       : 836
    Colonnes ODS : 61
    Colonnes PREP: 7
    Exclues      : 54 (88.5%)
    
    Exclusions:
      - Techniques ETL  : 7
      - 100% NULL       : 39
      - Constantes      : 8
      - Faible valeur   : 0
    ============================================================================
*/

SELECT
    "cod_crn" AS cod_crn,
    "nom_crn" AS nom_crn,
    "usr_crt" AS usr_crt,
    "dat_crt" AS dat_crt,
    "usr_mod" AS usr_mod,
    "dat_mod" AS dat_mod,
    "_etl_run_id" AS _etl_run_id
FROM {{ source('ods', 'crn') }}
