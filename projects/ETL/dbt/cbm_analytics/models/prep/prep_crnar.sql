{{ config(materialized='view') }}

/*
    ============================================================================
    Modèle PREP : crnar
    ============================================================================
    Généré automatiquement le 2025-12-05 15:19:50
    
    Source       : ods.crnar
    Lignes       : 429
    Colonnes ODS : 61
    Colonnes PREP: 13
    Exclues      : 48 (78.7%)
    
    Exclusions:
      - Techniques ETL  : 7
      - 100% NULL       : 36
      - Constantes      : 5
      - Faible valeur   : 0
    ============================================================================
*/

SELECT
    "cod_crn" AS cod_crn,
    "cod_pc" AS cod_pc,
    "nom_pc" AS nom_pc,
    "ref_pc" AS ref_pc,
    "usr_crt" AS usr_crt,
    "dat_crt" AS dat_crt,
    "usr_mod" AS usr_mod,
    "dat_mod" AS dat_mod,
    "px_vte" AS px_vte,
    "marque" AS marque,
    "texte" AS texte,
    "cod_pro" AS cod_pro,
    "_etl_run_id" AS _etl_run_id
FROM {{ source('ods', 'crnar') }}
