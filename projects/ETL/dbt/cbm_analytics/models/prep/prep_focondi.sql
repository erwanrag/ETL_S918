{{ config(materialized='view') }}

/*
    ============================================================================
    Modèle PREP : focondi
    ============================================================================
    Généré automatiquement le 2025-12-05 15:23:48
    
    Source       : ods.focondi
    Lignes       : 102,670
    Colonnes ODS : 127
    Colonnes PREP: 16
    Exclues      : 111 (87.4%)
    
    Exclusions:
      - Techniques ETL  : 7
      - 100% NULL       : 100
      - Constantes      : 4
      - Faible valeur   : 0
    ============================================================================
*/

SELECT
    "promo" AS promo,
    "cod_fou" AS cod_fou,
    "cod_pro" AS cod_pro,
    "dat_deb" AS dat_deb,
    "dat_fin" AS dat_fin,
    "condi_a" AS condi_a,
    "c_promo" AS c_promo,
    "sf_tar" AS sf_tar,
    "s3_tar" AS s3_tar,
    "s4_tar" AS s4_tar,
    "depot" AS depot,
    "usr_crt" AS usr_crt,
    "dat_crt" AS dat_crt,
    "usr_mod" AS usr_mod,
    "dat_mod" AS dat_mod,
    "_etl_run_id" AS _etl_run_id
FROM {{ source('ods', 'focondi') }}
