{{ config(materialized='view') }}

/*
    ============================================================================
    Modèle PREP : clcondi
    ============================================================================
    Généré automatiquement le 2025-12-05 15:19:49
    
    Source       : ods.clcondi
    Lignes       : 429,907
    Colonnes ODS : 166
    Colonnes PREP: 17
    Exclues      : 149 (89.8%)
    
    Exclusions:
      - Techniques ETL  : 7
      - 100% NULL       : 129
      - Constantes      : 13
      - Faible valeur   : 0
    ============================================================================
*/

SELECT
    "promo" AS promo,
    "cat_tar" AS cat_tar,
    "cod_cli" AS cod_cli,
    "cod_fou" AS cod_fou,
    "cod_pro" AS cod_pro,
    "dat_deb" AS dat_deb,
    "dat_fin" AS dat_fin,
    "no_tarif" AS no_tarif,
    "applic" AS applic,
    "depot" AS depot,
    "usr_crt" AS usr_crt,
    "dat_crt" AS dat_crt,
    "usr_mod" AS usr_mod,
    "dat_mod" AS dat_mod,
    "typ_gsd" AS typ_gsd,
    "uniq_id" AS uniq_id,
    "_etl_run_id" AS _etl_run_id
FROM {{ source('ods', 'clcondi') }}
