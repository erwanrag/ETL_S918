{{ config(materialized='view') }}

/*
    ============================================================================
    Modèle PREP : fournis
    ============================================================================
    Généré automatiquement le 2025-12-05 15:23:48
    
    Source       : ods.fournis
    Lignes       : 36
    Colonnes ODS : 221
    Colonnes PREP: 29
    Exclues      : 192 (86.9%)
    
    Exclusions:
      - Techniques ETL  : 7
      - 100% NULL       : 139
      - Constantes      : 41
      - Faible valeur   : 5
    ============================================================================
*/

SELECT
    "typ_elem" AS typ_elem,
    "cod_fou" AS cod_fou,
    "nom_fou" AS nom_fou,
    "ville" AS ville,
    "mt_franco" AS mt_franco,
    "mt_mini" AS mt_mini,
    "regime" AS regime,
    "type_fac" AS type_fac,
    "frais_app" AS frais_app,
    "edt_cde" AS edt_cde,
    "edt_rec" AS edt_rec,
    "siret" AS siret,
    "capital" AS capital,
    "usr_crt" AS usr_crt,
    "dat_crt" AS dat_crt,
    "usr_mod" AS usr_mod,
    "dat_mod" AS dat_mod,
    "statut" AS statut,
    "txt_com" AS txt_com,
    "pays" AS pays,
    "internet" AS internet,
    "mot_cle" AS mot_cle,
    "notel" AS notel,
    "k_post2" AS k_post2,
    "acheteur" AS acheteur,
    "inc_deb" AS inc_deb,
    "phone" AS phone,
    "note_calc" AS note_calc,
    "_etl_run_id" AS _etl_run_id
FROM {{ source('ods', 'fournis') }}
