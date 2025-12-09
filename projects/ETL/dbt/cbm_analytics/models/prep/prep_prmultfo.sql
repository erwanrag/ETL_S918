{{ config(materialized='view') }}

/*
    ============================================================================
    Modèle PREP : prmultfo
    ============================================================================
    Généré automatiquement le 2025-12-05 15:34:14
    
    Source       : ods.prmultfo
    Lignes       : 873
    Colonnes ODS : 172
    Colonnes PREP: 25
    Exclues      : 147 (85.5%)
    
    Exclusions:
      - Techniques ETL  : 7
      - 100% NULL       : 80
      - Constantes      : 55
      - Faible valeur   : 5
    ============================================================================
*/

SELECT
    "cod_pro" AS cod_pro,
    "cod_fou" AS cod_fou,
    "des_ach" AS des_ach,
    "delai" AS delai,
    "refext" AS refext,
    "px_refa" AS px_refa,
    "fpx_refa" AS fpx_refa,
    "rem_ach" AS rem_ach,
    "gencod-a" AS gencod_a,
    "nom_pro" AS nom_pro,
    "refint" AS refint,
    "principa" AS principa,
    "calc_uv" AS calc_uv,
    "sf_tar" AS sf_tar,
    "s3_tar" AS s3_tar,
    "s4_tar" AS s4_tar,
    "prio_fou" AS prio_fou,
    "mini_ach" AS mini_ach,
    "mult_ach" AS mult_ach,
    "nb_ua" AS nb_ua,
    "phone" AS phone,
    "f_sf_tar" AS f_sf_tar,
    "f_s3_tar" AS f_s3_tar,
    "pays_ori" AS pays_ori,
    "_etl_run_id" AS _etl_run_id
FROM {{ source('ods', 'prmultfo') }}
