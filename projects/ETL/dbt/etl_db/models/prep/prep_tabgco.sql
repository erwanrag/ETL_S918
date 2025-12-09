{{ config(materialized='view') }}

/*
    ============================================================================
    Modèle PREP : tabgco
    ============================================================================
    Généré automatiquement le 2025-12-05 15:38:21
    
    Source       : ods.tabgco
    Lignes       : 6,116
    Colonnes ODS : 291
    Colonnes PREP: 32
    Exclues      : 259 (89.0%)
    
    Exclusions:
      - Techniques ETL  : 7
      - 100% NULL       : 189
      - Constantes      : 60
      - Faible valeur   : 3
    ============================================================================
*/

SELECT
    "ndos" AS ndos,
    "n_tab" AS n_tab,
    "a_tab" AS a_tab,
    "type_tab" AS type_tab,
    "inti_tab" AS inti_tab,
    "fa_prcm" AS fa_prcm,
    "fp_cptv" AS fp_cptv,
    "fp_cpta" AS fp_cpta,
    "civilite" AS civilite,
    "ville" AS ville,
    "pays" AS pays,
    "dern_z" AS dern_z,
    "productif" AS productif,
    "mel" AS mel,
    "adresse4" AS adresse4,
    "fct_com" AS fct_com,
    "fct_tlv" AS fct_tlv,
    "fct_aa" AS fct_aa,
    "fct_ce" AS fct_ce,
    "externe" AS externe,
    "cod_cli" AS cod_cli,
    "cod_fou" AS cod_fou,
    "fp_cptr" AS fp_cptr,
    "fp_cptt" AS fp_cptt,
    "texte" AS texte,
    "k_post2" AS k_post2,
    "WEB" AS web,
    "datent" AS datent,
    "nobur" AS nobur,
    "non_sscc" AS non_sscc,
    "non_sor_sscc" AS non_sor_sscc,
    "_etl_run_id" AS _etl_run_id
FROM {{ source('ods', 'tabgco') }}
