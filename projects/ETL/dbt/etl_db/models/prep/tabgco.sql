{{ config(
    materialized='table',
) }}

/*
    ============================================================================
    Modèle PREP : tabgco
    ============================================================================
    Généré automatiquement le 2025-12-12 16:58:08
    
    Source       : ods.tabgco
    Lignes       : 6,116
    Colonnes ODS : 291
    Colonnes PREP: 48  (+ _prep_loaded_at)
    Exclues      : 244 (83.8%)
    
    Stratégie    : TABLE
    Full Refresh: Oui
    Merge        : N/A
    Incremental  : Enabled (_etl_valid_from)
    Index        : 0 répliqué(s)
    
    Exclusions:
      - Techniques ETL  : 5
      - 100% NULL       : 175
      - Constantes      : 59
      - Faible valeur   : 5
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
    "cli_cai" AS cli_cai,
    "dern_z" AS dern_z,
    "productif" AS productif,
    "nb_int" AS nb_int,
    "cod_equ" AS cod_equ,
    "no_tel_1" AS no_tel_1,
    "no_tel_2" AS no_tel_2,
    "no_tel_3" AS no_tel_3,
    "no_fax_1" AS no_fax_1,
    "no_fax_2" AS no_fax_2,
    "no_fax_3" AS no_fax_3,
    "mel" AS mel,
    "no_port" AS no_port,
    "min_t" AS min_t,
    "adresse4" AS adresse4,
    "fct_com" AS fct_com,
    "fct_tlv" AS fct_tlv,
    "fct_aa" AS fct_aa,
    "fct_ce" AS fct_ce,
    "cod_tlv" AS cod_tlv,
    "externe" AS externe,
    "usr_pro" AS usr_pro,
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
    "_etl_loaded_at" AS _etl_loaded_at,
    "_etl_run_id" AS _etl_run_id,
    "_etl_valid_from" AS _etl_source_timestamp,
    CURRENT_TIMESTAMP AS _prep_loaded_at
FROM {{ source('ods', 'tabgco') }}
