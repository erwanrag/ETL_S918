{{ config(
    materialized='table',
) }}

/*
============================================================================
PREP MODEL : prmultfo
============================================================================
Generated : 2025-12-21 05:00:37
Source    : ods.prmultfo
Rows ODS  : 15,308
Cols ODS  : 172
Cols PREP : 50 (+ _prep_loaded_at)
Strategy  : TABLE
============================================================================
*/

SELECT
    "cod_pro" AS cod_pro,
    "cod_fou" AS cod_fou,
    "des_ach" AS des_ach,
    "delai" AS delai,
    "refext" AS refext,
    "devise" AS devise,
    "px_refa" AS px_refa,
    "pxnet" AS pxnet,
    "fpx_refa" AS fpx_refa,
    "dat_fpxa" AS dat_fpxa,
    "uni_ach" AS uni_ach,
    "lib_unia" AS lib_unia,
    "lib_cona" AS lib_cona,
    "conv_sto" AS conv_sto,
    "rem_ach" AS rem_ach,
    "gencod-a" AS gencod_a,
    "typ_elem" AS typ_elem,
    "nom_pro" AS nom_pro,
    "refint" AS refint,
    "principa" AS principa,
    "statut" AS statut,
    "conv_md" AS conv_md,
    "conv_dec" AS conv_dec,
    "calc_uv" AS calc_uv,
    "zal_1" AS zal_1,
    "znu_1" AS znu_1,
    "znu_4" AS znu_4,
    "zta_2" AS zta_2,
    "fam_tar" AS fam_tar,
    "sf_tar" AS sf_tar,
    "cod_conv" AS cod_conv,
    "s3_tar" AS s3_tar,
    "s4_tar" AS s4_tar,
    "prio_fou" AS prio_fou,
    "dat_prin" AS dat_prin,
    "mini_ach" AS mini_ach,
    "mult_ach" AS mult_ach,
    "nb_ua" AS nb_ua,
    "pp_ua" AS pp_ua,
    "phone" AS phone,
    "px_max" AS px_max,
    "art_cs_u" AS art_cs_u,
    "f_s3_tar" AS f_s3_tar,
    "dat_import" AS dat_import,
    "qte_eco" AS qte_eco,
    "pays_ori" AS pays_ori,
    "_etl_loaded_at" AS _etl_loaded_at,
    "_etl_run_id" AS _etl_run_id,
    "_etl_valid_from" AS _etl_source_timestamp,
    CURRENT_TIMESTAMP AS _prep_loaded_at
FROM {{ source('ods', 'prmultfo') }}
