{{ config(materialized='view') }}

-- Staging model for crnar

SELECT
    "cod_crn" AS cod_crn,
    "cod_pc" AS cod_pc,
    "cod_pro" AS cod_pro,
    "dat_crt" AS dat_crt,
    "dat_mod" AS dat_mod,
    "flag_repli" AS flag_repli,
    "gencod-v" AS gencod_v,
    "lib_conv" AS lib_conv,
    "lib_surv" AS lib_surv,
    "lib_univ" AS lib_univ,
    "marque" AS marque,
    "nb_cv" AS nb_cv,
    "nb_uv" AS nb_uv,
    "nom_pc" AS nom_pc,
    "pds_net" AS pds_net,
    "poid_brut" AS poid_brut,
    "pp_uv" AS pp_uv,
    "px_vte" AS px_vte,
    "rayon" AS rayon,
    "ref_pc" AS ref_pc,
    "TabPart_crnar" AS tabpart_crnar,
    "texte" AS texte,
    "usr_crt" AS usr_crt,
    "usr_mod" AS usr_mod,
    "zal" AS zal,
    "zda" AS zda,
    "zlo" AS zlo,
    "znu" AS znu,
    "zta" AS zta,
    _loaded_at,
    _source_file,
    _sftp_log_id
FROM {{ source('raw', 'raw_crnar') }}
