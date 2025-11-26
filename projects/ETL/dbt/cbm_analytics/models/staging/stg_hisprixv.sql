{{ config(materialized='view') }}

-- Staging model for hisprixv

SELECT
    "cod_cli" AS cod_cli,
    "cod_pro" AS cod_pro,
    "coef_t2" AS coef_t2,
    "coef_t3" AS coef_t3,
    "coef_t4" AS coef_t4,
    "dat_mod" AS dat_mod,
    "dat_px" AS dat_px,
    "depot" AS depot,
    "flag_repli" AS flag_repli,
    "hr_mod" AS hr_mod,
    "no_tarif" AS no_tarif,
    "px_refv" AS px_refv,
    "px_vte" AS px_vte,
    "qte" AS qte,
    "qte_rq" AS qte_rq,
    "TabPart_hisprixv" AS tabpart_hisprixv,
    "usr_mod" AS usr_mod,
    "zon_lib" AS zon_lib,
    _loaded_at,
    _source_file,
    _sftp_log_id
FROM {{ source('raw', 'raw_hisprixv') }}
