{{ config(materialized='view') }}

-- Staging model for hisprixa

SELECT
    "cod_fou" AS cod_fou,
    "cod_pro" AS cod_pro,
    "dat_mod" AS dat_mod,
    "dat_px" AS dat_px,
    "depot" AS depot,
    "flag_repli" AS flag_repli,
    "hr_mod" AS hr_mod,
    "px_ach" AS px_ach,
    "px_ach_rq" AS px_ach_rq,
    "px_refa" AS px_refa,
    "qte" AS qte,
    "qte_rq" AS qte_rq,
    "TabPart_hisprixa" AS tabpart_hisprixa,
    "usr_mod" AS usr_mod,
    "zon_lib" AS zon_lib,
    _loaded_at,
    _source_file,
    _sftp_log_id
FROM {{ source('raw', 'raw_hisprixa') }}
