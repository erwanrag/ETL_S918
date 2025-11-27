{{ config(materialized='view') }}

-- Staging model for lldespro

SELECT
    "cod_pro" AS cod_pro,
    "des_lan" AS des_lan,
    "des_lan2" AS des_lan2,
    "flag_repli" AS flag_repli,
    "langue" AS langue,
    "TabPart_lldespro" AS tabpart_lldespro,
    _loaded_at,
    _source_file,
    _sftp_log_id
FROM {{ source('ods', 'lldespro') }}
