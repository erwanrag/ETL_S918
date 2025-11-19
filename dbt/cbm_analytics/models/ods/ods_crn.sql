{{
    config(
        materialized='table',
        schema='ods',
        tags=['ods', 'crn']
    )
}}

/*
=================================================================
Modèle : ods_crn
Description : Modèle ODS auto-généré
Source : staging.stg_crn
Stratégie : TABLE
=================================================================
*/


WITH staging AS (
    SELECT * FROM {{ ref('stg_crn') }}
),

final AS (
    SELECT
        cod_crn AS code_crn,
        dat_crt AS date_crt,
        dat_mod AS date_mod,
        no_info AS numero_info,

        -- Metadata
        _loaded_at AS source_loaded_at,
        _source_file AS source_file,
        _sftp_log_id AS sftp_log_id,
        CURRENT_TIMESTAMP AS ods_updated_at
        
    FROM staging
)

SELECT * FROM final