{{
    config(
        materialized='table',
        schema='ods',
        tags=['ods', 'crnar']
    )
}}

/*
=================================================================
Modèle : ods_crnar
Description : Modèle ODS auto-généré
Source : staging.stg_crnar
Stratégie : TABLE
=================================================================
*/


WITH staging AS (
    SELECT * FROM {{ ref('stg_crnar') }}
),

final AS (
    SELECT
        cod_crn AS code_crn,
        cod_pc AS code_pc,
        cod_pro AS code_pro,
        dat_crt AS date_crt,
        dat_mod AS date_mod,
        gencod_v,
        px_vte AS prix_vte,

        -- Metadata
        _loaded_at AS source_loaded_at,
        _source_file AS source_file,
        _sftp_log_id AS sftp_log_id,
        CURRENT_TIMESTAMP AS ods_updated_at
        
    FROM staging
)

SELECT * FROM final