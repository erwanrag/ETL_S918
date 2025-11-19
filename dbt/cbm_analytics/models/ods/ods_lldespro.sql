{{
    config(
        materialized='table',
        schema='ods',
        tags=['ods', 'lldespro']
    )
}}

/*
=================================================================
Modèle : ods_lldespro
Description : Modèle ODS auto-généré
Source : staging.stg_lldespro
Stratégie : TABLE
=================================================================
*/


WITH staging AS (
    SELECT * FROM {{ ref('stg_lldespro') }}
),

final AS (
    SELECT
        cod_pro AS code_pro,

        -- Metadata
        _loaded_at AS source_loaded_at,
        _source_file AS source_file,
        _sftp_log_id AS sftp_log_id,
        CURRENT_TIMESTAMP AS ods_updated_at
        
    FROM staging
)

SELECT * FROM final