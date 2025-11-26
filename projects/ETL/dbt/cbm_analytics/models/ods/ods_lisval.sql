{{
    config(
        materialized='table',
        schema='ods',
        tags=['ods', 'lisval']
    )
}}

/*
=================================================================
Modèle : ods_lisval
Description : Modèle ODS auto-généré
Source : staging.stg_lisval
Stratégie : TABLE
=================================================================
*/


WITH staging AS (
    SELECT * FROM {{ ref('stg_lisval') }}
),

final AS (
    SELECT
        cod_autre AS code_autre,
        cod_tiers AS code_tiers,
        no_ordre AS numero_ordre,

        -- Metadata
        _loaded_at AS source_loaded_at,
        _source_file AS source_file,
        _sftp_log_id AS sftp_log_id,
        CURRENT_TIMESTAMP AS ods_updated_at
        
    FROM staging
)

SELECT * FROM final