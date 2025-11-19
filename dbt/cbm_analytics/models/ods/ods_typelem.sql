{{
    config(
        materialized='table',
        schema='ods',
        tags=['ods', 'typelem']
    )
}}

/*
=================================================================
Modèle : ods_typelem
Description : Modèle ODS auto-généré
Source : staging.stg_typelem
Stratégie : TABLE
=================================================================
*/


WITH staging AS (
    SELECT * FROM {{ ref('stg_typelem') }}
),

final AS (
    SELECT
        mt_mini AS montant_mini,

        -- Metadata
        _loaded_at AS source_loaded_at,
        _source_file AS source_file,
        _sftp_log_id AS sftp_log_id,
        CURRENT_TIMESTAMP AS ods_updated_at
        
    FROM staging
)

SELECT * FROM final