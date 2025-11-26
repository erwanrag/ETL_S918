{{
    config(
        materialized='table',
        schema='ods',
        tags=['ods', 'fournis']
    )
}}

/*
=================================================================
Modèle : ods_fournis
Description : Modèle ODS auto-généré
Source : staging.stg_fournis
Stratégie : TABLE
=================================================================
*/


WITH staging AS (
    SELECT * FROM {{ ref('stg_fournis') }}
),

final AS (
    SELECT
        cod_fop AS code_fop,
        cod_for AS code_for,
        cod_fou AS code_fou,
        dat_crt AS date_crt,
        dat_mod AS date_mod,
        mt_franco AS montant_franco,
        mt_mini AS montant_mini,
        statut,

        -- Metadata
        _loaded_at AS source_loaded_at,
        _source_file AS source_file,
        _sftp_log_id AS sftp_log_id,
        CURRENT_TIMESTAMP AS ods_updated_at
        
    FROM staging
)

SELECT * FROM final