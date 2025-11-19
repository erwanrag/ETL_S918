{{
    config(
        materialized='table',
        schema='ods',
        tags=['ods', 'tabgco']
    )
}}

/*
=================================================================
Modèle : ods_tabgco
Description : Modèle ODS auto-généré
Source : staging.stg_tabgco
Stratégie : TABLE
=================================================================
*/


WITH staging AS (
    SELECT * FROM {{ ref('stg_tabgco') }}
),

final AS (
    SELECT
        cod_cli AS code_cli,
        cod_equ AS code_equ,
        cod_fou AS code_fou,
        cod_tlv AS code_tlv,
        depot,
        no_fax AS numero_fax,
        no_port AS numero_port,
        no_tel AS numero_tel,
        statut,

        -- Metadata
        _loaded_at AS source_loaded_at,
        _source_file AS source_file,
        _sftp_log_id AS sftp_log_id,
        CURRENT_TIMESTAMP AS ods_updated_at
        
    FROM staging
)

SELECT * FROM final