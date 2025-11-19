{{
    config(
        materialized='table',
        schema='ods',
        tags=['ods', 'prnomenc']
    )
}}

/*
=================================================================
Modèle : ods_prnomenc
Description : Modèle ODS auto-généré
Source : staging.stg_prnomenc
Stratégie : TABLE
=================================================================
*/


WITH staging AS (
    SELECT * FROM {{ ref('stg_prnomenc') }}
),

final AS (
    SELECT
        cod_dec1 AS code_dec1,
        cod_dec2 AS code_dec2,
        cod_dec3 AS code_dec3,
        cod_dec4 AS code_dec4,
        cod_dec5 AS code_dec5,
        cod_fou AS code_fou,
        cod_nmc AS code_nmc,
        cod_pro AS code_pro,
        dat_app AS date_app,
        dat_fin AS date_fin,
        depot,
        fcod_nmc,
        no_info AS numero_info,
        no_page AS numero_page,
        qte_avc AS quantite_avc,
        qte_pf AS quantite_pf,

        -- Metadata
        _loaded_at AS source_loaded_at,
        _source_file AS source_file,
        _sftp_log_id AS sftp_log_id,
        CURRENT_TIMESTAMP AS ods_updated_at
        
    FROM staging
)

SELECT * FROM final