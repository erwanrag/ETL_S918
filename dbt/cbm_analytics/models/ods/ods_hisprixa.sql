{{
    config(
        materialized='table',
        schema='ods',
        tags=['ods', 'hisprixa']
    )
}}

/*
=================================================================
Modèle : ods_hisprixa
Description : Modèle ODS auto-généré
Source : staging.stg_hisprixa
Stratégie : TABLE
=================================================================
*/


WITH staging AS (
    SELECT * FROM {{ ref('stg_hisprixa') }}
),

final AS (
    SELECT
        cod_fou AS code_fou,
        cod_pro AS code_pro,
        dat_mod AS date_mod,
        dat_px AS date_px,
        depot,
        px_ach AS prix_ach,
        px_ach_rq AS prix_ach_rq,
        px_refa AS prix_refa,
        qte_rq AS quantite_rq,

        -- Metadata
        _loaded_at AS source_loaded_at,
        _source_file AS source_file,
        _sftp_log_id AS sftp_log_id,
        CURRENT_TIMESTAMP AS ods_updated_at
        
    FROM staging
)

SELECT * FROM final