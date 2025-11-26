{{
    config(
        materialized='table',
        schema='ods',
        tags=['ods', 'hisprixv']
    )
}}

/*
=================================================================
Modèle : ods_hisprixv
Description : Modèle ODS auto-généré
Source : staging.stg_hisprixv
Stratégie : TABLE
=================================================================
*/


WITH staging AS (
    SELECT * FROM {{ ref('stg_hisprixv') }}
),

final AS (
    SELECT
        cod_cli AS code_cli,
        cod_pro AS code_pro,
        dat_mod AS date_mod,
        dat_px AS date_px,
        depot,
        no_tarif AS numero_tarif,
        px_refv AS prix_refv,
        px_vte AS prix_vte,
        qte_rq AS quantite_rq,

        -- Metadata
        _loaded_at AS source_loaded_at,
        _source_file AS source_file,
        _sftp_log_id AS sftp_log_id,
        CURRENT_TIMESTAMP AS ods_updated_at
        
    FROM staging
)

SELECT * FROM final