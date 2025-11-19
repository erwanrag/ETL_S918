{{
    config(
        materialized='table',
        schema='ods',
        tags=['ods', 'prprixv']
    )
}}

/*
=================================================================
Modèle : ods_prprixv
Description : Modèle ODS auto-généré
Source : staging.stg_prprixv
Stratégie : TABLE
=================================================================
*/


WITH staging AS (
    SELECT * FROM {{ ref('stg_prprixv') }}
),

final AS (
    SELECT
        cod_cli AS code_cli,
        cod_pro AS code_pro,
        cod_rvt_vte AS code_rvt_vte,
        dat_fpxv AS date_fpxv,
        depot,
        fpx_mini,
        fpx_refv,
        fpx_vte,
        fqte_rq,
        no_tarif AS numero_tarif,
        px_mini AS prix_mini,
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