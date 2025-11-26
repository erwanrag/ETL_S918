{{
    config(
        materialized='table',
        schema='ods',
        tags=['ods', 'deppro']
    )
}}

/*
=================================================================
Modèle : ods_deppro
Description : Modèle ODS auto-généré
Source : staging.stg_deppro
Stratégie : TABLE
=================================================================
*/


WITH staging AS (
    SELECT * FROM {{ ref('stg_deppro') }}
),

final AS (
    SELECT
        cod_fou AS code_fou,
        cod_pro AS code_pro,
        dat_ent AS date_ent,
        dat_fpxv AS date_fpxv,
        dat_prin AS date_prin,
        dat_remp AS date_remp,
        depot,
        dpx_rvt,
        fpx_mini,
        fpx_refv,
        px_mini AS prix_mini,
        px_refv AS prix_refv,
        px_rvt AS prix_rvt,
        px_sim AS prix_sim,
        px_std AS prix_std,
        px_std2 AS prix_std2,
        px_std3 AS prix_std3,
        px_ttc AS prix_ttc,

        -- Metadata
        _loaded_at AS source_loaded_at,
        _source_file AS source_file,
        _sftp_log_id AS sftp_log_id,
        CURRENT_TIMESTAMP AS ods_updated_at
        
    FROM staging
)

SELECT * FROM final