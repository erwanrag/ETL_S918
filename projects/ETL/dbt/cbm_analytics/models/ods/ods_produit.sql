{{
    config(
        materialized='table',
        schema='ods',
        tags=['ods', 'produit']
    )
}}

/*
=================================================================
Modèle : ods_produit
Description : Modèle ODS auto-généré
Source : staging.stg_produit
Stratégie : TABLE
=================================================================
*/


WITH staging AS (
    SELECT * FROM {{ ref('stg_produit') }}
),

final AS (
    SELECT
        cod_cal AS code_cal,
        cod_cat AS code_cat,
        cod_cli AS code_cli,
        cod_conv AS code_conv,
        cod_for AS code_for,
        cod_nom AS code_nom,
        cod_nue AS code_nue,
        cod_par AS code_par,
        cod_prev AS code_prev,
        cod_pro AS code_pro,
        cod_prx AS code_prx,
        dat_crt AS date_crt,
        dat_ent AS date_ent,
        dat_fpxr AS date_fpxr,
        dat_fpxv AS date_fpxv,
        dat_import AS date_import,
        dat_mod AS date_mod,
        dat_remp AS date_remp,
        dpx_rvt,
        fpx_mini,

        -- Metadata
        _loaded_at AS source_loaded_at,
        _source_file AS source_file,
        _sftp_log_id AS sftp_log_id,
        CURRENT_TIMESTAMP AS ods_updated_at
        
    FROM staging
)

SELECT * FROM final