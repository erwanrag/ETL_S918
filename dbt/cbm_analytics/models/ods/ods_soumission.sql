{{
    config(
        materialized='table',
        schema='ods',
        tags=['ods', 'soumission']
    )
}}

/*
=================================================================
Modèle : ods_soumission
Description : Modèle ODS auto-généré
Source : staging.stg_soumission
Stratégie : TABLE
=================================================================
*/


WITH staging AS (
    SELECT * FROM {{ ref('stg_soumission') }}
),

final AS (
    SELECT
        cod_dec1 AS code_dec1,
        cod_dec2 AS code_dec2,
        cod_dec3 AS code_dec3,
        cod_dec4 AS code_dec4,
        cod_dec5 AS code_dec5,
        cod_pro AS code_pro,
        cod_tiers AS code_tiers,
        dat_deb AS date_deb,
        dat_fin AS date_fin,
        dat_liv AS date_liv,
        depot,
        no_cde AS numero_cde,
        no_contrat AS numero_contrat,
        no_lot AS numero_lot,
        px_net AS prix_net,
        qte_his AS quantite_his,
        region_depots,
        statut,

        -- Metadata
        _loaded_at AS source_loaded_at,
        _source_file AS source_file,
        _sftp_log_id AS sftp_log_id,
        CURRENT_TIMESTAMP AS ods_updated_at
        
    FROM staging
)

SELECT * FROM final