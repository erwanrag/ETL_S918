{{
    config(
        materialized='table',
        schema='ods',
        tags=['ods', 'client']
    )
}}

/*
=================================================================
Modèle : ods_client
Description : Modèle ODS auto-généré
Source : staging.stg_client
Stratégie : TABLE
=================================================================
*/


WITH staging AS (
    SELECT * FROM {{ ref('stg_client') }}
),

final AS (
    SELECT
        cod_adh AS code_adh,
        cod_cli AS code_cli,
        cod_tlv AS code_tlv,
        dat_crt AS date_crt,
        dat_mod AS date_mod,
        dat_sta AS date_sta,
        depot,
        gencod_det,
        mt_franco AS montant_franco,
        mt_mini AS montant_mini,
        no_ctrl AS numero_ctrl,
        no_port AS numero_port,
        no_tar_loc AS numero_tar_loc,
        no_tarif AS numero_tarif,
        statut,

        -- Metadata
        _loaded_at AS source_loaded_at,
        _source_file AS source_file,
        _sftp_log_id AS sftp_log_id,
        CURRENT_TIMESTAMP AS ods_updated_at
        
    FROM staging
)

SELECT * FROM final