{{
    config(
        materialized='incremental',
        schema='ods',
        unique_key='uniq_id',
        on_schema_change='sync_all_columns',
        tags=['ods', 'entetcli']
    )
}}

/*
=================================================================
Modèle : ods_entetcli
Description : Modèle ODS auto-généré
Source : staging.stg_entetcli
Stratégie : INCREMENTAL
=================================================================
*/


WITH staging AS (
    SELECT * FROM {{ ref('stg_entetcli') }}
    
    {% if is_incremental() %}
    WHERE _loaded_at > (SELECT MAX(source_loaded_at) FROM {{ this }})
    {% endif %}
),

final AS (
    SELECT
        cod_cli AS code_cli,
        cod_dev AS code_dev,
        cod_for AS code_for,
        cod_op AS code_op,
        cod_site AS code_site,
        cod_tlv AS code_tlv,
        dat_acc AS date_acc,
        dat_bl AS date_bl,
        dat_bp AS date_bp,
        dat_cde AS date_cde,
        dat_crt AS date_crt,
        dat_dep AS date_dep,
        dat_dev AS date_dev,
        dat_ech AS date_ech,
        dat_fac AS date_fac,
        dat_liv AS date_liv,
        dat_livd AS date_livd,
        dat_mad AS date_mad,
        dat_mod AS date_mod,
        dat_px AS date_px,

        -- Metadata
        _loaded_at AS source_loaded_at,
        _source_file AS source_file,
        _sftp_log_id AS sftp_log_id,
        CURRENT_TIMESTAMP AS ods_updated_at
        
    FROM staging
)

SELECT * FROM final