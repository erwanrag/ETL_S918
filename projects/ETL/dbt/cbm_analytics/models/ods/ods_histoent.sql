{{
    config(
        materialized='incremental',
        schema='ods',
        unique_key='uniq_id',
        on_schema_change='sync_all_columns',
        tags=['ods', 'histoent']
    )
}}

/*
=================================================================
Modèle : ods_histoent
Description : Modèle ODS auto-généré
Source : staging.stg_histoent
Stratégie : INCREMENTAL
=================================================================
*/


WITH staging AS (
    SELECT * FROM {{ ref('stg_histoent') }}
    
    {% if is_incremental() %}
    WHERE _loaded_at > (SELECT MAX(source_loaded_at) FROM {{ this }})
    {% endif %}
),

final AS (
    SELECT
        cod_cf AS code_cf,
        cod_fof AS code_fof,
        cod_fop AS code_fop,
        cod_for AS code_for,
        cod_op AS code_op,
        cod_prof AS code_prof,
        cod_site AS code_site,
        cod_tlv AS code_tlv,
        dat_acc AS date_acc,
        dat_bl AS date_bl,
        dat_bp AS date_bp,
        dat_cde AS date_cde,
        dat_crt AS date_crt,
        dat_dep AS date_dep,
        dat_ech AS date_ech,
        dat_enl AS date_enl,
        dat_fac AS date_fac,
        dat_his AS date_his,
        dat_liv AS date_liv,
        dat_livd AS date_livd,

        -- Metadata
        _loaded_at AS source_loaded_at,
        _source_file AS source_file,
        _sftp_log_id AS sftp_log_id,
        CURRENT_TIMESTAMP AS ods_updated_at
        
    FROM staging
)

SELECT * FROM final