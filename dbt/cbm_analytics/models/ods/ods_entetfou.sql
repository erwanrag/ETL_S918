{{
    config(
        materialized='incremental',
        schema='ods',
        unique_key='uniq_id',
        on_schema_change='sync_all_columns',
        tags=['ods', 'entetfou']
    )
}}

/*
=================================================================
Modèle : ods_entetfou
Description : Modèle ODS auto-généré
Source : staging.stg_entetfou
Stratégie : INCREMENTAL
=================================================================
*/


WITH staging AS (
    SELECT * FROM {{ ref('stg_entetfou') }}
    
    {% if is_incremental() %}
    WHERE _loaded_at > (SELECT MAX(source_loaded_at) FROM {{ this }})
    {% endif %}
),

final AS (
    SELECT
        cod_fof AS code_fof,
        cod_fop AS code_fop,
        cod_for AS code_for,
        cod_fou AS code_fou,
        cod_prof AS code_prof,
        dat_acc AS date_acc,
        dat_arc AS date_arc,
        dat_cde AS date_cde,
        dat_crt AS date_crt,
        dat_ech AS date_ech,
        dat_edi AS date_edi,
        dat_enl AS date_enl,
        dat_liv AS date_liv,
        dat_livd AS date_livd,
        dat_mod AS date_mod,
        dat_px AS date_px,
        dat_rel AS date_rel,
        dat_val AS date_val,
        depot,
        flottant_dat_fac,

        -- Metadata
        _loaded_at AS source_loaded_at,
        _source_file AS source_file,
        _sftp_log_id AS sftp_log_id,
        CURRENT_TIMESTAMP AS ods_updated_at
        
    FROM staging
)

SELECT * FROM final