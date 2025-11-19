{{
    config(
        materialized='incremental',
        schema='ods',
        unique_key='uniq_id',
        on_schema_change='sync_all_columns',
        tags=['ods', 'lignefou']
    )
}}

/*
=================================================================
Modèle : ods_lignefou
Description : Modèle ODS auto-généré
Source : staging.stg_lignefou
Stratégie : INCREMENTAL
=================================================================
*/


WITH staging AS (
    SELECT * FROM {{ ref('stg_lignefou') }}
    
    {% if is_incremental() %}
    WHERE _loaded_at > (SELECT MAX(source_loaded_at) FROM {{ this }})
    {% endif %}
),

final AS (
    SELECT
        cod_cli AS code_cli,
        cod_conv AS code_conv,
        cod_dec1 AS code_dec1,
        cod_dec2 AS code_dec2,
        cod_dec3 AS code_dec3,
        cod_dec4 AS code_dec4,
        cod_dec5 AS code_dec5,
        cod_fou AS code_fou,
        cod_fou_df AS code_fou_df,
        cod_nom AS code_nom,
        cod_ori AS code_ori,
        cod_pro AS code_pro,
        cod_prolie AS code_prolie,
        cod_rvt_ach AS code_rvt_ach,
        dat_acc AS date_acc,
        dat_cde AS date_cde,
        dat_enl AS date_enl,
        dat_liv AS date_liv,
        dat_livd AS date_livd,
        depot,

        -- Metadata
        _loaded_at AS source_loaded_at,
        _source_file AS source_file,
        _sftp_log_id AS sftp_log_id,
        CURRENT_TIMESTAMP AS ods_updated_at
        
    FROM staging
)

SELECT * FROM final