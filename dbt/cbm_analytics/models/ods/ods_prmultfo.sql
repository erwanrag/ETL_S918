{{
    config(
        materialized='table',
        schema='ods',
        tags=['ods', 'prmultfo']
    )
}}

/*
=================================================================
Modèle : ods_prmultfo
Description : Modèle ODS auto-généré
Source : staging.stg_prmultfo
Stratégie : TABLE
=================================================================
*/


WITH staging AS (
    SELECT * FROM {{ ref('stg_prmultfo') }}
),

final AS (
    SELECT
        cod_conv AS code_conv,
        cod_fou AS code_fou,
        cod_pro AS code_pro,
        dat_fpxa AS date_fpxa,
        dat_import AS date_import,
        dat_prin AS date_prin,
        fpx_refa,
        gencod_a,
        px_max AS prix_max,
        px_refa AS prix_refa,
        qte_eco AS quantite_eco,
        statut,

        -- Metadata
        _loaded_at AS source_loaded_at,
        _source_file AS source_file,
        _sftp_log_id AS sftp_log_id,
        CURRENT_TIMESTAMP AS ods_updated_at
        
    FROM staging
)

SELECT * FROM final