{{
    config(
        materialized='table',
        schema='ods',
        tags=['ods', 'stock']
    )
}}

/*
=================================================================
Modèle : ods_stock
Description : Modèle ODS auto-généré
Source : staging.stg_stock
Stratégie : TABLE
=================================================================
*/


WITH staging AS (
    SELECT * FROM {{ ref('stg_stock') }}
),

final AS (
    SELECT
        cod_pro AS code_pro,
        dat_cal AS date_cal,
        dat_ent AS date_ent,
        dat_sor AS date_sor,
        depot,
        px_rvt AS prix_rvt,
        px_sim AS prix_sim,
        px_std AS prix_std,
        px_std2 AS prix_std2,
        px_std3 AS prix_std3,
        qte_sit AS quantite_sit,

        -- Metadata
        _loaded_at AS source_loaded_at,
        _source_file AS source_file,
        _sftp_log_id AS sftp_log_id,
        CURRENT_TIMESTAMP AS ods_updated_at
        
    FROM staging
)

SELECT * FROM final