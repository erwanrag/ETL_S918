{{
    config(
        materialized='table',
        schema='ods',
        tags=['ods', 'clcondi']
    )
}}

/*
=================================================================
Modèle : ods_clcondi
Description : Modèle ODS auto-généré
Source : staging.stg_clcondi
Stratégie : TABLE
=================================================================
*/


WITH staging AS (
    SELECT * FROM {{ ref('stg_clcondi') }}
),

final AS (
    SELECT
        cod_cli AS code_cli,
        cod_dec AS code_dec,
        cod_dec1 AS code_dec1,
        cod_dec2 AS code_dec2,
        cod_dec3 AS code_dec3,
        cod_dec4 AS code_dec4,
        cod_dec5 AS code_dec5,
        cod_fou AS code_fou,
        cod_pro AS code_pro,
        dat_crt AS date_crt,
        dat_deb AS date_deb,
        dat_fin AS date_fin,
        dat_mod AS date_mod,
        depot,
        no_cond AS numero_cond,
        no_tarif AS numero_tarif,
        px_brut AS prix_brut,
        px_cli AS prix_cli,
        px_net AS prix_net,
        px_poi AS prix_poi,

        -- Metadata
        _loaded_at AS source_loaded_at,
        _source_file AS source_file,
        _sftp_log_id AS sftp_log_id,
        CURRENT_TIMESTAMP AS ods_updated_at
        
    FROM staging
)

SELECT * FROM final