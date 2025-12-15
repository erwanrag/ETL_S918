{{ config(
    materialized='incremental',
    unique_key='cod_crn',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    post_hook=[
        "CREATE UNIQUE INDEX IF NOT EXISTS crnar_pkey ON {{ this }} USING btree (cod_crn, cod_pc)",
        "ANALYZE {{ this }}",
        "DELETE FROM {{ this }} WHERE cod_crn NOT IN (SELECT cod_crn FROM {{ source('ods', 'crnar') }})"
    ]
) }}

/*
    ============================================================================
    Modèle PREP : crnar
    ============================================================================
    Généré automatiquement le 2025-12-12 16:40:05
    
    Source       : ods.crnar
    Lignes       : 382,255
    Colonnes ODS : 56
    Colonnes PREP: 25  (+ _prep_loaded_at)
    Exclues      : 32 (57.1%)
    
    Stratégie    : INCREMENTAL
    Unique Key  : cod_crn
    Merge        : INSERT/UPDATE + DELETE orphans
    Incremental  : Enabled (_etl_valid_from)
    Index        : 2 répliqué(s) + ANALYZE
    
    Exclusions:
      - Techniques ETL  : 1
      - 100% NULL       : 15
      - Constantes      : 14
      - Faible valeur   : 2
    ============================================================================
*/

SELECT
    "cod_crn" AS cod_crn,
    "cod_pc" AS cod_pc,
    "nom_pc" AS nom_pc,
    "ref_pc" AS ref_pc,
    "usr_crt" AS usr_crt,
    "dat_crt" AS dat_crt,
    "usr_mod" AS usr_mod,
    "dat_mod" AS dat_mod,
    "zal_4" AS zal_4,
    "zta_1" AS zta_1,
    "zda_3" AS zda_3,
    "px_vte" AS px_vte,
    "marque" AS marque,
    "zlo_1" AS zlo_1,
    "zlo_2" AS zlo_2,
    "texte" AS texte,
    "pp_uv" AS pp_uv,
    "cod_pro" AS cod_pro,
    "poid_brut_1" AS poid_brut_1,
    "poid_brut_2" AS poid_brut_2,
    "poid_brut_3" AS poid_brut_3,
    "gencod-v" AS gencod_v,
    "_etl_valid_from" AS _etl_source_timestamp,
    "_etl_run_id" AS _etl_run_id,
    CURRENT_TIMESTAMP AS _prep_loaded_at
FROM {{ source('ods', 'crnar') }}

{% if is_incremental() %}
    WHERE "_etl_valid_from" > (
        SELECT COALESCE(MAX(_etl_source_timestamp), '1900-01-01'::timestamp) 
        FROM {{ this }}
    )
{% endif %}
