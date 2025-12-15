{{ config(
    materialized='incremental',
    unique_key='cod_crn',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    post_hook=[
        "CREATE UNIQUE INDEX IF NOT EXISTS crn_pkey ON {{ this }} USING btree (cod_crn)",
        "ANALYZE {{ this }}",
        "DELETE FROM {{ this }} WHERE cod_crn NOT IN (SELECT cod_crn FROM {{ source('ods', 'crn') }})"
    ]
) }}

/*
    ============================================================================
    Modèle PREP : crn
    ============================================================================
    Généré automatiquement le 2025-12-12 16:39:40
    
    Source       : ods.crn
    Lignes       : 836
    Colonnes ODS : 56
    Colonnes PREP: 15  (+ _prep_loaded_at)
    Exclues      : 42 (75.0%)
    
    Stratégie    : INCREMENTAL
    Unique Key  : cod_crn
    Merge        : INSERT/UPDATE + DELETE orphans
    Incremental  : Enabled (_etl_valid_from)
    Index        : 2 répliqué(s) + ANALYZE
    
    Exclusions:
      - Techniques ETL  : 1
      - 100% NULL       : 24
      - Constantes      : 17
      - Faible valeur   : 0
    ============================================================================
*/

SELECT
    "cod_crn" AS cod_crn,
    "nom_crn" AS nom_crn,
    "usr_crt" AS usr_crt,
    "dat_crt" AS dat_crt,
    "usr_mod" AS usr_mod,
    "dat_mod" AS dat_mod,
    "zta_1" AS zta_1,
    "zta_2" AS zta_2,
    "zta_3" AS zta_3,
    "num_tel" AS num_tel,
    "num_fax" AS num_fax,
    "no_info" AS no_info,
    "_etl_valid_from" AS _etl_source_timestamp,
    "_etl_run_id" AS _etl_run_id,
    CURRENT_TIMESTAMP AS _prep_loaded_at
FROM {{ source('ods', 'crn') }}

{% if is_incremental() %}
    WHERE "_etl_valid_from" > (
        SELECT COALESCE(MAX(_etl_source_timestamp), '1900-01-01'::timestamp) 
        FROM {{ this }}
    )
{% endif %}
