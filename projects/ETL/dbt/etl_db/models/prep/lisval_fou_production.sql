{{ config(
    materialized='table',
) }}

/*
    ============================================================================
    Modèle PREP : lisval_fou_production
    ============================================================================
    Généré automatiquement le 2025-12-12 16:41:22
    
    Source       : ods.lisval_fou_production
    Lignes       : 5
    Colonnes ODS : 80
    Colonnes PREP: 8  (+ _prep_loaded_at)
    Exclues      : 73 (91.2%)
    
    Stratégie    : TABLE
    Full Refresh: Oui
    Merge        : N/A
    Incremental  : Enabled (_etl_valid_from)
    Index        : 0 répliqué(s)
    
    Exclusions:
      - Techniques ETL  : 1
      - 100% NULL       : 34
      - Constantes      : 38
      - Faible valeur   : 0
    ============================================================================
*/

SELECT
    "cod_tiers" AS cod_tiers,
    "zt0" AS zt0,
    "no_ordre" AS no_ordre,
    "cod_autre" AS cod_autre,
    "uniq_id" AS uniq_id,
    "_etl_valid_from" AS _etl_source_timestamp,
    "_etl_run_id" AS _etl_run_id,
    CURRENT_TIMESTAMP AS _prep_loaded_at
FROM {{ source('ods', 'lisval_fou_production') }}
