{{ config(
    materialized='table',
) }}

/*
    ============================================================================
    Modèle PREP : lisval_produits_lies
    ============================================================================
    Généré automatiquement le 2025-12-12 16:41:23
    
    Source       : ods.lisval_produits_lies
    Lignes       : 4,471
    Colonnes ODS : 80
    Colonnes PREP: 9  (+ _prep_loaded_at)
    Exclues      : 72 (90.0%)
    
    Stratégie    : TABLE
    Full Refresh: Oui
    Merge        : N/A
    Incremental  : Enabled (_etl_valid_from)
    Index        : 0 répliqué(s)
    
    Exclusions:
      - Techniques ETL  : 1
      - 100% NULL       : 34
      - Constantes      : 37
      - Faible valeur   : 0
    ============================================================================
*/

SELECT
    "cod_tiers" AS cod_tiers,
    "zt0" AS zt0,
    "ze0" AS ze0,
    "no_ordre" AS no_ordre,
    "cod_autre" AS cod_autre,
    "uniq_id" AS uniq_id,
    "_etl_valid_from" AS _etl_source_timestamp,
    "_etl_run_id" AS _etl_run_id,
    CURRENT_TIMESTAMP AS _prep_loaded_at
FROM {{ source('ods', 'lisval_produits_lies') }}
