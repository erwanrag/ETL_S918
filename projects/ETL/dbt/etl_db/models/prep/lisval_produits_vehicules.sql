{{ config(
    materialized='table',
) }}

/*
    ============================================================================
    Modèle PREP : lisval_produits_vehicules
    ============================================================================
    Généré automatiquement le 2025-12-12 16:41:32
    
    Source       : ods.lisval_produits_vehicules
    Lignes       : 180,719
    Colonnes ODS : 80
    Colonnes PREP: 12  (+ _prep_loaded_at)
    Exclues      : 69 (86.2%)
    
    Stratégie    : TABLE
    Full Refresh: Oui
    Merge        : N/A
    Incremental  : Enabled (_etl_valid_from)
    Index        : 0 répliqué(s)
    
    Exclusions:
      - Techniques ETL  : 1
      - 100% NULL       : 33
      - Constantes      : 35
      - Faible valeur   : 0
    ============================================================================
*/

SELECT
    "typ_fich" AS typ_fich,
    "liste" AS liste,
    "cod_tiers" AS cod_tiers,
    "zt0" AS zt0,
    "zt1" AS zt1,
    "ze0" AS ze0,
    "no_ordre" AS no_ordre,
    "cod_autre" AS cod_autre,
    "uniq_id" AS uniq_id,
    "_etl_valid_from" AS _etl_source_timestamp,
    "_etl_run_id" AS _etl_run_id,
    CURRENT_TIMESTAMP AS _prep_loaded_at
FROM {{ source('ods', 'lisval_produits_vehicules') }}
