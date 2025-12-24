-- ============================================================================
-- Comparaison ODS (Python prod) vs ODS_DBT (dbt test)
-- ============================================================================
-- Usage : psql -d s918_dwh -f tests/compare_ods_vs_ods_dbt.sql

-- ============================================================================
-- 1. Volumétrie globale
-- ============================================================================
\echo ''
\echo '==================================================================='
\echo '1. VOLUMÉTRIE GLOBALE'
\echo '==================================================================='

SELECT 
    'ods (Python prod)' AS source,
    COUNT(*) AS tables
FROM information_schema.tables
WHERE table_schema = 'ods'

UNION ALL

SELECT 
    'ods_dbt (dbt test)' AS source,
    COUNT(*) AS tables
FROM information_schema.tables
WHERE table_schema = 'ods_dbt';


-- ============================================================================
-- 2. Volumétrie par table (histolig exemple)
-- ============================================================================
\echo ''
\echo '==================================================================='
\echo '2. VOLUMÉTRIE HISTOLIG'
\echo '==================================================================='

SELECT 
    'ods' AS source,
    COUNT(*) AS total_rows,
    COUNT(*) FILTER (WHERE _etl_valid_from IS NOT NULL) AS with_timestamp,
    NULL::BIGINT AS current_rows,
    NULL::BIGINT AS deleted_rows
FROM ods.histolig

UNION ALL

SELECT 
    'ods_dbt' AS source,
    COUNT(*) AS total_rows,
    COUNT(*) FILTER (WHERE _etl_valid_from IS NOT NULL) AS with_timestamp,
    COUNT(*) FILTER (WHERE _etl_is_current = TRUE) AS current_rows,
    COUNT(*) FILTER (WHERE _etl_is_deleted = TRUE) AS deleted_rows
FROM ods_dbt.histolig;


-- ============================================================================
-- 3. Colonnes SCD2 (ods_dbt uniquement)
-- ============================================================================
\echo ''
\echo '==================================================================='
\echo '3. STATISTIQUES SCD2 (ods_dbt)'
\echo '==================================================================='

SELECT 
    COUNT(*) AS total_versions,
    COUNT(*) FILTER (WHERE _etl_is_current = TRUE) AS versions_courantes,
    COUNT(*) FILTER (WHERE _etl_is_current = FALSE) AS versions_historiques,
    COUNT(*) FILTER (WHERE _etl_is_deleted = TRUE) AS suppressions_detectees,
    COUNT(*) FILTER (WHERE _etl_valid_to IS NOT NULL) AS versions_closes,
    MAX(_etl_valid_from) AS derniere_maj,
    MAX(last_seen_date) AS derniere_apparition
FROM ods_dbt.histolig;


-- ============================================================================
-- 4. Clés métier manquantes (différences)
-- ============================================================================
\echo ''
\echo '==================================================================='
\echo '4. DIFFÉRENCES CLÉS MÉTIER'
\echo '==================================================================='

-- Clés dans ODS mais pas dans ODS_DBT (current)
SELECT 
    'ODS only (manquant ods_dbt)' AS diff,
    COUNT(*) AS count
FROM ods.histolig o
LEFT JOIN ods_dbt.histolig d 
    ON o.uniq_id = d.uniq_id 
    AND d._etl_is_current = TRUE
WHERE d.uniq_id IS NULL

UNION ALL

-- Clés dans ODS_DBT (current) mais pas dans ODS
SELECT 
    'ODS_DBT only (manquant ods)' AS diff,
    COUNT(*) AS count
FROM ods_dbt.histolig d
LEFT JOIN ods.histolig o 
    ON d.uniq_id = o.uniq_id
WHERE o.uniq_id IS NULL
  AND d._etl_is_current = TRUE;


-- ============================================================================
-- 5. Échantillon données (5 premières lignes)
-- ============================================================================
\echo ''
\echo '==================================================================='
\echo '5. ÉCHANTILLON ODS_DBT (5 lignes)'
\echo '==================================================================='

SELECT 
    uniq_id,
    dat_mvt,
    _etl_valid_from,
    _etl_valid_to,
    _etl_is_current,
    _etl_is_deleted,
    last_seen_date
FROM ods_dbt.histolig
ORDER BY _etl_valid_from DESC
LIMIT 5;


-- ============================================================================
-- 6. Historique versions (exemple 1 clé)
-- ============================================================================
\echo ''
\echo '==================================================================='
\echo '6. HISTORIQUE VERSIONS (exemple 1 clé)'
\echo '==================================================================='

WITH sample_key AS (
    SELECT uniq_id 
    FROM ods_dbt.histolig 
    WHERE _etl_is_current = FALSE
    LIMIT 1
)
SELECT 
    h.uniq_id,
    h._etl_valid_from,
    h._etl_valid_to,
    h._etl_is_current,
    h._etl_is_deleted,
    EXTRACT(EPOCH FROM (COALESCE(h._etl_valid_to, NOW()) - h._etl_valid_from)) / 86400 AS duree_jours
FROM ods_dbt.histolig h
INNER JOIN sample_key s ON h.uniq_id = s.uniq_id
ORDER BY h._etl_valid_from;


-- ============================================================================
-- 7. Performances requête (EXPLAIN ANALYZE)
-- ============================================================================
\echo ''
\echo '==================================================================='
\echo '7. PERFORMANCES REQUÊTE (récents > 2024-01-01)'
\echo '==================================================================='

\echo ''
\echo '[ODS Python prod]'
EXPLAIN ANALYZE
SELECT COUNT(*) 
FROM ods.histolig 
WHERE dat_mvt >= '2024-01-01';

\echo ''
\echo '[ODS_DBT dbt test]'
EXPLAIN ANALYZE
SELECT COUNT(*) 
FROM ods_dbt.histolig 
WHERE dat_mvt >= '2024-01-01' 
  AND _etl_is_current = TRUE;


-- ============================================================================
-- 8. Taille stockage
-- ============================================================================
\echo ''
\echo '==================================================================='
\echo '8. TAILLE STOCKAGE'
\echo '==================================================================='

SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS taille_totale,
    pg_size_pretty(pg_relation_size(schemaname||'.'||tablename)) AS taille_table,
    pg_size_pretty(pg_indexes_size(schemaname||'.'||tablename)) AS taille_index
FROM pg_tables
WHERE schemaname IN ('ods', 'ods_dbt')
  AND tablename = 'histolig'
ORDER BY schemaname;


\echo ''
\echo '==================================================================='
\echo 'FIN COMPARAISON'
\echo '==================================================================='