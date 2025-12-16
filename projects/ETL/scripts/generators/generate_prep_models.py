"""
============================================================================
Script de génération automatique des modèles dbt PREP - VERSION DYNAMIQUE
============================================================================
Stratégie :
- Détection automatique PRIMARY KEY depuis PostgreSQL
- Détection colonnes ETL (_etl_*) présentes dans ODS
- Réplication des index ODS → PREP
- Génération conditionnelle MERGE incrémental
- DELETE orphelins SEULEMENT en mode incremental
============================================================================
"""

import os
import sys
import psycopg2
from pathlib import Path
from typing import List, Dict, Tuple, Optional, Set
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()


class DatabaseConfig:
    """Configuration de la base de données"""
    def __init__(self):
        self.host = os.getenv('POSTGRES_HOST', 'localhost')
        self.port = int(os.getenv('POSTGRES_PORT', 5432))
        self.database = os.getenv('POSTGRES_DATABASE', 'etl_db')
        self.user = os.getenv('POSTGRES_USER', 'postgres')
        self.password = os.getenv('DBT_POSTGRES_PASSWORD')
        
    def get_connection_string(self) -> str:
        return f"host={self.host} port={self.port} dbname={self.database} user={self.user} password={self.password}"


class TableMetadata:
    """Métadonnées d'une table ODS"""
    
    def __init__(self, table_name: str, config: DatabaseConfig):
        self.table_name = table_name
        self.config = config
        self.primary_key = None
        self.etl_columns = set()
        self.all_columns = []
        
        self._load_metadata()
    
    def _load_metadata(self):
        """Charge toutes les métadonnées depuis PostgreSQL"""
        conn = psycopg2.connect(self.config.get_connection_string())
        cur = conn.cursor()
        
        try:
            # 1. Récupérer PRIMARY KEY OU UNIQUE INDEX depuis ODS
            cur.execute("""
                SELECT STRING_AGG(a.attname, ', ' ORDER BY array_position(i.indkey, a.attnum))
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                WHERE i.indrelid = ('ods.' || %s)::regclass
                  AND (i.indisprimary OR i.indisunique)
                GROUP BY i.indexrelid
                ORDER BY i.indisprimary DESC
                LIMIT 1
            """, (self.table_name,))
            
            pk_result = cur.fetchone()
            if pk_result and pk_result[0]:
                pk_str = pk_result[0]
                # Si c'est une clé composite, parser en liste
                if ',' in pk_str:
                    self.primary_key = [col.strip() for col in pk_str.split(',')]
                    print(f"  🔑 Composite key detected: {self.primary_key}")
                else:
                    self.primary_key = pk_str
                    print(f"  🔑 Primary key detected: {self.primary_key}")
            else:
                print(f"  ⚠️  No unique key found → TABLE materialization")
                self.primary_key = None
            
            # 2. Récupérer toutes les colonnes
            cur.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'ods'
                  AND table_name = %s
                ORDER BY ordinal_position
            """, (self.table_name,))
            
            self.all_columns = [row[0] for row in cur.fetchall()]
            
            # 3. Identifier les colonnes ETL présentes
            for col in self.all_columns:
                if col.startswith('_etl_'):
                    self.etl_columns.add(col)
            
            if self.etl_columns:
                print(f"  📊 ETL columns found: {', '.join(sorted(self.etl_columns))}")
            
        finally:
            cur.close()
            conn.close()


class IndexReplicator:
    """Gère la réplication des index depuis ODS vers PREP"""
    
    def __init__(self, config: DatabaseConfig):
        self.config = config

    def get_ods_indexes(self, table_name: str) -> List[str]:
        """Récupère les index ODS business (pas techniques ETL)"""
        conn = psycopg2.connect(self.config.get_connection_string())
        cur = conn.cursor()
        
        try:
            cur.execute("""
                SELECT 
                    indexname,
                    indexdef
                FROM pg_indexes
                WHERE schemaname = 'ods' 
                AND tablename = %s
                ORDER BY indexname
            """, (table_name,))
            
            indexes = []
            
            for idx_name, idx_def in cur.fetchall():
                # Ignorer les index techniques ETL
                if any(etl_col in idx_def.lower() for etl_col in 
                    ['_etl_valid_to', 'idx_ods_', 'upsert']):
                    print(f"  ⏭️  Skipping ETL index: {idx_name}")
                    continue
                
                # Adapter l'index pour PREP
                adapted_idx = idx_def.replace(f'ON ods.{table_name}', 'ON {{ this }}')
                
                # Remplacer _etl_valid_from par _etl_source_timestamp
                adapted_idx = adapted_idx.replace('_etl_valid_from', '_etl_source_timestamp')
                
                # Ajouter IF NOT EXISTS
                if 'IF NOT EXISTS' not in adapted_idx:
                    adapted_idx = adapted_idx.replace('CREATE INDEX', 'CREATE INDEX IF NOT EXISTS')
                    adapted_idx = adapted_idx.replace('CREATE UNIQUE INDEX', 'CREATE UNIQUE INDEX IF NOT EXISTS')
                
                indexes.append(adapted_idx)
                print(f"  ✅ Replicated index: {idx_name}")
            
            # Ajouter ANALYZE
            if indexes:
                indexes.append('ANALYZE {{ this }}')
            
            return indexes
            
        finally:
            cur.close()
            conn.close()


class ColumnAnalyzer:
    """Analyse les colonnes pour déterminer leur utilité"""
    
    def __init__(self, table_name: str, config: DatabaseConfig):
        self.table_name = table_name
        self.config = config
        
    def analyze_columns(self) -> Dict:
        """Analyse toutes les colonnes de la table ODS"""
        conn = psycopg2.connect(self.config.get_connection_string())
        cur = conn.cursor()
        
        try:
            # Récupérer les colonnes
            cur.execute("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = 'ods'
                AND table_name = %s
                ORDER BY ordinal_position
            """, (self.table_name,))
            
            columns = cur.fetchall()
            
            # Compter les lignes
            cur.execute(f'SELECT COUNT(*) FROM ods.{self.table_name}')
            total_rows = cur.fetchone()[0]
            
            analysis = {
                'total_rows': total_rows,
                'total_columns': len(columns),
                'columns': {},
                'excluded_etl': 0,
                'excluded_null': 0,
                'excluded_constant': 0,
                'excluded_low_value': 0,
                'kept_columns': 0
            }
            
            print(f"\n📊 Analyzing {len(columns)} columns in ods.{self.table_name} ({total_rows:,} rows)...")
            
            # Optimisation : Limiter l'échantillon pour grandes tables
            sample_clause = ""
            if total_rows > 100000:
                sample_size = min(100000, total_rows)
                sample_clause = f"TABLESAMPLE SYSTEM ((100000.0 / {total_rows}) * 100)"
                print(f"  ⚡ Using sampling ({sample_size:,} rows) for performance")
            
            for col_name, data_type in columns:
                # Analyse basique : NULL%, cardinalité (avec échantillonnage)
                cur.execute(f"""
                    SELECT 
                        COUNT(*) FILTER (WHERE "{col_name}" IS NULL) * 100.0 / NULLIF(COUNT(*), 0) as null_pct,
                        COUNT(DISTINCT "{col_name}") as cardinality
                    FROM ods.{self.table_name} {sample_clause}
                """)
                
                null_pct, cardinality = cur.fetchone()
                
                analysis['columns'][col_name] = {
                    'data_type': data_type,
                    'null_pct': float(null_pct) if null_pct else 0.0,
                    'cardinality': cardinality or 0
                }
            
            return analysis
            
        finally:
            cur.close()
            conn.close()


class ColumnFilter:
    """Filtre les colonnes selon des règles métier DYNAMIQUES"""
    
    def __init__(self, table_name: str, metadata: TableMetadata):
        self.table_name = table_name
        self.metadata = metadata
        
        # Colonnes ETL à TOUJOURS exclure (techniques SCD2 uniquement)
        self.always_exclude = {
            '_etl_valid_to'  # Seulement valid_to, pas hashdiff
        }
        
        # Préfixes de colonnes métier importantes
        self.important_prefixes = [
            'cod_', 'num_', 'no_',  # Identifiants
            'dat_', 'date_',  # Dates
            'usr_', 'qui_',  # Utilisateurs
            'mnt_', 'qte_', 'px_', 'prix_', 'mt_'  # Montants/quantités
        ]
        
    def should_keep_column(self, col_name: str, col_info: Dict) -> Tuple[bool, str]:
        """Détermine si une colonne doit être gardée"""
        col_lower = col_name.lower()
        
        # 1. TOUJOURS garder la PRIMARY KEY
        if isinstance(self.metadata.primary_key, list):
            if col_name in self.metadata.primary_key:
                return (True, "primary_key")
        elif col_name == self.metadata.primary_key:
            return (True, "primary_key")
        
        # 2. Exclure colonnes techniques SCD2
        if col_name in self.always_exclude:
            return (False, "excluded_etl_scd2")
        
        # 3. Garder les colonnes ETL utiles
        if col_name in self.metadata.etl_columns:
            if col_name in ['_etl_run_id', '_etl_loaded_at', '_etl_valid_from']:
                return (True, "etl_useful")
            return (False, "etl_not_needed")
        
        # 4. Garder si correspond à un préfixe important
        for prefix in self.important_prefixes:
            if col_lower.startswith(prefix):
                return (True, f"pattern_{prefix}")
        
        # 5. Exclure si 100% NULL
        if col_info['null_pct'] >= 100.0:
            return (False, "100%_null")
        
        # 6. Exclure si constante (cardinalité = 1 et non NULL)
        if col_info['cardinality'] == 1 and col_info['null_pct'] < 100.0:
            return (False, "constant")
        
        # 7. Exclure si >95% NULL et faible cardinalité
        if col_info['null_pct'] > 95.0 and col_info['cardinality'] < 5:
            return (False, "low_value")
        
        # 8. Garder par défaut
        return (True, "default_keep")
    
    def filter_columns(self, analysis: Dict) -> Tuple[List[str], Dict]:
        """Filtre les colonnes selon les règles"""
        useful_columns = []
        
        for col_name, col_info in analysis['columns'].items():
            keep, reason = self.should_keep_column(col_name, col_info)
            
            if keep:
                useful_columns.append(col_name)
                analysis['kept_columns'] += 1
            else:
                if 'etl' in reason:
                    analysis['excluded_etl'] += 1
                elif reason == '100%_null':
                    analysis['excluded_null'] += 1
                elif reason == 'constant':
                    analysis['excluded_constant'] += 1
                elif reason == 'low_value':
                    analysis['excluded_low_value'] += 1
        
        return useful_columns, analysis

def generate_prep_model(
    table_name: str,
    columns: List[str],
    analysis: Dict,
    indexes: List[str],
    metadata: TableMetadata
) -> str:
    """Génère le contenu SQL du modèle PREP avec MERGE incrémental"""

    # ------------------------------------------------------------------
    # Stratégie d'incrémental
    # ------------------------------------------------------------------
    unique_key = metadata.primary_key
    has_etl_valid_from = '_etl_valid_from' in columns

    if not unique_key:
        materialization = 'table'
        incremental_strategy = None
        unique_key_str = None
        unique_key_lower = None
        print(f"  ⚠️  No PRIMARY KEY → TABLE materialization")
    elif isinstance(unique_key, list):
        materialization = 'incremental'
        incremental_strategy = 'merge'
        unique_key_lower = [k.lower() for k in unique_key]
        unique_key_str = str(unique_key_lower)
        print(f"  ✅ MERGE enabled with composite key: {unique_key}")
    else:
        materialization = 'incremental'
        incremental_strategy = 'merge'
        unique_key_lower = unique_key.lower()
        unique_key_str = f"'{unique_key_lower}'"
        print(f"  ✅ MERGE enabled with unique_key: {unique_key}")

    # ------------------------------------------------------------------
    # Config dbt
    # ------------------------------------------------------------------
    config_lines = [f"    materialized='{materialization}',"]

    if incremental_strategy:
        config_lines.extend([
            f"    unique_key={unique_key_str},",
            "    incremental_strategy='merge',",
            "    on_schema_change='sync_all_columns',"
        ])

    # ------------------------------------------------------------------
    # Post-hooks (ORDRE IMPORTANT)
    #   1. DELETE orphelins
    #   2. CREATE INDEX
    #   3. ANALYZE
    # ------------------------------------------------------------------
    post_hooks = []

    # 1️⃣ DELETE orphelins (NULL-safe, scalable)
    if incremental_strategy and unique_key:
        if isinstance(unique_key_lower, list):
            join_conditions = " AND ".join(
                [f"s.{k} = t.{k}" for k in unique_key_lower]
            )
        else:
            join_conditions = f"s.{unique_key_lower} = t.{unique_key_lower}"

        cleanup_sql = (
            "{% if is_incremental() %}"
            f"DELETE FROM {{{{ this }}}} t "
            f"WHERE NOT EXISTS ("
            f"SELECT 1 FROM {{{{ source('ods', '{table_name}') }}}} s "
            f"WHERE {join_conditions}"
            f")"
            "{% endif %}"
        )

        post_hooks.append(f'"{cleanup_sql}"')
        print("  ✅ DELETE orphaned rows enabled (incremental only)")

    # 2️⃣ Index
    if indexes:
        post_hooks.extend([f'"{idx}"' for idx in indexes])

    if post_hooks:
        config_lines.append(
            "    post_hook=[\n        " + ",\n        ".join(post_hooks) + "\n    ]"
        )

    config_block = "{{ config(\n" + "\n".join(config_lines) + "\n) }}"

    # ------------------------------------------------------------------
    # SELECT
    # ------------------------------------------------------------------
    cols_sql = []
    for col in columns:
        alias = col.lower().replace('-', '_')
        if col == '_etl_valid_from':
            cols_sql.append(f'    "{col}" AS _etl_source_timestamp')
        else:
            cols_sql.append(f'    "{col}" AS {alias}')

    cols_sql.append("    CURRENT_TIMESTAMP AS _prep_loaded_at")
    select_block = ",\n".join(cols_sql)

    incremental_where = ""
    if incremental_strategy == 'merge' and has_etl_valid_from:
        incremental_where = """
{% if is_incremental() %}
WHERE "_etl_valid_from" > (
    SELECT COALESCE(MAX(_etl_source_timestamp), '1900-01-01'::timestamp)
    FROM {{ this }}
)
{% endif %}"""

    # ------------------------------------------------------------------
    # SQL FINAL
    # ------------------------------------------------------------------
    sql = f"""{config_block}

/*
============================================================================
PREP MODEL : {table_name}
============================================================================
Generated : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Source    : ods.{table_name}
Rows ODS  : {analysis['total_rows']:,}
Cols ODS  : {analysis['total_columns']}
Cols PREP : {analysis['kept_columns'] + 1} (+ _prep_loaded_at)
Strategy  : {materialization.upper()}
============================================================================
*/

SELECT
{select_block}
FROM {{{{ source('ods', '{table_name}') }}}}{incremental_where}
"""
    return sql



def process_single_table(table_name: str, config: DatabaseConfig, output_dir: Path, 
                         dry_run: bool) -> Tuple[str, Dict]:
    """Traite une seule table"""
    print(f"\n{'='*80}")
    print(f"🔄 Processing table: {table_name}")
    print(f"{'='*80}")
    
    # 1. Charger les métadonnées
    metadata = TableMetadata(table_name, config)
    
    # 2. Analyser les colonnes
    analyzer = ColumnAnalyzer(table_name, config)
    analysis = analyzer.analyze_columns()
    
    # 3. Filtrer les colonnes
    filter_obj = ColumnFilter(table_name, metadata)
    useful_cols, analysis = filter_obj.filter_columns(analysis)
    
    print(f"\n📊 Column Analysis:")
    print(f"  Total columns : {analysis['total_columns']}")
    print(f"  Kept columns  : {analysis['kept_columns']}")
    print(f"  Excluded      : {analysis['total_columns'] - analysis['kept_columns']} ({100*(analysis['total_columns'] - analysis['kept_columns'])/analysis['total_columns']:.1f}%)")
    
    # 4. Récupérer les index ODS
    print(f"\n🔍 Replicating indexes from ODS...")
    index_replicator = IndexReplicator(config)
    indexes = index_replicator.get_ods_indexes(table_name)
    
    if indexes:
        print(f"  ✅ {len(indexes) - 1} business indexes + ANALYZE")
    else:
        print(f"  ⚠️  No indexes found in ODS")
    
    # 5. Générer le modèle SQL
    if not dry_run:
        model_file = output_dir / f"{table_name}.sql"
        content = generate_prep_model(table_name, useful_cols, analysis, indexes, metadata)
        
        with open(model_file, 'w', encoding='utf-8') as f:
            f.write(content)
        
        print(f"\n✅ Generated: {model_file}")
    else:
        print(f"\n🔍 DRY RUN: Would generate {table_name}.sql")
    
    return (table_name, analysis)


def main():
    """Point d'entrée principal"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Génère les modèles dbt PREP (100% dynamique)')
    parser.add_argument('--table', type=str, help='Traiter une seule table')
    parser.add_argument('--dry-run', action='store_true', help='Mode simulation')
    parser.add_argument('--parallel', type=int, default=4, help='Nombre de workers parallèles')
    
    args = parser.parse_args()
    
    # Configuration
    config = DatabaseConfig()
    dbt_project_dir = Path(os.getenv('ETL_DBT_PROJECT', 'E:/Prefect/projects/ETL/dbt/etl_db'))
    output_dir = dbt_project_dir / 'models' / 'prep'
    
    print(f"\n{'='*80}")
    print(f"🚀 PREP Model Generator - 100% DYNAMIC")
    print(f"{'='*80}")
    print(f"Project dir : {dbt_project_dir}")
    print(f"Output dir  : {output_dir}")
    print(f"Mode        : {'DRY RUN' if args.dry_run else 'WRITE'}")
    
    # Créer le répertoire si nécessaire
    if not args.dry_run:
        output_dir.mkdir(parents=True, exist_ok=True)
    
    # Déterminer les tables à traiter
    if args.table:
        tables = [args.table]
    else:
        # Récupérer toutes les tables ODS
        conn = psycopg2.connect(config.get_connection_string())
        cur = conn.cursor()
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'ods'
            ORDER BY table_name
        """)
        tables = [row[0] for row in cur.fetchall()]
        cur.close()
        conn.close()
    
    print(f"\n📋 Tables to process: {len(tables)}")
    
    # Traiter les tables
    all_reports = {}
    
    if args.table or len(tables) == 1:
        # Mode séquentiel pour une seule table
        for table_name in tables:
            table_name, report = process_single_table(
                table_name, config, output_dir, args.dry_run
            )
            all_reports[table_name] = report
    else:
        # Mode parallèle pour plusieurs tables
        with ThreadPoolExecutor(max_workers=args.parallel) as executor:
            futures = {
                executor.submit(
                    process_single_table, 
                    table_name, config, output_dir, args.dry_run
                ): table_name 
                for table_name in tables
            }
            
            for future in as_completed(futures):
                table_name = futures[future]
                try:
                    table_name, report = future.result()
                    all_reports[table_name] = report
                except Exception as exc:
                    print(f"\n❌ Error processing {table_name}: {exc}")
    
    # Résumé final
    print(f"\n{'='*80}")
    print(f"📊 FINAL SUMMARY")
    print(f"{'='*80}")
    print(f"Tables processed : {len(all_reports)}")
    
    total_ods_cols = sum(r['total_columns'] for r in all_reports.values())
    total_prep_cols = sum(r['kept_columns'] for r in all_reports.values())
    
    print(f"Total ODS columns  : {total_ods_cols:,}")
    print(f"Total PREP columns : {total_prep_cols:,}")
    print(f"Reduction          : {100*(total_ods_cols-total_prep_cols)/total_ods_cols:.1f}%")
    
    # Sauvegarder le rapport
    if not args.dry_run:
        report_file = dbt_project_dir / 'PREP_ANALYSIS_REPORT.json'
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(all_reports, f, indent=2)
        print(f"\n✅ Report saved: {report_file}")
    
    print(f"\n{'='*80}")
    print(f"✅ DONE - 100% DYNAMIC")
    print(f"{'='*80}\n")


if __name__ == '__main__':
    main()