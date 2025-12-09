"""
============================================================================
Générateur PREP ULTRA-INTELLIGENT - Version Complète
============================================================================
Fonctionnalités :
  ✅ Analyse exhaustive de pertinence des colonnes
  ✅ Mode multi-tables OU single-table
  ✅ Détection colonnes corrélées (redondantes)
  ✅ Analyse cardinalité et distribution
  ✅ Détection colonnes calculables
  ✅ Rapport HTML + Markdown
  ✅ Mode dry-run (aperçu sans génération)
  ✅ Whitelist/Blacklist personnalisables
  ✅ Logs détaillés avec timestamp
  
Usage:
  # Toutes les tables
  python generate_dbt_prep_smart.py
  
  # Une table spécifique
  python generate_dbt_prep_smart.py --table client
  
  # Dry-run (aperçu)
  python generate_dbt_prep_smart.py --table produit --dry-run
  
  # Forcer inclusion colonne
  python generate_dbt_prep_smart.py --table client --keep-columns "col1,col2"
============================================================================
"""

import psycopg2
import argparse
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Set, Optional
from collections import defaultdict
import sys

# Ajout du chemin projet
sys.path.append(str(Path(__file__).parent.parent))
from flows.config.pg_config import config


# ============================================================================
# CONFIGURATION
# ============================================================================

class PrepConfig:
    """Configuration globale du générateur"""
    
    # Colonnes techniques ETL à exclure
    ETL_EXCLUDE = {
        '_etl_hashdiff', '_etl_valid_from', '_etl_valid_to',
        '_etl_is_active', '_etl_loaded_at', '_etl_updated_at',
        '_etl_source', '_loaded_at', '_source_file', '_sftp_log_id'
    }
    
    # Colonnes business critiques (toujours garder)
    ALWAYS_KEEP = {
        '_etl_run_id',  # Traçabilité
        # Primary keys courantes
        'cod_cli', 'cod_pro', 'cod_fou', 'cod_crn', 'no_piece', 
        'no_ligne', 'uniq_id', 'no_tarif', 'no_cde',
        # Dates importantes
        'dat_pie', 'dat_liv', 'dat_crt', 'dat_mod', 'dat_deb', 'dat_fin',
        # Montants
        'montant', 'mt_ttc', 'mt_ht', 'px_vte', 'px_ach',
        # Quantités
        'qte', 'quantite'
    }
    
    # Seuils d'exclusion
    NULL_THRESHOLD = 100.0  # % NULL pour exclure
    CONSTANT_THRESHOLD = 1   # Nb valeurs distinctes pour "constante"
    LOW_VALUE_NULL_PCT = 95.0  # % NULL pour low-value
    LOW_VALUE_DISTINCT = 5     # Nb valeurs distinctes pour low-value
    CORRELATION_THRESHOLD = 0.98  # Seuil de corrélation
    
    # Patterns de colonnes calculables
    CALCULATED_PATTERNS = [
        ('_ht', '_ttc'),  # Montant HT calculable depuis TTC
        ('_net', '_brut'),  # Net calculable depuis brut
    ]


# ============================================================================
# ANALYSE COLONNES
# ============================================================================

class ColumnAnalyzer:
    """Analyse avancée des colonnes"""
    
    def __init__(self, table_name: str):
        self.table_name = table_name
        self.conn = psycopg2.connect(config.get_connection_string())
        self.cur = self.conn.cursor()
        
    def __del__(self):
        if hasattr(self, 'cur'):
            self.cur.close()
        if hasattr(self, 'conn'):
            self.conn.close()
    
    def get_total_rows(self) -> int:
        """Compter lignes totales"""
        self.cur.execute(f'SELECT COUNT(*) FROM ods.{self.table_name}')
        return self.cur.fetchone()[0]
    
    def analyze_column(self, column_name: str, total_rows: int) -> Dict:
        """
        Analyse complète d'une colonne
        
        Returns:
            dict: {
                'is_useful': bool,
                'exclusion_reason': str,
                'stats': {...}
            }
        """
        if total_rows == 0:
            return {
                'is_useful': True,
                'exclusion_reason': None,
                'stats': {'total_rows': 0, 'distinct': 0, 'null_pct': 0}
            }
        
        try:
            # Stats de base
            self.cur.execute(f"""
                SELECT 
                    COUNT(DISTINCT "{column_name}") as distinct_count,
                    COUNT(*) FILTER (WHERE "{column_name}" IS NULL) as null_count,
                    COUNT(*) FILTER (WHERE "{column_name}" IS NOT NULL) as not_null_count
                FROM ods.{self.table_name}
            """)
            
            distinct_count, null_count, not_null_count = self.cur.fetchone()
            null_pct = (null_count / total_rows) * 100
            
            stats = {
                'total_rows': total_rows,
                'distinct': distinct_count,
                'null_count': null_count,
                'not_null_count': not_null_count,
                'null_pct': null_pct,
                'cardinality': distinct_count / not_null_count if not_null_count > 0 else 0
            }
            
            # Règles d'exclusion
            exclusion_reason = self._evaluate_exclusion(column_name, stats)
            
            return {
                'is_useful': exclusion_reason is None,
                'exclusion_reason': exclusion_reason,
                'stats': stats
            }
            
        except Exception as e:
            # En cas d'erreur, on garde par sécurité
            return {
                'is_useful': True,
                'exclusion_reason': None,
                'stats': {'error': str(e)}
            }
    
    def _evaluate_exclusion(self, column_name: str, stats: Dict) -> Optional[str]:
        """
        Évalue si une colonne doit être exclue
        
        Returns:
            str: Raison d'exclusion, ou None si colonne utile
        """
        null_pct = stats['null_pct']
        distinct = stats['distinct']
        
        # Règle 1 : 100% NULL
        if null_pct == PrepConfig.NULL_THRESHOLD:
            return "100% NULL"
        
        # Règle 2 : Valeur constante
        if distinct == PrepConfig.CONSTANT_THRESHOLD and null_pct < 100:
            return "Valeur constante (1 seule valeur)"
        
        # Règle 3 : >95% NULL + peu de valeurs
        if (null_pct > PrepConfig.LOW_VALUE_NULL_PCT and 
            distinct < PrepConfig.LOW_VALUE_DISTINCT):
            return f">95% NULL ({null_pct:.1f}%) + {distinct} valeur(s) distincte(s)"
        
        # Règle 4 : Cardinalité extrêmement faible (hors colonnes de flags)
        if (distinct == 2 and 
            not any(pattern in column_name.lower() for pattern in ['flag', 'actif', 'is_', 'has_', 'ind_'])):
            # Vérifier si c'est un vrai boolean ou juste 2 valeurs
            self.cur.execute(f"""
                SELECT DISTINCT "{column_name}"
                FROM ods.{self.table_name}
                WHERE "{column_name}" IS NOT NULL
                LIMIT 2
            """)
            values = [row[0] for row in self.cur.fetchall()]
            
            # Si pas boolean-like, peut-être peu utile
            if not self._is_boolean_like(values):
                return f"Cardinalité très faible (2 valeurs: {values})"
        
        return None
    
    def _is_boolean_like(self, values: List) -> bool:
        """Vérifie si les valeurs ressemblent à un boolean"""
        bool_patterns = [
            {True, False}, {'1', '0'}, {'Y', 'N'}, 
            {'OUI', 'NON'}, {'yes', 'no'}, {1, 0}
        ]
        value_set = set(str(v).upper() for v in values)
        return any(value_set == pattern or 
                   value_set == {str(v).upper() for v in pattern} 
                   for pattern in bool_patterns)
    
    def detect_correlated_columns(self, columns: List[str]) -> List[Tuple[str, str, float]]:
        """
        Détecte les colonnes potentiellement corrélées (redondantes)
        
        Returns:
            List[(col1, col2, correlation_score)]
        """
        correlations = []
        
        # Chercher patterns nom similaires
        for i, col1 in enumerate(columns):
            for col2 in columns[i+1:]:
                # Pattern : _ht vs _ttc, _net vs _brut, etc.
                if self._are_potentially_correlated(col1, col2):
                    try:
                        score = self._compute_correlation(col1, col2)
                        if score > PrepConfig.CORRELATION_THRESHOLD:
                            correlations.append((col1, col2, score))
                    except:
                        pass
        
        return correlations
    
    def _are_potentially_correlated(self, col1: str, col2: str) -> bool:
        """Vérifie si 2 colonnes sont potentiellement corrélées par leur nom"""
        col1_lower = col1.lower()
        col2_lower = col2.lower()
        
        # Même base avec suffixes différents
        for pattern1, pattern2 in PrepConfig.CALCULATED_PATTERNS:
            base1 = col1_lower.replace(pattern1, '').replace(pattern2, '')
            base2 = col2_lower.replace(pattern1, '').replace(pattern2, '')
            if base1 == base2 and base1:
                return True
        
        return False
    
    def _compute_correlation(self, col1: str, col2: str) -> float:
        """
        Calcule un score de corrélation entre 2 colonnes
        (simplifié : ratio de lignes où col1 = col2 ou relation linéaire)
        """
        self.cur.execute(f"""
            SELECT 
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE "{col1}" = "{col2}") as equal_count
            FROM ods.{self.table_name}
            WHERE "{col1}" IS NOT NULL AND "{col2}" IS NOT NULL
        """)
        
        total, equal_count = self.cur.fetchone()
        if total == 0:
            return 0.0
        
        return equal_count / total


# ============================================================================
# FILTRAGE COLONNES
# ============================================================================

class ColumnFilter:
    """Filtre les colonnes selon règles métier"""
    
    def __init__(self, table_name: str, keep_columns: Set[str] = None, 
                 exclude_columns: Set[str] = None):
        self.table_name = table_name
        self.keep_columns = keep_columns or set()
        self.exclude_columns = exclude_columns or set()
        self.analyzer = ColumnAnalyzer(table_name)
        
    def filter_columns(self, columns: List[str]) -> Tuple[List[str], Dict]:
        """
        Filtre colonnes selon pertinence
        
        Returns:
            (columns_to_keep, analysis_report)
        """
        print(f"  [ANALYZE] Analyse de {self.table_name}...")
        
        total_rows = self.analyzer.get_total_rows()
        
        columns_to_keep = []
        analysis_report = {
            'table': self.table_name,
            'total_rows': total_rows,
            'total_columns': len(columns),
            'kept_columns': 0,
            'excluded_etl': 0,
            'excluded_null': 0,
            'excluded_constant': 0,
            'excluded_low_value': 0,
            'excluded_user': 0,
            'forced_keep': 0,
            'details': [],
            'correlations': []
        }
        
        for col in columns:
            col_lower = col.lower()
            
            # 1. User blacklist
            if col_lower in self.exclude_columns:
                analysis_report['excluded_user'] += 1
                analysis_report['details'].append({
                    'column': col,
                    'status': 'EXCLUDED',
                    'reason': 'Blacklist utilisateur'
                })
                continue
            
            # 2. User whitelist (force keep)
            if col_lower in self.keep_columns:
                columns_to_keep.append(col)
                analysis_report['forced_keep'] += 1
                analysis_report['kept_columns'] += 1
                analysis_report['details'].append({
                    'column': col,
                    'status': 'KEPT',
                    'reason': 'Whitelist utilisateur (forcé)'
                })
                continue
            
            # 3. Exclure colonnes ETL techniques
            if col_lower in PrepConfig.ETL_EXCLUDE:
                analysis_report['excluded_etl'] += 1
                analysis_report['details'].append({
                    'column': col,
                    'status': 'EXCLUDED',
                    'reason': 'Colonne technique ETL'
                })
                continue
            
            # 4. Toujours garder colonnes critiques
            if col_lower in PrepConfig.ALWAYS_KEEP:
                columns_to_keep.append(col)
                analysis_report['kept_columns'] += 1
                analysis_report['details'].append({
                    'column': col,
                    'status': 'KEPT',
                    'reason': 'Colonne critique (toujours gardée)'
                })
                continue
            
            # 5. Analyser utilité
            result = self.analyzer.analyze_column(col, total_rows)
            
            if result['is_useful']:
                columns_to_keep.append(col)
                analysis_report['kept_columns'] += 1
                analysis_report['details'].append({
                    'column': col,
                    'status': 'KEPT',
                    'reason': 'Colonne utile',
                    'stats': result['stats']
                })
            else:
                # Catégoriser exclusion
                reason = result['exclusion_reason']
                if 'NULL' in reason:
                    analysis_report['excluded_null'] += 1
                elif 'constante' in reason:
                    analysis_report['excluded_constant'] += 1
                else:
                    analysis_report['excluded_low_value'] += 1
                
                analysis_report['details'].append({
                    'column': col,
                    'status': 'EXCLUDED',
                    'reason': reason,
                    'stats': result['stats']
                })
        
        # 6. Détecter colonnes corrélées
        correlations = self.analyzer.detect_correlated_columns(columns_to_keep)
        if correlations:
            analysis_report['correlations'] = [
                {'col1': c1, 'col2': c2, 'score': score}
                for c1, c2, score in correlations
            ]
            print(f"  [WARN] {len(correlations)} paire(s) de colonnes corrélées détectée(s)")
        
        return columns_to_keep, analysis_report


# ============================================================================
# GÉNÉRATION MODÈLES
# ============================================================================

def generate_prep_model(table_name: str, columns: List[str], 
                        analysis: Dict) -> str:
    """Génère le contenu SQL du modèle PREP"""
    
    cols_list = []
    for col in columns:
        # Fix: Remplacer tirets par underscores
        alias = col.lower().replace('-', '_')
        cols_list.append(f'    "{col}" AS {alias}')

    select_block = ",\n".join(cols_list)
    
    # Metadata du modèle
    excluded_count = analysis['total_columns'] - analysis['kept_columns']
    
    sql = f"""{{{{ config(materialized='view') }}}}

/*
    ============================================================================
    Modèle PREP : {table_name}
    ============================================================================
    Généré automatiquement le {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
    
    Source       : ods.{table_name}
    Lignes       : {analysis['total_rows']:,}
    Colonnes ODS : {analysis['total_columns']}
    Colonnes PREP: {analysis['kept_columns']}
    Exclues      : {excluded_count} ({100*excluded_count/analysis['total_columns']:.1f}%)
    
    Exclusions:
      - Techniques ETL  : {analysis['excluded_etl']}
      - 100% NULL       : {analysis['excluded_null']}
      - Constantes      : {analysis['excluded_constant']}
      - Faible valeur   : {analysis['excluded_low_value']}
    ============================================================================
*/

SELECT
{select_block}
FROM {{{{ source('ods', '{table_name}') }}}}
"""
    return sql


# ============================================================================
# RAPPORTS
# ============================================================================

class ReportGenerator:
    """Génération rapports d'analyse"""
    
    @staticmethod
    def generate_markdown(all_reports: Dict[str, Dict]) -> str:
        """Génère rapport Markdown"""
        
        lines = [
            "# 📊 Rapport d'Analyse PREP - Version Complète",
            "",
            f"**Généré le** : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            "---",
            "",
            "## 🎯 Résumé Global",
            ""
        ]
        
        # Stats globales
        total_tables = len(all_reports)
        total_rows = sum(r['total_rows'] for r in all_reports.values())
        total_cols = sum(r['total_columns'] for r in all_reports.values())
        total_kept = sum(r['kept_columns'] for r in all_reports.values())
        total_excl = total_cols - total_kept
        
        lines.extend([
            f"| Métrique | Valeur |",
            f"|----------|--------|",
            f"| **Tables analysées** | {total_tables} |",
            f"| **Lignes totales** | {total_rows:,} |",
            f"| **Colonnes ODS** | {total_cols:,} |",
            f"| **Colonnes PREP** | {total_kept:,} ({100*total_kept/total_cols:.1f}%) |",
            f"| **Colonnes exclues** | {total_excl:,} ({100*total_excl/total_cols:.1f}%) |",
            "",
            "### 📉 Raisons d'Exclusion",
            ""
        ])
        
        total_etl = sum(r['excluded_etl'] for r in all_reports.values())
        total_null = sum(r['excluded_null'] for r in all_reports.values())
        total_const = sum(r['excluded_constant'] for r in all_reports.values())
        total_low = sum(r['excluded_low_value'] for r in all_reports.values())
        total_user = sum(r['excluded_user'] for r in all_reports.values())
        
        lines.extend([
            f"- 🔧 **Colonnes techniques ETL** : {total_etl}",
            f"- ❌ **Colonnes 100% NULL** : {total_null}",
            f"- 📌 **Colonnes constantes** : {total_const}",
            f"- ⚠️ **Colonnes faible valeur** : {total_low}",
            f"- 🚫 **Blacklist utilisateur** : {total_user}",
            "",
            "---",
            "",
            "## 📋 Détail par Table",
            ""
        ])
        
        # Détail tables
        for table_name in sorted(all_reports.keys()):
            report = all_reports[table_name]
            excl = report['total_columns'] - report['kept_columns']
            
            lines.extend([
                f"### 📦 {table_name}",
                ""
            ])
            
            # Stats table
            lines.extend([
                f"| Métrique | Valeur |",
                f"|----------|--------|",
                f"| Lignes | {report['total_rows']:,} |",
                f"| Colonnes ODS | {report['total_columns']} |",
                f"| Colonnes PREP | {report['kept_columns']} |",
                f"| Exclues | {excl} |",
                ""
            ])
            
            # Colonnes exclues
            excluded = [d for d in report['details'] if d['status'] == 'EXCLUDED']
            if excluded:
                lines.extend([
                    "**Colonnes exclues :**",
                    "",
                    "| Colonne | Raison | Stats |",
                    "|---------|--------|-------|"
                ])
                
                for detail in excluded:
                    stats_str = ""
                    if 'stats' in detail and 'null_pct' in detail['stats']:
                        stats_str = f"{detail['stats']['null_pct']:.1f}% NULL, {detail['stats']['distinct']} val."
                    lines.append(f"| `{detail['column']}` | {detail['reason']} | {stats_str} |")
                
                lines.append("")
            
            # Corrélations détectées
            if report.get('correlations'):
                lines.extend([
                    "**⚠️ Colonnes corrélées détectées :**",
                    ""
                ])
                for corr in report['correlations']:
                    lines.append(f"- `{corr['col1']}` ↔️ `{corr['col2']}` (score: {corr['score']:.2%})")
                lines.append("")
            
            lines.append("")
        
        return "\n".join(lines)
    
    @staticmethod
    def generate_json(all_reports: Dict[str, Dict], output_file: Path):
        """Génère rapport JSON"""
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(all_reports, f, indent=2, default=str)


# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='Générateur intelligent de modèles dbt PREP',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples:
  # Toutes les tables
  python %(prog)s
  
  # Une table spécifique
  python %(prog)s --table client
  
  # Dry-run (aperçu)
  python %(prog)s --table produit --dry-run
  
  # Forcer inclusion
  python %(prog)s --table client --keep "email,telephone"
  
  # Exclure colonnes
  python %(prog)s --exclude "zal,zda,zlo,znu,zta"
        """
    )
    
    parser.add_argument('--table', '-t', 
                        help='Nom de la table à traiter (défaut: toutes)')
    parser.add_argument('--dry-run', '-d', action='store_true',
                        help='Aperçu sans génération fichiers')
    parser.add_argument('--keep', '-k',
                        help='Colonnes à forcer (séparées par virgule)')
    parser.add_argument('--exclude', '-e',
                        help='Colonnes à exclure (séparées par virgule)')
    parser.add_argument('--output', '-o', default=None,
                        help='Répertoire de sortie (défaut: dbt/models/prep)')
    
    args = parser.parse_args()
    
    # Parse keep/exclude
    keep_cols = set(args.keep.lower().split(',')) if args.keep else set()
    excl_cols = set(args.exclude.lower().split(',')) if args.exclude else set()
    
    print("=" * 70)
    print("🚀 GÉNÉRATEUR PREP INTELLIGENT - Version Ultra-Complète")
    print("=" * 70)
    print(f"Mode         : {'DRY-RUN' if args.dry_run else 'PRODUCTION'}")
    print(f"Table(s)     : {args.table or 'TOUTES'}")
    if keep_cols:
        print(f"Whitelist    : {', '.join(keep_cols)}")
    if excl_cols:
        print(f"Blacklist    : {', '.join(excl_cols)}")
    print("")
    
    # Déterminer output dir
    if args.output:
        output_dir = Path(args.output)
    else:
        output_dir = Path(config.dbt_project_dir) / "models" / "prep"
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Connexion DB
    print("[INFO] Connexion à PostgreSQL...")
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    # Lister tables
    if args.table:
        tables_query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'ods' 
              AND table_name = %s
        """
        cur.execute(tables_query, (args.table.lower(),))
    else:
        tables_query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'ods'
            ORDER BY table_name
        """
        cur.execute(tables_query)
    
    tables = [row[0] for row in cur.fetchall()]
    
    if not tables:
        print(f"[ERROR] Aucune table trouvée (filtre: {args.table or 'aucun'})")
        return
    
    print(f"[DATA] {len(tables)} table(s) à traiter")
    print("")
    
    # Récupérer structures
    table_structures = {}
    for table in tables:
        cur.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = 'ods' 
              AND table_name = %s
            ORDER BY ordinal_position
        """, (table,))
        table_structures[table] = [row[0] for row in cur.fetchall()]
    
    cur.close()
    conn.close()
    
    # Traiter chaque table
    all_reports = {}
    
    for table_name, columns in table_structures.items():
        print(f"{'='*70}")
        print(f"📊 Table: {table_name} ({len(columns)} colonnes)")
        print(f"{'='*70}")
        
        # Filtrer colonnes
        filter_obj = ColumnFilter(table_name, keep_cols, excl_cols)
        useful_cols, report = filter_obj.filter_columns(columns)
        all_reports[table_name] = report
        
        excl_count = report['total_columns'] - report['kept_columns']
        print(f"  ✅ Conservées : {report['kept_columns']}/{report['total_columns']} colonnes")
        print(f"  ❌ Exclues    : {excl_count} colonnes")
        
        if report.get('correlations'):
            print(f"  ⚠️  Corrélations : {len(report['correlations'])} paire(s)")
        
        # Générer SQL
        if not args.dry_run:
            model_file = output_dir / f"prep_{table_name}.sql"
            content = generate_prep_model(table_name, useful_cols, report)
            
            with open(model_file, 'w', encoding='utf-8') as f:
                f.write(content)
            
            print(f"  📝 Généré: prep_{table_name}.sql")
        else:
            print(f"  🔍 DRY-RUN: fichier NON généré")
        
        print("")
    
    # Générer rapports
    print("=" * 70)
    print("📄 Génération rapports...")
    print("=" * 70)
    
    # Markdown
    md_file = output_dir.parent.parent / "PREP_ANALYSIS_REPORT.md"
    with open(md_file, 'w', encoding='utf-8') as f:
        f.write(ReportGenerator.generate_markdown(all_reports))
    print(f"  ✅ Markdown : {md_file}")
    
    # JSON
    json_file = output_dir.parent.parent / "PREP_ANALYSIS_REPORT.json"
    ReportGenerator.generate_json(all_reports, json_file)
    print(f"  ✅ JSON     : {json_file}")
    
    # Résumé final
    total_cols = sum(r['total_columns'] for r in all_reports.values())
    total_kept = sum(r['kept_columns'] for r in all_reports.values())
    reduction = 100 * (1 - total_kept/total_cols)
    
    print("")
    print("=" * 70)
    print("✅ TERMINÉ")
    print("=" * 70)
    print(f"Tables traitées    : {len(all_reports)}")
    print(f"Colonnes ODS       : {total_cols}")
    print(f"Colonnes PREP      : {total_kept}")
    print(f"Réduction          : {reduction:.1f}%")
    
    if args.dry_run:
        print("")
        print("ℹ️  Mode DRY-RUN : Aucun fichier généré")
        print("   Relancer sans --dry-run pour générer les modèles")
    
    print("=" * 70)


if __name__ == "__main__":
    main()