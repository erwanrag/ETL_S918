"""
============================================================================
Script de nettoyage des émojis - Compatible Windows cp1252
============================================================================
Fichier : utils/clean_emojis.py

Remplace tous les émojis dans les fichiers Python par des équivalents texte
pour résoudre les problèmes d'encodage Windows.

USAGE:
    python utils/clean_emojis.py --dry-run    # Voir les changements sans modifier
    python utils/clean_emojis.py              # Appliquer les changements
============================================================================
"""

import os
import re
from pathlib import Path
from typing import Dict, List, Tuple


# ============================================================================
# MAPPING ÉMOJIS → TEXTE
# ============================================================================

EMOJI_REPLACEMENTS = {
    # Succès / Validation
    '[OK]': '[OK]',
    '[OK]': '[OK]',
    '[SUCCESS]': '[SUCCESS]',
    '[TARGET]': '[TARGET]',
    
    # Erreurs / Avertissements
    '[ERROR]': '[ERROR]',
    '[WARN]': '[WARN]',
    '[ERROR]': '[ERROR]',
    '[CRITICAL]': '[CRITICAL]',
    
    # Informations
    '[INFO]': '[INFO]',
    '[TIP]': '[TIP]',
    '[NOTE]': '[NOTE]',
    '[LIST]': '[LIST]',
    '[DATA]': '[DATA]',
    '[STATS]': '[STATS]',
    '[STATS]': '[STATS]',
    
    # Actions
    '[START]': '[START]',
    '[SYNC]': '[SYNC]',
    '[CONFIG]': '[CONFIG]',
    '[SETTINGS]': '[SETTINGS]',
    '[TOOLS]': '[TOOLS]',
    '[DROP]': '[DROP]',
    '[BUILD]': '[BUILD]',
    
    # Étapes
    '[1]': '[1]',
    '[2]': '[2]',
    '[3]': '[3]',
    '[4]': '[4]',
    '[5]': '[5]',
    
    # Fichiers / Dossiers
    '[FOLDER]': '[FOLDER]',
    '[OPEN]': '[OPEN]',
    '[FILE]': '[FILE]',
    '[PACKAGE]': '[PACKAGE]',
    
    # Base de données
    '[SAVE]': '[SAVE]',
    '[DB]': '[DB]',
    '[KEY]': '[KEY]',
    '[INDEX]': '[INDEX]',
    
    # Temps
    '[TIME]': '[TIME]',
    '[TIMER]': '[TIMER]',
    '[SKIP]': '[SKIP]',
    '[PAUSE]': '[PAUSE]',
    '[STOP]': '[STOP]',
    
    # Réseau
    '[WEB]': '[WEB]',
    '[NETWORK]': '[NETWORK]',
    '[CONNECT]': '[CONNECT]',
    
    # Autres
    '[STYLE]': '[STYLE]',
    '[SEARCH]': '[SEARCH]',
    '[MERGE]': '[MERGE]',
    '[ADD]': '[ADD]',
    '[REMOVE]': '[REMOVE]',
    '[NEW]': '[NEW]',
    '[NEW]': '[NEW]',
    '[AUTO]': '[AUTO]',
    '[VIEW]': '[VIEW]',
    '[COMMENT]': '[COMMENT]',
    '[TAG]': '[TAG]',
    '[-]': '[-]',
    '[-]': '[-]',
    '[PLAY]': '[PLAY]',
    '[STAR]': '[STAR]',
    '[STRONG]': '[STRONG]',
}


# ============================================================================
# FONCTIONS DE NETTOYAGE
# ============================================================================

def contains_emoji(text: str) -> bool:
    """Vérifier si le texte contient des émojis"""
    for emoji in EMOJI_REPLACEMENTS.keys():
        if emoji in text:
            return True
    return False


def replace_emojis(text: str) -> str:
    """Remplacer tous les émojis par leur équivalent texte"""
    result = text
    for emoji, replacement in EMOJI_REPLACEMENTS.items():
        result = result.replace(emoji, replacement)
    return result


def clean_file(file_path: Path, dry_run: bool = False) -> Tuple[bool, int, List[str]]:
    """
    Nettoyer un fichier Python des émojis
    
    Returns:
        (modified, nb_replacements, changed_lines)
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
    except UnicodeDecodeError:
        # Essayer avec un autre encodage
        try:
            with open(file_path, 'r', encoding='latin-1') as f:
                content = f.read()
        except Exception as e:
            print(f"[ERROR] Impossible de lire {file_path}: {e}")
            return False, 0, []
    
    if not contains_emoji(content):
        return False, 0, []
    
    # Remplacer les émojis
    new_content = replace_emojis(content)
    
    # Compter les changements
    nb_replacements = sum(content.count(emoji) for emoji in EMOJI_REPLACEMENTS.keys())
    
    # Identifier les lignes modifiées
    old_lines = content.split('\n')
    new_lines = new_content.split('\n')
    changed_lines = []
    
    for i, (old, new) in enumerate(zip(old_lines, new_lines), 1):
        if old != new:
            changed_lines.append(f"  Line {i}: {old.strip()[:60]}...")
    
    # Écrire le fichier (si pas dry-run)
    if not dry_run:
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(new_content)
        except Exception as e:
            print(f"[ERROR] Impossible d'écrire {file_path}: {e}")
            return False, 0, []
    
    return True, nb_replacements, changed_lines


def clean_directory(directory: Path, dry_run: bool = False) -> Dict[str, any]:
    """
    Nettoyer récursivement un répertoire
    
    Returns:
        Statistics dict
    """
    stats = {
        'files_scanned': 0,
        'files_modified': 0,
        'total_replacements': 0,
        'modified_files': []
    }
    
    # Parcourir tous les fichiers .py
    for py_file in directory.rglob('*.py'):
        # Ignorer les venv et __pycache__
        if 'venv' in str(py_file) or '__pycache__' in str(py_file):
            continue
        
        stats['files_scanned'] += 1
        
        modified, nb_replacements, changed_lines = clean_file(py_file, dry_run)
        
        if modified:
            stats['files_modified'] += 1
            stats['total_replacements'] += nb_replacements
            stats['modified_files'].append({
                'path': str(py_file.relative_to(directory)),
                'replacements': nb_replacements,
                'lines': changed_lines
            })
    
    return stats


# ============================================================================
# MAIN
# ============================================================================

def main(dry_run: bool = False):
    """Fonction principale"""
    
    # Déterminer le répertoire racine du projet
    script_dir = Path(__file__).parent.parent
    
    print("=" * 70)
    print("NETTOYAGE DES EMOJIS - Projet ETL")
    print("=" * 70)
    print(f"\nRepertoire: {script_dir}")
    print(f"Mode: {'DRY-RUN (simulation)' if dry_run else 'MODIFICATION'}")
    print("\n" + "=" * 70 + "\n")
    
    # Directories à nettoyer
    directories = ['flows', 'tasks', 'utils']
    
    total_stats = {
        'files_scanned': 0,
        'files_modified': 0,
        'total_replacements': 0,
        'modified_files': []
    }
    
    for dir_name in directories:
        dir_path = script_dir / dir_name
        
        if not dir_path.exists():
            print(f"[WARN] Repertoire non trouve: {dir_name}")
            continue
        
        print(f"\n[SCAN] {dir_name}/")
        print("-" * 70)
        
        stats = clean_directory(dir_path, dry_run)
        
        # Agrégation
        total_stats['files_scanned'] += stats['files_scanned']
        total_stats['files_modified'] += stats['files_modified']
        total_stats['total_replacements'] += stats['total_replacements']
        total_stats['modified_files'].extend(stats['modified_files'])
        
        print(f"  Fichiers scannes: {stats['files_scanned']}")
        print(f"  Fichiers modifies: {stats['files_modified']}")
        print(f"  Remplacements: {stats['total_replacements']}")
    
    # Résumé
    print("\n" + "=" * 70)
    print("RESUME")
    print("=" * 70)
    print(f"\nTotal fichiers scannes: {total_stats['files_scanned']}")
    print(f"Total fichiers modifies: {total_stats['files_modified']}")
    print(f"Total remplacements: {total_stats['total_replacements']}")
    
    if total_stats['modified_files']:
        print(f"\n[INFO] FICHIERS MODIFIES ({len(total_stats['modified_files'])}):")
        print("-" * 70)
        for file_info in total_stats['modified_files']:
            print(f"\n{file_info['path']} ({file_info['replacements']} remplacements)")
            if len(file_info['lines']) <= 5:
                for line in file_info['lines']:
                    print(line)
            else:
                for line in file_info['lines'][:3]:
                    print(line)
                print(f"  ... et {len(file_info['lines']) - 3} autres lignes")
    
    if dry_run:
        print("\n" + "=" * 70)
        print("[DRY-RUN] Aucun fichier n'a ete modifie.")
        print("Relancez sans --dry-run pour appliquer les changements.")
        print("=" * 70)
    else:
        print("\n" + "=" * 70)
        print("[OK] Nettoyage termine avec succes !")
        print("=" * 70)
    
    return total_stats


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Nettoyer les emojis des fichiers Python",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples:
  python utils/clean_emojis.py --dry-run    # Simulation
  python utils/clean_emojis.py              # Application
        """
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Simuler sans modifier les fichiers'
    )
    
    args = parser.parse_args()
    
    try:
        main(dry_run=args.dry_run)
    except KeyboardInterrupt:
        print("\n\n[STOP] Interruption utilisateur (Ctrl+C)")
    except Exception as e:
        print(f"\n[ERROR] Erreur inattendue: {e}")
        import traceback
        traceback.print_exc()