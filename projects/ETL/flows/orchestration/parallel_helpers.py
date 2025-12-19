"""
============================================================================
Helpers pour parallélisation intelligente
============================================================================
"""

def group_tables_by_size(tables: list, table_sizes: dict) -> dict:
    """
    Groupe les tables par taille pour parallélisation optimale
    
    Seuils :
    - Small  : < 10,000 lignes  -> Parallèle max
    - Medium : 10K - 50K lignes -> Parallèle limité (3 threads)
    - Large  : > 50K lignes     -> Séquentiel
    """
    groups = {
        'small': [],
        'medium': [],
        'large': []
    }
    
    for table in tables:
        row_count = table_sizes.get(table, 0)
        
        if row_count < 10_000:
            groups['small'].append(table)
        elif row_count < 50_000:
            groups['medium'].append(table)
        else:
            groups['large'].append(table)
    
    return groups


def log_grouping_info(groups: dict, table_sizes: dict, logger):
    """Affiche les infos de groupement dans les logs"""
    logger.info("=" * 70)
    logger.info("[PARALLEL] Groupement par taille :")
    
    for group_name, tables in groups.items():
        if not tables:
            continue
        
        total_rows = sum(table_sizes.get(t, 0) for t in tables)
        
        logger.info(
            f"  [{group_name.upper():6s}] : "
            f"{len(tables):2d} tables, "
            f"{total_rows:,} lignes -> {tables}"
        )
    
    logger.info("=" * 70)