"""
============================================================================
Script Test - Validation Corrections ConfigName
============================================================================
Fichier : E:/Prefect/projects/ETL/tests/test_configname_fix.py

Tests unitaires et d'intégration pour valider les corrections
============================================================================
"""

import sys
from pathlib import Path

sys.path.append(r'E:\Prefect\projects\ETL')

# ============================================================================
# TEST 1 : Parser Filename
# ============================================================================

def test_parser():
    """Test parsing noms fichiers"""
    print("="*80)
    print("TEST 1 : Parser Filename")
    print("="*80)
    
    from utils.filename_parser import parse_parquet_filename, parse_and_resolve
    
    # Test cas simples
    test_cases = [
        {
            'filename': 'client_20241201.parquet',
            'metadata': {'table_name': 'client'},
            'expected_physical': 'client'
        },
        {
            'filename': 'client_20241201_143022.parquet',
            'metadata': {'table_name': 'client'},
            'expected_physical': 'client'
        },
        {
            'filename': 'lisval_fou_production_20241201.parquet',
            'metadata': {
                'table_name': 'lisval',
                'config_name': 'lisval_fou_production'
            },
            'expected_physical': 'lisval_fou_production'
        },
        {
            'filename': 'lisval_20241201.parquet',
            'metadata': {
                'table_name': 'lisval',
                'config_name': 'lisval_fou_production'
            },
            'expected_physical': 'lisval_fou_production'
        }
    ]
    
    all_passed = True
    
    for i, case in enumerate(test_cases, 1):
        print(f"\n[Test {i}] {case['filename']}")
        
        result = parse_and_resolve(
            case['filename'],
            case['metadata']
        )
        
        print(f"  File ID     : {result['table_identifier']}")
        print(f"  Table Name  : {result['table_name']}")
        print(f"  Config Name : {result['config_name']}")
        print(f"  Physical    : {result['physical_name']}")
        print(f"  Valid       : {result['is_valid']}")
        
        if result['validation_message']:
            print(f"  Message     : {result['validation_message']}")
        
        # Vérifier résultat attendu
        if result['physical_name'] == case['expected_physical']:
            print(f"  ✅ PASS")
        else:
            print(f"  ❌ FAIL - Attendu: {case['expected_physical']}")
            all_passed = False
    
    print("\n" + "="*80)
    if all_passed:
        print("✅ TOUS LES TESTS PARSER PASSENT")
    else:
        print("❌ CERTAINS TESTS PARSER ÉCHOUENT")
    print("="*80)
    
    return all_passed


# ============================================================================
# TEST 2 : Vérifier Metadata Tables
# ============================================================================

def test_metadata_tables():
    """Vérifie que les tables existent en metadata"""
    print("\n" + "="*80)
    print("TEST 2 : Vérification Metadata Tables")
    print("="*80)
    
    import psycopg2
    from flows.config.pg_config import config
    
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    try:
        # Tables à vérifier
        test_tables = ['client', 'lisval_fou_production', 'lisval']
        
        for table_id in test_tables:
            print(f"\n[Check] {table_id}")
            
            cur.execute("""
                SELECT 
                    "TableName",
                    "ConfigName",
                    "PrimaryKeyCols",
                    "IsActive"
                FROM metadata.etl_tables
                WHERE "ConfigName" = %s OR "TableName" = %s
                LIMIT 1
            """, (table_id, table_id))
            
            row = cur.fetchone()
            
            if row:
                print(f"  ✅ Trouvé")
                print(f"     TableName  : {row[0]}")
                print(f"     ConfigName : {row[1]}")
                print(f"     PrimaryKeys: {row[2]}")
                print(f"     IsActive   : {row[3]}")
            else:
                print(f"  ❌ Non trouvé dans metadata.etl_tables")
        
    finally:
        cur.close()
        conn.close()
    
    print("\n" + "="*80)


# ============================================================================
# TEST 3 : Scan Fichiers SFTP
# ============================================================================

def test_scan_sftp():
    """Scan fichiers disponibles"""
    print("\n" + "="*80)
    print("TEST 3 : Scan Fichiers SFTP")
    print("="*80)
    
    from flows.config.pg_config import config
    
    parquet_dir = Path(config.sftp_parquet_dir)
    
    if not parquet_dir.exists():
        print(f"❌ Répertoire introuvable : {parquet_dir}")
        return
    
    print(f"\n📁 {parquet_dir}\n")
    
    files = list(parquet_dir.glob("*.parquet"))
    
    if not files:
        print("⚠️  Aucun fichier .parquet trouvé")
        return
    
    print(f"Fichiers trouvés : {len(files)}\n")
    
    for f in sorted(files, key=lambda x: x.stat().st_mtime, reverse=True):
        size_mb = f.stat().st_size / (1024 * 1024)
        mtime = f.stat().st_mtime
        
        print(f"  {f.name}")
        print(f"    Taille : {size_mb:.2f} MB")
        print(f"    Modifié: {Path(f).stat().st_mtime}")
    
    print("\n" + "="*80)


# ============================================================================
# TEST 4 : Flow Manuel (Dry Run)
# ============================================================================

def test_manual_flow_dry():
    """Test flow manuel sans exécution réelle"""
    print("\n" + "="*80)
    print("TEST 4 : Flow Manuel (Dry Run)")
    print("="*80)
    
    from flows.ingestion.manual_table_import import (
        verify_table_metadata,
        find_table_file
    )
    
    # Test tables
    test_tables = ['client', 'lisval_fou_production']
    
    for table_id in test_tables:
        print(f"\n[Test] {table_id}")
        
        # Vérifier metadata
        meta = verify_table_metadata(table_id)
        
        if meta:
            print(f"  ✅ Metadata OK")
            print(f"     TableName : {meta['table_name']}")
            print(f"     ConfigName: {meta['config_name']}")
        else:
            print(f"  ❌ Metadata non trouvée")
            continue
        
        # Chercher fichier
        file_info = find_table_file(table_id, "AUTO")
        
        if file_info:
            print(f"  ✅ Fichier trouvé")
            print(f"     Path      : {Path(file_info['parquet_path']).name}")
            print(f"     Load Mode : {file_info['final_load_mode']}")
            print(f"     Physical  : {file_info['config_name'] or file_info['table_name']}")
        else:
            print(f"  ❌ Fichier non trouvé")
    
    print("\n" + "="*80)


# ============================================================================
# TEST 5 : Vérifier Cohérence Chaîne
# ============================================================================

def test_chain_consistency():
    """Vérifie cohérence RAW -> STAGING -> ODS"""
    print("\n" + "="*80)
    print("TEST 5 : Cohérence Chaîne RAW->STAGING->ODS")
    print("="*80)
    
    import psycopg2
    from flows.config.pg_config import config
    
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    try:
        # Vérifier tables existantes
        test_configs = ['client', 'lisval_fou_production']
        
        for config_name in test_configs:
            print(f"\n[Check] {config_name}")
            
            # RAW
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = 'raw'
                    AND table_name = %s
                )
            """, (f'raw_{config_name.lower()}',))
            
            has_raw = cur.fetchone()[0]
            print(f"  RAW     : {'✅' if has_raw else '❌'} raw.raw_{config_name.lower()}")
            
            # STAGING
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = 'staging_etl'
                    AND table_name = %s
                )
            """, (f'stg_{config_name.lower()}',))
            
            has_stg = cur.fetchone()[0]
            print(f"  STAGING : {'✅' if has_stg else '❌'} staging_etl.stg_{config_name.lower()}")
            
            # ODS
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = 'ods_etl'
                    AND table_name = %s
                )
            """, (f'ods_{config_name.lower()}',))
            
            has_ods = cur.fetchone()[0]
            print(f"  ODS     : {'✅' if has_ods else '❌'} ods_etl.ods_{config_name.lower()}")
            
            if has_raw and has_stg and has_ods:
                print(f"  ✅ Chaîne complète")
            else:
                print(f"  ⚠️  Chaîne incomplète")
    
    finally:
        cur.close()
        conn.close()
    
    print("\n" + "="*80)


# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    print("\n" + "="*80)
    print("🧪 SUITE DE TESTS - CORRECTIONS CONFIGNAME")
    print("="*80)
    
    try:
        # Test 1 : Parser
        test_parser()
        
        # Test 2 : Metadata
        test_metadata_tables()
        
        # Test 3 : Scan SFTP
        test_scan_sftp()
        
        # Test 4 : Flow manuel (dry)
        test_manual_flow_dry()
        
        # Test 5 : Cohérence chaîne
        test_chain_consistency()
        
        print("\n" + "="*80)
        print("✅ TOUS LES TESTS TERMINÉS")
        print("="*80)
        
    except Exception as e:
        print("\n" + "="*80)
        print(f"❌ ERREUR : {e}")
        print("="*80)
        import traceback
        traceback.print_exc()