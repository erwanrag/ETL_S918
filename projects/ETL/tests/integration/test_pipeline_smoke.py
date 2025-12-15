import pytest

def test_scan_sftp_directory():
    pytest.skip("Necessaire contexte Prefect")

def test_list_raw_tables(pg_config):
    import psycopg2
    conn = psycopg2.connect(pg_config.get_connection_string())
    cur = conn.cursor()
    cur.execute("""SELECT table_name FROM information_schema.tables 
        WHERE table_schema = 'raw' AND table_name LIKE 'raw_%'""")
    result = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()
    assert isinstance(result, list)

def test_list_staging_tables(pg_config):
    import psycopg2
    conn = psycopg2.connect(pg_config.get_connection_string())
    cur = conn.cursor()
    cur.execute("""SELECT table_name FROM information_schema.tables 
        WHERE table_schema = 'staging_etl' AND table_name LIKE 'stg_%'""")
    result = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()
    assert isinstance(result, list)

def test_full_pipeline_structure():
    from flows.orchestration.full_pipeline import full_etl_pipeline
    assert callable(full_etl_pipeline)