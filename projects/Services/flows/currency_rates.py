# -*- coding: utf-8 -*-
"""
============================================================================
Flow Prefect : Gestion Devises (Codes ISO + Taux de Change)
============================================================================
"""
from prefect import flow, task
from prefect.logging import get_run_logger
import requests
import psycopg2
from datetime import datetime
import sys
from pathlib import Path
from tenacity import retry, stop_after_attempt, wait_exponential

# Ajouter le projet au path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# Imports
from shared.config import config
from shared.alerting.alert_manager import get_alert_manager, AlertLevel

# Alerting Setup
try:
    alert_mgr = get_alert_manager()
    ALERTING_ENABLED = True
except ImportError:
    ALERTING_ENABLED = False

# API Constants
API_CODES_URL = "https://openexchangerates.org/api/currencies.json"
API_RATES_URL = "https://open.er-api.com/v6/latest/EUR"

@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
@task(name="[API] Fetch Currency Codes", retries=3)
def fetch_currency_codes():
    logger = get_run_logger()
    try:
        response = requests.get(API_CODES_URL, timeout=30)
        response.raise_for_status()
        codes = response.json()
        logger.info(f"[OK] {len(codes)} codes fetched")
        return codes
    except Exception as e:
        logger.error(f"[ERROR] API Codes: {e}")
        raise

@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
@task(name="[DB] Save Codes", retries=2)
def save_currency_codes(codes: dict):
    logger = get_run_logger()
    conn = None
    try:
        conn = psycopg2.connect(config.get_connection_string())
        cur = conn.cursor()
        sql = """
            INSERT INTO reference.currencies (currency_code, currency_name, last_updated)
            VALUES (%s, %s, NOW())
            ON CONFLICT (currency_code) DO UPDATE
            SET currency_name = EXCLUDED.currency_name,
                last_updated = EXCLUDED.last_updated;
        """
        for code, name in codes.items():
            cur.execute(sql, (code, name))
        conn.commit()
        logger.info(f"[OK] {len(codes)} codes saved")
    except Exception as e:
        if conn: conn.rollback()
        logger.error(f"[ERROR] DB Save Codes: {e}")
        raise
    finally:
        if conn:
            cur.close()
            conn.close()

@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
@task(name="[API] Fetch Exchange Rates", retries=3)
def fetch_exchange_rates():
    logger = get_run_logger()
    try:
        response = requests.get(API_RATES_URL, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if data.get("result") != "success":
            raise ValueError(f"API Error: {data.get('error-type', 'Unknown')}")
            
        rates = data["rates"]
        rate_date = data.get("time_last_update_utc", datetime.now().strftime("%Y-%m-%d"))
        
        if "," in rate_date:
            from datetime import datetime as dt
            rate_date = dt.strptime(rate_date, "%a, %d %b %Y %H:%M:%S %z").strftime("%Y-%m-%d")
            
        logger.info(f"[OK] {len(rates)} rates for {rate_date}")
        return {"date": rate_date, "base": "EUR", "rates": rates}
    except Exception as e:
        logger.error(f"[ERROR] API Rates: {e}")
        raise

@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
@task(name="[DB] Save Rates", retries=2)
def save_exchange_rates(data: dict):
    logger = get_run_logger()
    conn = None
    try:
        conn = psycopg2.connect(config.get_connection_string())
        cur = conn.cursor()
        
        rate_date = data["date"]
        rates = data["rates"]
        
        cur.execute("SELECT currency_code FROM reference.currencies")
        valid_codes = {row[0] for row in cur.fetchall()}
        
        filtered_rates = {k: v for k, v in rates.items() if k in valid_codes and v > 0}
        
        # 1. History
        sql_hist = """
            INSERT INTO reference.currency_rates (date, currency, rate)
            VALUES (%s, %s, %s)
            ON CONFLICT (date, currency) DO UPDATE SET rate = EXCLUDED.rate;
        """
        for curr, rate in filtered_rates.items():
            cur.execute(sql_hist, (rate_date, curr, rate))
            
        # 2. Snapshot
        cur.execute("TRUNCATE reference.currency_rates_today;")
        sql_today = "INSERT INTO reference.currency_rates_today (currency, rate, updated_at) VALUES (%s, %s, NOW());"
        for curr, rate in filtered_rates.items():
            cur.execute(sql_today, (curr, rate))
            
        conn.commit()
        logger.info(f"[OK] Saved {len(filtered_rates)} rates")
    except Exception as e:
        if conn: conn.rollback()
        logger.error(f"[ERROR] DB Save Rates: {e}")
        raise
    finally:
        if conn:
            cur.close()
            conn.close()

@flow(name="[SVC] 💶 Gestion Devises", log_prints=True)
def load_currency_data_flow():
    """Flow consolide : Codes ISO + Taux"""
    logger = get_run_logger()
    start_time = datetime.now()
    results = {"codes": 0, "rates": 0, "errors": []}
    
    try:
        # 1. Codes
        codes = fetch_currency_codes()
        save_currency_codes(codes)
        results["codes"] = len(codes)
        
        # 2. Rates
        data = fetch_exchange_rates()
        save_exchange_rates(data)
        results["rates"] = len(data["rates"])
        
        if ALERTING_ENABLED:
            alert_mgr.send_alert(
                level=AlertLevel.INFO,
                title="Services - Currency Data SUCCESS",
                message=f"Loaded {results['codes']} codes, {results['rates']} rates",
                context={"Service": "Currency", "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
            )
            
        return {"nb_codes": results["codes"], "nb_rates": results["rates"]}
        
    except Exception as e:
        logger.error(f"[CRITICAL] {e}")
        if ALERTING_ENABLED:
            alert_mgr.send_alert(
                level=AlertLevel.CRITICAL,
                title="Services - Currency Data FAILED",
                message=str(e),
                context={"Service": "Currency"}
            )
        raise

if __name__ == "__main__":
    load_currency_data_flow()