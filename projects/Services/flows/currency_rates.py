"""
============================================================================
Flow Prefect : Gestion Devises (Codes ISO + Taux de Change)
============================================================================
Description :
    - Récupération codes ISO 4217 (currencies)
    - Récupération taux de change quotidiens
    - Stockage dans PostgreSQL schema reference
    
Tables PostgreSQL :
    - reference.currencies (codes ISO)
    - reference.currency_rates (historique)
    - reference.currency_rates_today (snapshot du jour)

APIs Utilisées :
    - https://open.er-api.com/v6/codes (codes ISO)
    - https://api.exchangerate.host/latest (taux de change)
============================================================================
"""

from prefect import flow, task
from prefect.logging import get_run_logger
import requests
import psycopg2
from datetime import datetime
import sys
from pathlib import Path

# Ajouter Services au path
SERVICES_PATH = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(SERVICES_PATH))

from config.pg_config import config

# =============================================================================
# CONFIGURATION APIs
# =============================================================================

API_CODES_URL = "https://open.er-api.com/v6/codes"
API_RATES_URL = "https://api.exchangerate.host/latest?base=EUR"

# =============================================================================
# TASKS - CODES ISO DEVISES
# =============================================================================

@task(name="📥 Fetch Currency Codes", retries=3, retry_delay_seconds=60)
def fetch_currency_codes():
    """Récupérer codes devises ISO 4217 depuis API"""
    logger = get_run_logger()
    
    try:
        response = requests.get(API_CODES_URL, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if data.get("result") != "success":
            raise ValueError(f"API error: {data.get('error-type', 'Unknown')}")
        
        codes = data["supported_codes"]
        logger.info(f"✅ {len(codes)} codes devises récupérés")
        return codes
        
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Erreur requête API: {e}")
        raise
    except Exception as e:
        logger.error(f"❌ Erreur inattendue: {e}")
        raise


@task(name="💾 Save Currency Codes", retries=2)
def save_currency_codes(codes: list):
    """Sauvegarder codes devises dans reference.currencies"""
    logger = get_run_logger()
    
    conn = None
    try:
        conn = psycopg2.connect(config.get_connection_string())
        cur = conn.cursor()
        
        sql = """
            INSERT INTO reference.currencies
            (currency_code, currency_name, last_updated)
            VALUES (%s, %s, NOW())
            ON CONFLICT (currency_code) DO UPDATE
            SET currency_name = EXCLUDED.currency_name,
                last_updated = NOW();
        """
        
        inserted = 0
        for code, name in codes:
            cur.execute(sql, (code, name))
            inserted += 1
        
        conn.commit()
        logger.info(f"✅ {inserted} codes devises insérés/mis à jour")
        
    except psycopg2.Error as e:
        if conn:
            conn.rollback()
        logger.error(f"❌ Erreur PostgreSQL: {e}")
        raise
    finally:
        if conn:
            cur.close()
            conn.close()


# =============================================================================
# TASKS - TAUX DE CHANGE
# =============================================================================

@task(name="📈 Fetch Exchange Rates", retries=3, retry_delay_seconds=60)
def fetch_exchange_rates():
    """Récupérer taux de change quotidiens depuis API"""
    logger = get_run_logger()
    
    try:
        response = requests.get(API_RATES_URL, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if not data.get("success", True):
            raise ValueError(f"API error: {data.get('error', 'Unknown')}")
        
        rates = data["rates"]
        rate_date = data.get("date", datetime.now().strftime("%Y-%m-%d"))
        
        logger.info(f"✅ {len(rates)} taux de change récupérés pour {rate_date}")
        return {"date": rate_date, "rates": rates}
        
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Erreur requête API: {e}")
        raise
    except Exception as e:
        logger.error(f"❌ Erreur inattendue: {e}")
        raise


@task(name="💾 Save Exchange Rates", retries=2)
def save_exchange_rates(data: dict):
    """
    Sauvegarder taux de change dans :
    - reference.currency_rates (historique)
    - reference.currency_rates_today (snapshot)
    """
    logger = get_run_logger()
    
    conn = None
    try:
        conn = psycopg2.connect(config.get_connection_string())
        cur = conn.cursor()
        
        rate_date = data["date"]
        rates = data["rates"]
        
        # =====================================================================
        # 1. Historique
        # =====================================================================
        sql_hist = """
            INSERT INTO reference.currency_rates (date, currency, rate)
            VALUES (%s, %s, %s)
            ON CONFLICT (date, currency) DO UPDATE
            SET rate = EXCLUDED.rate;
        """
        
        # =====================================================================
        # 2. Snapshot du jour
        # =====================================================================
        cur.execute("TRUNCATE reference.currency_rates_today;")
        
        sql_today = """
            INSERT INTO reference.currency_rates_today (currency, rate, updated_at)
            VALUES (%s, %s, NOW());
        """
        
        inserted_hist = 0
        inserted_today = 0
        
        for currency, rate in rates.items():
            # Historique
            cur.execute(sql_hist, (rate_date, currency, rate))
            inserted_hist += 1
            
            # Snapshot
            cur.execute(sql_today, (currency, rate))
            inserted_today += 1
        
        conn.commit()
        logger.info(f"✅ Historique: {inserted_hist} taux")
        logger.info(f"✅ Snapshot: {inserted_today} taux")
        
    except psycopg2.Error as e:
        if conn:
            conn.rollback()
        logger.error(f"❌ Erreur PostgreSQL: {e}")
        raise
    finally:
        if conn:
            cur.close()
            conn.close()


# =============================================================================
# FLOWS PRINCIPAUX
# =============================================================================

@flow(name="📘 Load Currency Codes ISO 4217")
def load_currency_codes_flow():
    """
    Flow : Charger codes devises ISO 4217
    Fréquence recommandée : 1x/mois
    """
    logger = get_run_logger()
    logger.info("🚀 DÉBUT - Chargement codes ISO 4217")
    
    codes = fetch_currency_codes()
    save_currency_codes(codes)
    
    logger.info(f"✅ FIN - {len(codes)} codes traités")
    return {"nb_codes": len(codes)}


@flow(name="📈 Load Daily Exchange Rates")
def load_exchange_rates_flow():
    """
    Flow : Charger taux de change quotidiens
    Fréquence recommandée : 1x/jour (matin)
    """
    logger = get_run_logger()
    logger.info("🚀 DÉBUT - Chargement taux de change")
    
    data = fetch_exchange_rates()
    save_exchange_rates(data)
    
    logger.info(f"✅ FIN - {len(data['rates'])} taux pour {data['date']}")
    return {"date": data["date"], "nb_rates": len(data["rates"])}


# =============================================================================
# EXECUTION STANDALONE
# =============================================================================

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Gestion Devises")
    parser.add_argument(
        "--mode",
        choices=["codes", "rates", "both"],
        default="both",
        help="Charger codes, rates, ou les deux"
    )
    
    args = parser.parse_args()
    
    if args.mode in ("codes", "both"):
        load_currency_codes_flow()
    
    if args.mode in ("rates", "both"):
        load_exchange_rates_flow()