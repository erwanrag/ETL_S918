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

APIs Utilisées (GRATUITES, SANS CLÉ) :
    - https://openexchangerates.org/api/currencies.json (codes ISO)
    - https://open.er-api.com/v6/latest/EUR (taux de change)
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
# CONFIGURATION APIs - GRATUITES SANS CLÉ
# =============================================================================

# API codes ISO (gratuite, pas de clé requise)
API_CODES_URL = "https://openexchangerates.org/api/currencies.json"

# API taux de change (gratuite, pas de clé requise) 
API_RATES_URL = "https://open.er-api.com/v6/latest/EUR"

# =============================================================================
# TASKS - CODES ISO DEVISES
# =============================================================================

@task(name="📥 Fetch Currency Codes", retries=3, retry_delay_seconds=60)
def fetch_currency_codes():
    """
    Récupérer codes devises ISO 4217 depuis openexchangerates.org
    Retourne: dict {code: nom_complet}
    """
    logger = get_run_logger()
    
    try:
        response = requests.get(API_CODES_URL, timeout=30)
        response.raise_for_status()
        codes = response.json()
        
        logger.info(f"✅ {len(codes)} codes devises récupérés")
        return codes
        
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Erreur requête API: {e}")
        raise
    except Exception as e:
        logger.error(f"❌ Erreur inattendue: {e}")
        raise


@task(name="💾 Save Currency Codes", retries=2)
def save_currency_codes(codes: dict):
    """Sauvegarder codes devises dans reference.currencies"""
    logger = get_run_logger()
    
    conn = None
    try:
        conn = psycopg2.connect(config.get_connection_string())
        cur = conn.cursor()
        
        # Utiliser INSERT ON CONFLICT au lieu de TRUNCATE
        # pour éviter problème avec les FK
        sql = """
            INSERT INTO reference.currencies
            (currency_code, currency_name, last_updated)
            VALUES (%s, %s, NOW())
            ON CONFLICT (currency_code) DO UPDATE
            SET currency_name = EXCLUDED.currency_name,
                last_updated = NOW();
        """
        
        inserted = 0
        for code, name in codes.items():
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
    """
    Récupérer taux de change quotidiens depuis open.er-api.com
    Retourne: {date: "YYYY-MM-DD", base: "EUR", rates: {currency: rate}}
    """
    logger = get_run_logger()
    
    try:
        response = requests.get(API_RATES_URL, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        # Vérifier que la réponse est valide
        if data.get("result") != "success":
            raise ValueError(f"API error: {data.get('error-type', 'Unknown')}")
        
        rates = data["rates"]
        rate_date = data.get("time_last_update_utc", datetime.now().strftime("%Y-%m-%d"))
        
        # Extraire juste la date (format: "Wed, 10 Dec 2025 00:00:00 +0000")
        if "," in rate_date:
            from datetime import datetime as dt
            rate_date = dt.strptime(rate_date, "%a, %d %b %Y %H:%M:%S %z").strftime("%Y-%m-%d")
        
        logger.info(f"✅ {len(rates)} taux de change récupérés pour {rate_date}")
        return {"date": rate_date, "base": "EUR", "rates": rates}
        
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
        # FILTRE : Récupérer les codes valides depuis currencies
        # =====================================================================
        cur.execute("SELECT currency_code FROM reference.currencies")
        valid_codes = {row[0] for row in cur.fetchall()}
        
        # Filtrer les taux pour ne garder que les codes valides
        filtered_rates = {
            curr: rate for curr, rate in rates.items() 
            if curr in valid_codes
        }
        
        skipped = len(rates) - len(filtered_rates)
        if skipped > 0:
            invalid_codes = set(rates.keys()) - valid_codes
            logger.warning(f"⚠️ {skipped} devises ignorées (codes invalides) : {sorted(invalid_codes)[:5]}...")
        
        # =====================================================================
        # 1. Historique (INSERT avec ON CONFLICT)
        # =====================================================================
        sql_hist = """
            INSERT INTO reference.currency_rates (date, currency, rate)
            VALUES (%s, %s, %s)
            ON CONFLICT (date, currency) DO UPDATE
            SET rate = EXCLUDED.rate;
        """
        
        inserted_hist = 0
        for currency, rate in filtered_rates.items():
            cur.execute(sql_hist, (rate_date, currency, rate))
            inserted_hist += 1
        
        # =====================================================================
        # 2. Snapshot du jour (TRUNCATE + INSERT)
        # =====================================================================
        cur.execute("TRUNCATE reference.currency_rates_today;")
        
        sql_today = """
            INSERT INTO reference.currency_rates_today (currency, rate, updated_at)
            VALUES (%s, %s, NOW());
        """
        
        inserted_today = 0
        for currency, rate in filtered_rates.items():
            cur.execute(sql_today, (currency, rate))
            inserted_today += 1
        
        conn.commit()
        logger.info(f"✅ Historique: {inserted_hist} taux pour {rate_date}")
        logger.info(f"✅ Snapshot: {inserted_today} taux (aujourd'hui)")
        
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
        print("\n🌍 Chargement codes devises...")
        load_currency_codes_flow()
    
    if args.mode in ("rates", "both"):
        print("\n💱 Chargement taux de change...")
        load_exchange_rates_flow()