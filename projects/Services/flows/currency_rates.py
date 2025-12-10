"""
============================================================================
Flow Prefect : Gestion Devises (Codes ISO + Taux de Change)
============================================================================
Description :
    - Recuperation codes ISO 4217 (currencies)
    - Recuperation taux de change quotidiens
    - Stockage dans PostgreSQL schema reference
    
Tables PostgreSQL :
    - reference.currencies (codes ISO)
    - reference.currency_rates (historique)
    - reference.currency_rates_today (snapshot du jour)

APIs Utilisees (GRATUITES, SANS CLe) :
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
from tenacity import retry, stop_after_attempt, wait_exponential

# Ajouter Services au path
SERVICES_PATH = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(SERVICES_PATH))

from config.pg_config import config

# =============================================================================
# CONFIGURATION APIs - GRATUITES SANS CLe
# =============================================================================

# API codes ISO (gratuite, pas de cle requise)
API_CODES_URL = "https://openexchangerates.org/api/currencies.json"

# API taux de change (gratuite, pas de cle requise) 
API_RATES_URL = "https://open.er-api.com/v6/latest/EUR"

# =============================================================================
# TASKS - CODES ISO DEVISES
# =============================================================================
@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
@task(name="[TASK] Fetch Currency Codes", retries=3, retry_delay_seconds=60)
def fetch_currency_codes():
    """
    Recuperer codes devises ISO 4217 depuis openexchangerates.org
    Retourne: dict {code: nom_complet}
    """
    logger = get_run_logger()
    
    try:
        response = requests.get(API_CODES_URL, timeout=30)
        response.raise_for_status()
        codes = response.json()
        
        logger.info(f"[OK] {len(codes)} codes devises recuperes")
        return codes
        
    except requests.exceptions.RequestException as e:
        logger.error(f"[ERROR] Erreur requête API: {e}")
        raise
    except Exception as e:
        logger.error(f"[ERROR] Erreur inattendue: {e}")
        raise

@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
@task(name="[TASK] Save Currency Codes", retries=2)
def save_currency_codes(codes: dict):
    """Sauvegarder codes devises dans reference.currencies"""
    logger = get_run_logger()
    
    conn = None
    try:
        conn = psycopg2.connect(config.get_connection_string())
        cur = conn.cursor()
        
        # Utiliser INSERT ON CONFLICT au lieu de TRUNCATE
        # pour eviter probleme avec les FK
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
        logger.info(f"[OK] {inserted} codes devises inseres/mis a jour")
        
    except psycopg2.Error as e:
        if conn:
            conn.rollback()
        logger.error(f"[ERROR] Erreur PostgreSQL: {e}")
        raise
    finally:
        if conn:
            cur.close()
            conn.close()


# =============================================================================
# TASKS - TAUX DE CHANGE
# =============================================================================
@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
@task(name="[TASK] Fetch Exchange Rates", retries=3, retry_delay_seconds=60)
def fetch_exchange_rates():
    """
    Recuperer taux de change quotidiens depuis open.er-api.com
    Retourne: {date: "YYYY-MM-DD", base: "EUR", rates: {currency: rate}}
    """
    logger = get_run_logger()
    
    try:
        response = requests.get(API_RATES_URL, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        # Verifier que la reponse est valide
        if data.get("result") != "success":
            raise ValueError(f"API error: {data.get('error-type', 'Unknown')}")
        
        rates = data["rates"]
        rate_date = data.get("time_last_update_utc", datetime.now().strftime("%Y-%m-%d"))
        
        # Extraire juste la date (format: "Wed, 10 Dec 2025 00:00:00 +0000")
        if "," in rate_date:
            from datetime import datetime as dt
            rate_date = dt.strptime(rate_date, "%a, %d %b %Y %H:%M:%S %z").strftime("%Y-%m-%d")
        
        logger.info(f"[OK] {len(rates)} taux de change recuperes pour {rate_date}")
        return {"date": rate_date, "base": "EUR", "rates": rates}
        
    except requests.exceptions.RequestException as e:
        logger.error(f"[ERROR] Erreur requête API: {e}")
        raise
    except Exception as e:
        logger.error(f"[ERROR] Erreur inattendue: {e}")
        raise

from tenacity import retry, stop_after_attempt, wait_exponential

@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
@task(name="[TASK] Save Exchange Rates", retries=2)
def save_exchange_rates(data: dict):
    """
    Sauvegarder taux de change dans :
    - reference.currency_rates (historique)
    - reference.currency_rates_today (snapshot)
    
    Validations :
    - EUR = 1.0 (base)
    - Pas de taux négatifs/nuls
    - Alerte si taux > 10000 (suspect)
    """
    logger = get_run_logger()
    
    conn = None
    try:
        conn = psycopg2.connect(config.get_connection_string())
        cur = conn.cursor()
        
        rate_date = data["date"]
        rates = data["rates"]
        
        # =====================================================================
        # VALIDATION : EUR doit être 1.0 (base de référence)
        # =====================================================================
        if rates.get('EUR') != 1.0:
            raise ValueError(f"[ERROR] EUR rate invalide : {rates.get('EUR')} (attendu 1.0)")
        
        # =====================================================================
        # FILTRE : Recuperer les codes valides depuis currencies
        # =====================================================================
        cur.execute("SELECT currency_code FROM reference.currencies")
        valid_codes = {row[0] for row in cur.fetchall()}
        
        # Filtrer les taux pour ne garder que les codes valides
        filtered_rates = {}
        invalid_rates = []
        suspect_rates = []
        
        for curr, rate in rates.items():
            # Validation : pas de taux négatifs/nuls
            if rate <= 0:
                invalid_rates.append(f"{curr}={rate}")
                continue
            
            # Alerte : taux suspects (> 10000)
            if rate > 10000:
                suspect_rates.append(f"{curr}={rate}")
            
            # Filtrer codes invalides
            if curr in valid_codes:
                filtered_rates[curr] = rate
        
        # Logs warnings
        if invalid_rates:
            logger.warning(f"[WARNING] {len(invalid_rates)} taux negatifs/nuls ignores : {invalid_rates[:5]}")
        
        if suspect_rates:
            logger.warning(f"[WARNING] {len(suspect_rates)} taux suspects (>10000) : {suspect_rates[:5]}")
        
        skipped = len(rates) - len(filtered_rates) - len(invalid_rates)
        if skipped > 0:
            invalid_codes = set(rates.keys()) - valid_codes - {c.split('=')[0] for c in invalid_rates}
            logger.warning(f"[WARNING] {skipped} devises ignorees (codes invalides) : {sorted(invalid_codes)[:5]}...")
        
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
        logger.info(f"[OK] Historique: {inserted_hist} taux pour {rate_date}")
        logger.info(f"[OK] Snapshot: {inserted_today} taux (aujourd'hui)")
        
        # Log structuré pour métriques Prefect
        logger.info("exchange_rates_saved", extra={
            "date": rate_date,
            "valid_rates": inserted_hist,
            "invalid_rates": len(invalid_rates),
            "suspect_rates": len(suspect_rates),
            "skipped_codes": skipped
        })
        
    except psycopg2.Error as e:
        if conn:
            conn.rollback()
        logger.error(f"[ERROR] Erreur PostgreSQL: {e}")
        raise
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"[ERROR] Erreur inattendue: {e}")
        raise
    finally:
        if conn:
            cur.close()
            conn.close()

# =============================================================================
# FLOWS PRINCIPAUX
# =============================================================================

@flow(name="Load Currency Codes ISO 4217")
def load_currency_codes_flow():
    """
    Flow : Charger codes devises ISO 4217
    Frequence recommandee : 1x/mois
    """
    logger = get_run_logger()
    logger.info("[START] Chargement codes ISO 4217")
    
    codes = fetch_currency_codes()
    save_currency_codes(codes)
    
    logger.info(f"[OK] FIN - {len(codes)} codes traites")
    return {"nb_codes": len(codes)}


@flow(name="Load Daily Exchange Rates")
def load_exchange_rates_flow():
    """
    Flow : Charger taux de change quotidiens
    Frequence recommandee : 1x/jour (matin)
    """
    logger = get_run_logger()
    logger.info("[START] Chargement taux de change")
    
    data = fetch_exchange_rates()
    save_exchange_rates(data)
    
    logger.info(f"[OK] FIN - {len(data['rates'])} taux pour {data['date']}")
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
        print("\n Chargement codes devises...")
        load_currency_codes_flow()
    
    if args.mode in ("rates", "both"):
        print("\n Chargement taux de change...")
        load_exchange_rates_flow()