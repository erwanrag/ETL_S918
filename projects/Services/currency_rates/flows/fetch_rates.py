from prefect import flow, task
from prefect.logging import get_run_logger
from datetime import datetime
import requests
import psycopg2
from ...config.pg_config import Config

API_URL = "https://api.exchangerate.host/latest?base=EUR"

@task
def fetch_rates_task():
    logger = get_run_logger()
    response = requests.get(API_URL)
    data = response.json()

    logger.info(f"Taux récupérés : {len(data['rates'])} devises")
    return data

@task
def save_rates_task(data):
    logger = get_run_logger()

    conn = psycopg2.connect(Config.connection_string())
    cur = conn.cursor()

    date = data["date"]

    # --------------------------
    # 1) Table historique
    # --------------------------
    sql_hist = f"""
        INSERT INTO reference.currency_rates (date, currency, rate)
        VALUES (%s, %s, %s)
        ON CONFLICT (date, currency) DO UPDATE
        SET rate = EXCLUDED.rate;
    """

    # --------------------------
    # 2) Table snapshot (du jour)
    # --------------------------
    cur.execute("TRUNCATE reference.currency_rates_today;")

    sql_today = """
        INSERT INTO reference.currency_rates_today (currency, rate, updated_at)
        VALUES (%s, %s, NOW());
    """

    for currency, rate in data["rates"].items():
        # Historique
        cur.execute(sql_hist, (date, currency, rate))

        # Snapshot
        cur.execute(sql_today, (currency, rate))

    conn.commit()
    cur.close()
    conn.close()

    logger.info("Taux enregistrés dans historique + snapshot du jour")

@flow(name="📈 Currency Daily Rates")
def fetch_currency_rates():
    data = fetch_rates_task()
    save_rates_task(data)

    return {"date": data["date"], "nb": len(data["rates"])}

if __name__ == "__main__":
    fetch_currency_rates()
