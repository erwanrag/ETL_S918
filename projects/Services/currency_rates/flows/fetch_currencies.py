from prefect import flow, task
from prefect.logging import get_run_logger
import requests
import psycopg2
from datetime import datetime
from ...config.pg_config import Config


API_URL = "https://open.er-api.com/v6/codes"   # API officielle ISO 4217


@task
def fetch_currency_codes():
    logger = get_run_logger()
    response = requests.get(API_URL)

    data = response.json()

    if data.get("result") != "success":
        raise ValueError("API currency codes unavailable")

    logger.info(f"Codes récupérés : {len(data['supported_codes'])}")

    return data["supported_codes"]


@task
def save_currency_codes(codes):
    logger = get_run_logger()
    conn = psycopg2.connect(Config.connection_string())
    cur = conn.cursor()

    sql = """
        INSERT INTO reference.currencies
        (currency_code, currency_name, currency_country, numeric_code, minor_units, last_updated)
        VALUES (%s, %s, %s, %s, %s, NOW())
        ON CONFLICT (currency_code) DO UPDATE
        SET currency_name = EXCLUDED.currency_name,
            currency_country = EXCLUDED.currency_country,
            numeric_code = EXCLUDED.numeric_code,
            minor_units = EXCLUDED.minor_units,
            last_updated = NOW();
    """

    for code, name in codes:
        # L’API renvoie code + description complète
        # Exemple : ["USD", "United States Dollar"]

        parts = name.split(" - ")
        country = parts[0] if len(parts) > 1 else None

        cur.execute(sql, (
            code,
            name,
            country,
            None,
            None,
        ))

    conn.commit()
    cur.close()
    conn.close()

    logger.info("Codes ISO insérés/actualisés.")


@flow(name="📘 Load ISO 4217 Currency Codes")
def load_currency_codes():
    codes = fetch_currency_codes()
    save_currency_codes(codes)


if __name__ == "__main__":
    load_currency_codes()
