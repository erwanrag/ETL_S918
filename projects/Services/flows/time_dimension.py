"""
============================================================================
Flow Prefect : Construction Dimension Temporelle
============================================================================
Description :
    Génère une table de dimension temporelle complète avec :
    - Attributs jour/semaine/mois/trimestre/année
    - Formats multiples (ISO, US, FR, etc.)
    - Flags (weekend, leap year, etc.)
    
Table PostgreSQL :
    reference.time_dimension (date_id PK)
    
Période :
    2015-01-01 à 2035-12-31 (7,671 jours)
    
Exécution :
    Une seule fois à l'initialisation, puis mise à jour annuelle
============================================================================
"""

from prefect import flow, task
from prefect.logging import get_run_logger
import psycopg2
from datetime import date, timedelta
import calendar
import sys
from pathlib import Path

# Ajouter Services au path
SERVICES_PATH = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(SERVICES_PATH))

from config.pg_config import config

# =============================================================================
# FONCTIONS UTILITAIRES
# =============================================================================

def day_suffix(day: int) -> str:
    """Suffixe ordinal anglais (1st, 2nd, 3rd, 4th, ...)"""
    if 11 <= day <= 13:
        return "th"
    return {1: "st", 2: "nd", 3: "rd"}.get(day % 10, "th")


def first_day_of_week(d: date) -> date:
    """Premier jour de la semaine (lundi)"""
    return d - timedelta(days=d.weekday())


def last_day_of_week(d: date) -> date:
    """Dernier jour de la semaine (dimanche)"""
    return first_day_of_week(d) + timedelta(days=6)


def week_of_month(d: date) -> int:
    """Numéro de semaine dans le mois (1-5)"""
    return (d.day - 1) // 7 + 1


def first_of_month(d: date) -> date:
    """Premier jour du mois"""
    return d.replace(day=1)


def last_of_month(d: date) -> date:
    """Dernier jour du mois"""
    next_month = d.replace(day=28) + timedelta(days=4)
    return next_month.replace(day=1) - timedelta(days=1)


def first_of_quarter(d: date) -> date:
    """Premier jour du trimestre"""
    q = (d.month - 1) // 3 + 1
    return date(d.year, 3 * (q - 1) + 1, 1)


def last_of_quarter(d: date) -> date:
    """Dernier jour du trimestre"""
    fq = first_of_quarter(d)
    return (fq + timedelta(days=92)).replace(day=1) - timedelta(days=1)


def first_of_year(d: date) -> date:
    """Premier jour de l'année"""
    return date(d.year, 1, 1)


def last_of_year(d: date) -> date:
    """Dernier jour de l'année"""
    return date(d.year, 12, 31)


def has_53_weeks(year: int) -> bool:
    """L'année a 53 semaines (calendrier standard)"""
    return last_of_year(date(year, 12, 31)).isocalendar()[1] == 53


def has_53_iso_weeks(year: int) -> bool:
    """L'année a 53 semaines ISO"""
    return date(year, 12, 28).isocalendar()[1] == 53


# =============================================================================
# TASK PRINCIPAL
# =============================================================================

@task(name="📅 Generate Time Dimension Data", retries=1)
def generate_time_dimension():
    """Génère et insère toutes les dates dans reference.time_dimension"""
    logger = get_run_logger()
    
    conn = None
    try:
        conn = psycopg2.connect(config.get_connection_string())
        cur = conn.cursor()
        
        # Vider la table
        logger.info("🧹 TRUNCATE reference.time_dimension...")
        cur.execute("TRUNCATE reference.time_dimension;")
        conn.commit()
        
        # Paramètres
        start = date(2015, 1, 1)
        end = date(2035, 12, 31)
        total_days = (end - start).days + 1
        
        logger.info(f"📊 Génération {total_days} jours ({start} → {end})")
        
        # SQL INSERT
        sql = """
            INSERT INTO reference.time_dimension (
                date_id, day, day_suffix, day_name, day_of_week,
                day_of_week_in_month, day_of_year, is_weekend,
                week, isoweek, first_of_week, last_of_week,
                week_of_month, month, month_name, first_of_month,
                last_of_month, first_of_next_month, last_of_next_month,
                quarter, first_of_quarter, last_of_quarter, year,
                isoyear, first_of_year, last_of_year, is_leap_year,
                has53weeks, has53isoweeks,
                mmyyyy, style101, style103, style112, style120
            )
            VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s,
                %s, %s, %s, %s, %s
            );
        """
        
        current = start
        rows_inserted = 0
        batch = []
        batch_size = 1000
        
        while current <= end:
            # Calculs
            iso_year, iso_week, iso_weekday = current.isocalendar()
            day_name = current.strftime("%A")
            month_name = current.strftime("%B")
            is_weekend = current.weekday() >= 5
            quarter = (current.month - 1) // 3 + 1
            is_leap = calendar.isleap(current.year)
            
            # Dates suivantes
            next_month_first = (current.replace(day=28) + timedelta(days=4)).replace(day=1)
            next_month_last = (next_month_first.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
            
            # Formats de date
            mmyyyy = current.strftime("%m/%Y")
            style101 = current.strftime("%m/%d/%Y")  # US: MM/DD/YYYY
            style103 = current.strftime("%d/%m/%Y")  # EU: DD/MM/YYYY
            style112 = current.strftime("%Y%m%d")     # ISO: YYYYMMDD
            style120 = current.strftime("%Y-%m-%d")   # ISO: YYYY-MM-DD
            
            # Row
            row = (
                # date_id, day, day_suffix, day_name, day_of_week
                current, current.day, day_suffix(current.day), day_name, current.weekday() + 1,
                # day_of_week_in_month, day_of_year, is_weekend
                week_of_month(current), current.timetuple().tm_yday, is_weekend,
                # week, isoweek, first_of_week, last_of_week
                current.isocalendar()[1], iso_week, first_day_of_week(current), last_day_of_week(current),
                # week_of_month, month, month_name, first_of_month
                week_of_month(current), current.month, month_name, first_of_month(current),
                # last_of_month, first_of_next_month, last_of_next_month
                last_of_month(current), next_month_first, next_month_last,
                # quarter, first_of_quarter, last_of_quarter, year
                quarter, first_of_quarter(current), last_of_quarter(current), current.year,
                # isoyear, first_of_year, last_of_year, is_leap_year
                iso_year, first_of_year(current), last_of_year(current), is_leap,
                # has53weeks, has53isoweeks
                has_53_weeks(current.year), has_53_iso_weeks(current.year),
                # mmyyyy, style101, style103, style112, style120
                mmyyyy, style101, style103, style112, style120
            )
            
            batch.append(row)
            
            # Batch insert
            if len(batch) >= batch_size:
                cur.executemany(sql, batch)
                conn.commit()
                rows_inserted += len(batch)
                logger.info(f"⏳ {rows_inserted}/{total_days} jours insérés...")
                batch = []
            
            current += timedelta(days=1)
        
        # Insert final batch
        if batch:
            cur.executemany(sql, batch)
            conn.commit()
            rows_inserted += len(batch)
        
        logger.info(f"✅ TERMINÉ - {rows_inserted} jours insérés")
        
        return {"rows_inserted": rows_inserted, "start_date": str(start), "end_date": str(end)}
        
    except psycopg2.Error as e:
        if conn:
            conn.rollback()
        logger.error(f"❌ Erreur PostgreSQL: {e}")
        raise
    except Exception as e:
        logger.error(f"❌ Erreur inattendue: {e}")
        raise
    finally:
        if conn:
            cur.close()
            conn.close()


# =============================================================================
# FLOW PRINCIPAL
# =============================================================================

@flow(name="📅 Build Time Dimension")
def build_time_dimension_flow():
    """
    Flow : Construire dimension temporelle
    Fréquence : 1x à l'initialisation, puis annuel
    """
    logger = get_run_logger()
    logger.info("🚀 DÉBUT - Construction time_dimension")
    
    result = generate_time_dimension()
    
    logger.info(f"✅ FIN - {result['rows_inserted']} jours")
    return result


# =============================================================================
# EXECUTION STANDALONE
# =============================================================================

if __name__ == "__main__":
    build_time_dimension_flow()