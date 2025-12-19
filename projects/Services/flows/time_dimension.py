"""
============================================================================
Flow Prefect : Construction Dimension Temporelle
============================================================================
"""
from prefect import flow, task
from prefect.logging import get_run_logger
import psycopg2
from datetime import date, timedelta
import calendar
import sys
from pathlib import Path

# Ajouter le projet au path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from shared.config import config

# Helper functions
def day_suffix(day: int) -> str:
    if 11 <= day <= 13: return "th"
    return {1: "st", 2: "nd", 3: "rd"}.get(day % 10, "th")

def first_of_week(d): return d - timedelta(days=d.weekday())
def last_of_week(d): return first_of_week(d) + timedelta(days=6)
def week_of_month(d): return (d.day - 1) // 7 + 1
def first_of_month(d): return d.replace(day=1)
def last_of_month(d): return (d.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
def first_of_quarter(d): return date(d.year, 3 * ((d.month - 1) // 3) + 1, 1)
def last_of_quarter(d): return (first_of_quarter(d) + timedelta(days=92)).replace(day=1) - timedelta(days=1)
def first_of_year(d): return date(d.year, 1, 1)
def last_of_year(d): return date(d.year, 12, 31)

@task(name="[DB] Generate Time Dimension")
def generate_time_dimension():
    logger = get_run_logger()
    conn = None
    try:
        conn = psycopg2.connect(config.get_connection_string())
        cur = conn.cursor()
        
        logger.info("🧹 Truncate table...")
        cur.execute("TRUNCATE reference.time_dimension;")
        
        start, end = date(2015, 1, 1), date(2035, 12, 31)
        total_days = (end - start).days + 1
        logger.info(f"📅 Generating {total_days} days ({start} -> {end})")
        
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
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
        """
        
        batch, batch_size = [], 1000
        current = start
        rows_inserted = 0
        
        while current <= end:
            next_m_first = (current.replace(day=28) + timedelta(days=4)).replace(day=1)
            next_m_last = (next_m_first.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
            
            row = (
                current, current.day, day_suffix(current.day), current.strftime("%A"), current.weekday() + 1,
                week_of_month(current), current.timetuple().tm_yday, current.weekday() >= 5,
                current.isocalendar()[1], current.isocalendar()[1], first_of_week(current), last_of_week(current),
                week_of_month(current), current.month, current.strftime("%B"), first_of_month(current),
                last_of_month(current), next_m_first, next_m_last,
                (current.month - 1) // 3 + 1, first_of_quarter(current), last_of_quarter(current), current.year,
                current.isocalendar()[0], first_of_year(current), last_of_year(current), calendar.isleap(current.year),
                last_of_year(current).isocalendar()[1] == 53, date(current.year, 12, 28).isocalendar()[1] == 53,
                current.strftime("%m/%Y"), current.strftime("%m/%d/%Y"), current.strftime("%d/%m/%Y"), current.strftime("%Y%m%d"), current.strftime("%Y-%m-%d")
            )
            batch.append(row)
            
            if len(batch) >= batch_size:
                cur.executemany(sql, batch)
                conn.commit()
                rows_inserted += len(batch)
                batch = []
            
            current += timedelta(days=1)
            
        if batch:
            cur.executemany(sql, batch)
            conn.commit()
            rows_inserted += len(batch)
            
        logger.info(f"✅ inserted {rows_inserted} days")
        return {"rows_inserted": rows_inserted}
        
    except Exception as e:
        if conn: conn.rollback()
        logger.error(f"[ERROR] {e}")
        raise
    finally:
        if conn:
            cur.close()
            conn.close()

@flow(name="[SVC] 📅 Time Dimension")
def build_time_dimension_flow():
    """Construire dimension temporelle"""
    generate_time_dimension()

if __name__ == "__main__":
    build_time_dimension_flow()