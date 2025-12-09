from prefect import flow, task
from prefect.logging import get_run_logger
import psycopg2
from datetime import date, timedelta
import calendar
from Services.config.pg_config import Config


def day_suffix(day):
    if 11 <= day <= 13:
        return "th"
    return {1: "st", 2: "nd", 3: "rd"}.get(day % 10, "th")


def first_day_of_week(d):
    return d - timedelta(days=d.weekday())


def last_day_of_week(d):
    return first_day_of_week(d) + timedelta(days=6)


def week_of_month(d):
    return (d.day - 1) // 7 + 1


def first_of_month(d):
    return d.replace(day=1)


def last_of_month(d):
    next_month = d.replace(day=28) + timedelta(days=4)
    return next_month.replace(day=1) - timedelta(days=1)


def first_of_quarter(d):
    q = (d.month - 1) // 3 + 1
    return date(d.year, 3 * (q - 1) + 1, 1)


def last_of_quarter(d):
    fq = first_of_quarter(d)
    return fq.replace(month=fq.month + 3) - timedelta(days=1)


def first_of_year(d):
    return date(d.year, 1, 1)


def last_of_year(d):
    return date(d.year, 12, 31)


def has_53_weeks(year):
    return last_of_year(date(year, 12, 31)).isocalendar()[1] == 53


def has_53_iso_weeks(year):
    return date(year, 12, 28).isocalendar()[1] == 53


@task
def generate_time_dimension():
    logger = get_run_logger()

    conn = psycopg2.connect(Config.get_connection_string())
    cur = conn.cursor()

    logger.info("TRUNCATE reference.time_dimension...")
    cur.execute("TRUNCATE reference.time_dimension;")

    start = date(2015, 1, 1)
    end = date(2035, 12, 31)

    current = start
    rows = 0

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

    while current <= end:
        iso_year, iso_week, iso_weekday = current.isocalendar()

@flow(name="📅 Build Time Dimension")
def build_time_dimension_flow():
    generate_time_dimension()

if __name__ == "__main__":
    build_time_dimension_flow()

