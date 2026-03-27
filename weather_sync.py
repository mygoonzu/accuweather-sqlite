#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import fcntl
import html
import logging
import re
import sqlite3
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable
from urllib.parse import urljoin, urlparse
from zoneinfo import ZoneInfo

import requests


BASE_URL = "https://www.accuweather.com"
ALLOWED_HOST = "www.accuweather.com"
USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36"
DEFAULT_TIMEZONE = "Asia/Ho_Chi_Minh"
MAX_FORECAST_DAYS = 30
MAX_HISTORY_BACKFILL_DAYS = 366


@dataclass(frozen=True)
class Location:
    key: str
    slug: str
    city: str
    admin_area: str
    country: str = "Vietnam"

    @property
    def today_url(self) -> str:
        return f"{BASE_URL}/en/vn/{self.slug}/{self.key}/weather-forecast/{self.key}"

    @property
    def daily_url(self) -> str:
        return f"{BASE_URL}/en/vn/{self.slug}/{self.key}/daily-weather-forecast/{self.key}"

    def monthly_url(self, target_date: date) -> str:
        return f"{BASE_URL}/en/vn/{self.slug}/{self.key}/{target_date.strftime('%B').lower()}-weather/{self.key}"


LOCATIONS: tuple[Location, ...] = (
    Location(key="353412", slug="hanoi", city="Ha Noi", admin_area="Ha Noi"),
    Location(key="353981", slug="ho-chi-minh-city", city="Ho Chi Minh City", admin_area="Ho Chi Minh"),
    Location(key="352954", slug="da-nang", city="Da Nang", admin_area="Da Nang"),
)


@dataclass(frozen=True)
class ForecastRecord:
    location_key: str
    weather_date: date
    high_c: int | None
    low_c: int | None
    precip_probability: int | None
    phrase: str | None
    detail_url: str
    source_url: str


@dataclass(frozen=True)
class HistoryRecord:
    location_key: str
    weather_date: date
    actual_high_c: int | None
    actual_low_c: int | None
    source_url: str


class SyncError(RuntimeError):
    pass


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Sync AccuWeather history and forecast data for Ha Noi, Ho Chi Minh City, and Da Nang into SQLite."
    )
    parser.add_argument("--db", default="weather.db", help="SQLite database path. Default: %(default)s")
    parser.add_argument(
        "--lock-file",
        default=".weather_sync.lock",
        help="Path to the lock file that prevents concurrent sync runs. Default: %(default)s",
    )
    parser.add_argument(
        "--timezone",
        default=DEFAULT_TIMEZONE,
        help="IANA timezone used to determine 'today'. Default: %(default)s",
    )
    parser.add_argument("--log-level", default="INFO", choices=("DEBUG", "INFO", "WARNING", "ERROR"))

    subparsers = parser.add_subparsers(dest="command", required=True)

    list_parser = subparsers.add_parser("list-locations", help="Print built-in AccuWeather locations")
    list_parser.set_defaults(handler=handle_list_locations)

    status_parser = subparsers.add_parser("status", help="Show a summary of the SQLite database contents")
    status_parser.set_defaults(handler=handle_status)

    export_parser = subparsers.add_parser("export-csv", help="Export forecast or history rows from SQLite to CSV")
    export_parser.add_argument(
        "--table",
        required=True,
        choices=("forecast_daily", "history_daily"),
        help="Which logical table to export",
    )
    export_parser.add_argument("--output", required=True, help="CSV output path")
    export_parser.add_argument("--city", help="Optional city filter, e.g. 'Ha Noi'")
    export_parser.add_argument("--date-from", help="Inclusive date filter in YYYY-MM-DD format")
    export_parser.add_argument("--date-to", help="Inclusive date filter in YYYY-MM-DD format")
    export_parser.set_defaults(handler=handle_export_csv)

    sync_parser = subparsers.add_parser("sync", help="Fetch history and forecast, then upsert into SQLite")
    sync_parser.add_argument(
        "--history-backfill-days",
        type=int,
        default=7,
        help=f"How many previous days of history to upsert on each run. Max: {MAX_HISTORY_BACKFILL_DAYS}. Default: %(default)s",
    )
    sync_parser.add_argument(
        "--forecast-days",
        type=int,
        default=30,
        help=f"How many upcoming daily forecasts to keep from the daily page. Max: {MAX_FORECAST_DAYS}. Default: %(default)s",
    )
    sync_parser.add_argument(
        "--request-timeout",
        type=int,
        default=30,
        help="HTTP timeout in seconds. Default: %(default)s",
    )
    sync_parser.add_argument(
        "--min-request-interval",
        type=float,
        default=0.5,
        help="Minimum delay in seconds between outbound HTTP requests. Default: %(default)s",
    )
    sync_parser.add_argument("--dry-run", action="store_true", help="Fetch and parse, but do not write SQLite")
    sync_parser.set_defaults(handler=handle_sync)
    return parser


def configure_logging(level: str) -> None:
    logging.basicConfig(level=getattr(logging, level), format="%(asctime)s %(levelname)s %(message)s")


def month_start(any_date: date) -> date:
    return any_date.replace(day=1)


def previous_month(any_date: date) -> date:
    return (any_date.replace(day=1) - timedelta(days=1)).replace(day=1)


def parse_iso_date(value: str, flag_name: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise SystemExit(f"{flag_name} must be in YYYY-MM-DD format") from exc


def parse_temperature(text: str) -> int | None:
    match = re.search(r"-?\d+", html.unescape(text))
    return int(match.group()) if match else None


def clean_text(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = re.sub(r"\s+", " ", html.unescape(value)).strip()
    return cleaned or None


def validate_accuweather_url(url: str) -> str:
    parsed = urlparse(url)
    if parsed.scheme != "https" or parsed.netloc != ALLOWED_HOST:
        raise SyncError(f"Unexpected URL outside allowed host: {url}")
    return url


@contextmanager
def acquire_lock(lock_path: Path):
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    handle = lock_path.open("w")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as exc:
        handle.close()
        raise SystemExit(f"Another sync process is already running. Lock file: {lock_path}") from exc

    handle.write(f"{Path('/proc/self').resolve().name}\n")
    handle.flush()
    try:
        yield
    finally:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()


def infer_forecast_dates(md_values: list[str], reference_date: date) -> list[date]:
    resolved: list[date] = []
    current_year = reference_date.year
    previous_pair: tuple[int, int] | None = None
    for md in md_values:
        month_value, day_value = [int(part) for part in md.split("/")]
        current_pair = (month_value, day_value)
        if previous_pair and current_pair < previous_pair:
            current_year += 1
        resolved.append(date(current_year, month_value, day_value))
        previous_pair = current_pair
    return resolved


def extract_forecast_chunks(page_html: str) -> list[str]:
    parts = page_html.split('<div class="daily-wrapper"')
    return ['<div class="daily-wrapper"' + part for part in parts[1:]]


def parse_daily_forecasts(page_html: str, location: Location, reference_date: date) -> list[ForecastRecord]:
    chunks = extract_forecast_chunks(page_html)
    md_values: list[str] = []
    extracted: list[dict[str, object]] = []
    for chunk in chunks:
        href_match = re.search(r'<a class="daily-forecast-card\s*" href="([^"]+)"', chunk)
        md_match = re.search(r'<span class="module-header sub date">([^<]+)</span>', chunk)
        if not href_match or not md_match:
            continue
        high_match = re.search(r'<span class="high">([^<]+)</span>', chunk)
        low_match = re.search(r'<span class="low">/([^<]+)</span>', chunk)
        precip_match = re.search(r'<div class="precip">.*?(\d+)%\s*</div>', chunk, re.S)
        phrase_match = re.search(r'<div class="phrase">(.+?)</div>', chunk, re.S)
        md_text = clean_text(md_match.group(1))
        if md_text is None:
            continue
        md_values.append(md_text)
        detail_url = validate_accuweather_url(urljoin(BASE_URL, href_match.group(1)))
        extracted.append(
            {
                "detail_url": detail_url,
                "high_c": parse_temperature(high_match.group(1)) if high_match else None,
                "low_c": parse_temperature(low_match.group(1)) if low_match else None,
                "precip_probability": int(precip_match.group(1)) if precip_match else None,
                "phrase": clean_text(phrase_match.group(1)) if phrase_match else None,
            }
        )

    resolved_dates = infer_forecast_dates(md_values, reference_date)
    records: list[ForecastRecord] = []
    for resolved_date, raw in zip(resolved_dates, extracted, strict=True):
        records.append(
            ForecastRecord(
                location_key=location.key,
                weather_date=resolved_date,
                high_c=raw["high_c"],  # type: ignore[arg-type]
                low_c=raw["low_c"],  # type: ignore[arg-type]
                precip_probability=raw["precip_probability"],  # type: ignore[arg-type]
                phrase=raw["phrase"],  # type: ignore[arg-type]
                detail_url=raw["detail_url"],  # type: ignore[arg-type]
                source_url=location.daily_url,
            )
        )
    return records


def parse_monthly_history(page_html: str, location: Location, target_month: date) -> list[HistoryRecord]:
    panel_pattern = re.compile(
        r'<a(?P<attrs>[^>]*)class="(?P<classes>monthly-daypanel[^"]*)"(?P<after>[^>]*)>(?P<body>.*?)</a>',
        re.S,
    )
    records: list[HistoryRecord] = []
    source_url = location.monthly_url(target_month)

    for match in panel_pattern.finditer(page_html):
        classes = match.group("classes")
        if "is-past" not in classes:
            continue
        body = match.group("body")
        day_match = re.search(r'<div class="date">\s*(\d+)\s*</div>', body)
        high_match = re.search(r'<div class="high\s*">([^<]+)</div>', body)
        low_match = re.search(r'<div class="low">([^<]+)</div>', body)
        if not day_match:
            continue
        day_value = int(day_match.group(1))
        records.append(
            HistoryRecord(
                location_key=location.key,
                weather_date=date(target_month.year, target_month.month, day_value),
                actual_high_c=parse_temperature(high_match.group(1)) if high_match else None,
                actual_low_c=parse_temperature(low_match.group(1)) if low_match else None,
                source_url=source_url,
            )
        )
    return records


class AccuWeatherClient:
    def __init__(
        self,
        timeout: int,
        retries: int = 3,
        backoff_seconds: float = 5.0,
        min_request_interval: float = 0.5,
    ) -> None:
        self.timeout = timeout
        self.retries = retries
        self.backoff_seconds = backoff_seconds
        self.min_request_interval = min_request_interval
        self._last_request_monotonic = 0.0
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": USER_AGENT,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
                "Referer": BASE_URL,
            }
        )

    def fetch(self, url: str) -> str:
        validate_accuweather_url(url)
        for attempt in range(1, self.retries + 1):
            self._respect_min_interval()
            logging.debug("Fetching %s attempt=%s", url, attempt)
            response = self.session.get(url, timeout=self.timeout)
            self._last_request_monotonic = time.monotonic()
            validate_accuweather_url(response.url)
            if response.ok:
                content_type = response.headers.get("Content-Type", "")
                if "text/html" not in content_type:
                    raise SyncError(f"Unexpected content type for {url}: {content_type or 'missing'}")
                return response.text
            if response.status_code not in {403, 429, 500, 502, 503, 504} or attempt == self.retries:
                response.raise_for_status()
            sleep_seconds = self.backoff_seconds * attempt
            logging.warning(
                "Fetch failed url=%s status=%s attempt=%s/%s. Retrying in %.1fs",
                url,
                response.status_code,
                attempt,
                self.retries,
                sleep_seconds,
            )
            time.sleep(sleep_seconds)
        raise RuntimeError(f"Unreachable fetch retry loop for {url}")

    def _respect_min_interval(self) -> None:
        elapsed = time.monotonic() - self._last_request_monotonic
        remaining = self.min_request_interval - elapsed
        if remaining > 0:
            time.sleep(remaining)


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        PRAGMA foreign_keys = ON;
        PRAGMA journal_mode = WAL;
        PRAGMA busy_timeout = 5000;

        CREATE TABLE IF NOT EXISTS locations (
            location_key TEXT PRIMARY KEY,
            city TEXT NOT NULL,
            admin_area TEXT NOT NULL,
            country TEXT NOT NULL,
            slug TEXT NOT NULL,
            today_url TEXT NOT NULL,
            daily_url TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS forecast_daily (
            location_key TEXT NOT NULL,
            weather_date TEXT NOT NULL,
            high_c INTEGER,
            low_c INTEGER,
            precip_probability INTEGER,
            phrase TEXT,
            detail_url TEXT NOT NULL,
            source_url TEXT NOT NULL,
            scraped_at TEXT NOT NULL,
            PRIMARY KEY (location_key, weather_date),
            FOREIGN KEY (location_key) REFERENCES locations(location_key)
        );

        CREATE TABLE IF NOT EXISTS history_daily (
            location_key TEXT NOT NULL,
            weather_date TEXT NOT NULL,
            actual_high_c INTEGER,
            actual_low_c INTEGER,
            source_url TEXT NOT NULL,
            scraped_at TEXT NOT NULL,
            PRIMARY KEY (location_key, weather_date),
            FOREIGN KEY (location_key) REFERENCES locations(location_key)
        );

        CREATE INDEX IF NOT EXISTS idx_forecast_daily_date ON forecast_daily (weather_date);
        CREATE INDEX IF NOT EXISTS idx_history_daily_date ON history_daily (weather_date);
        """
    )


def upsert_locations(conn: sqlite3.Connection, locations: Iterable[Location]) -> None:
    conn.executemany(
        """
        INSERT INTO locations (location_key, city, admin_area, country, slug, today_url, daily_url)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(location_key) DO UPDATE SET
            city = excluded.city,
            admin_area = excluded.admin_area,
            country = excluded.country,
            slug = excluded.slug,
            today_url = excluded.today_url,
            daily_url = excluded.daily_url
        """,
        [
            (
                location.key,
                location.city,
                location.admin_area,
                location.country,
                location.slug,
                location.today_url,
                location.daily_url,
            )
            for location in locations
        ],
    )


def upsert_forecasts(conn: sqlite3.Connection, scraped_at: str, records: Iterable[ForecastRecord]) -> int:
    rows = [
        (
            record.location_key,
            record.weather_date.isoformat(),
            record.high_c,
            record.low_c,
            record.precip_probability,
            record.phrase,
            record.detail_url,
            record.source_url,
            scraped_at,
        )
        for record in records
    ]
    conn.executemany(
        """
        INSERT INTO forecast_daily (
            location_key, weather_date, high_c, low_c, precip_probability, phrase, detail_url, source_url, scraped_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(location_key, weather_date) DO UPDATE SET
            high_c = excluded.high_c,
            low_c = excluded.low_c,
            precip_probability = excluded.precip_probability,
            phrase = excluded.phrase,
            detail_url = excluded.detail_url,
            source_url = excluded.source_url,
            scraped_at = excluded.scraped_at
        """,
        rows,
    )
    return len(rows)


def upsert_history(conn: sqlite3.Connection, scraped_at: str, records: Iterable[HistoryRecord]) -> int:
    rows = [
        (
            record.location_key,
            record.weather_date.isoformat(),
            record.actual_high_c,
            record.actual_low_c,
            record.source_url,
            scraped_at,
        )
        for record in records
    ]
    conn.executemany(
        """
        INSERT INTO history_daily (
            location_key, weather_date, actual_high_c, actual_low_c, source_url, scraped_at
        )
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(location_key, weather_date) DO UPDATE SET
            actual_high_c = excluded.actual_high_c,
            actual_low_c = excluded.actual_low_c,
            source_url = excluded.source_url,
            scraped_at = excluded.scraped_at
        """,
        rows,
    )
    return len(rows)


def filter_dates[T](records: Iterable[T], allowed_dates: set[date]) -> list[T]:
    return [record for record in records if getattr(record, "weather_date") in allowed_dates]


def collect_months(start_date: date, end_date: date) -> list[date]:
    months: list[date] = []
    current = month_start(start_date)
    last = month_start(end_date)
    while current <= last:
        months.append(current)
        next_month = (current.replace(day=28) + timedelta(days=4)).replace(day=1)
        current = next_month
    return months


def handle_list_locations(args: argparse.Namespace) -> int:
    for location in LOCATIONS:
        print(f"{location.city}\t{location.key}\t{location.slug}\t{location.today_url}")
    return 0


def require_db_path(db_path: Path) -> None:
    if not db_path.exists():
        raise SystemExit(f"SQLite database not found: {db_path}")


def open_db(db_path: Path) -> sqlite3.Connection:
    require_db_path(db_path)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def handle_status(args: argparse.Namespace) -> int:
    db_path = Path(args.db)
    with open_db(db_path) as conn:
        location_count = conn.execute("SELECT COUNT(*) AS count FROM locations").fetchone()["count"]
        forecast_count = conn.execute("SELECT COUNT(*) AS count FROM forecast_daily").fetchone()["count"]
        history_count = conn.execute("SELECT COUNT(*) AS count FROM history_daily").fetchone()["count"]
        forecast_range = conn.execute(
            "SELECT MIN(weather_date) AS min_date, MAX(weather_date) AS max_date, MAX(scraped_at) AS scraped_at FROM forecast_daily"
        ).fetchone()
        history_range = conn.execute(
            "SELECT MIN(weather_date) AS min_date, MAX(weather_date) AS max_date, MAX(scraped_at) AS scraped_at FROM history_daily"
        ).fetchone()
        per_city = conn.execute(
            """
            SELECT
                l.city,
                COALESCE(f.forecast_count, 0) AS forecast_count,
                COALESCE(h.history_count, 0) AS history_count
            FROM locations l
            LEFT JOIN (
                SELECT location_key, COUNT(*) AS forecast_count
                FROM forecast_daily
                GROUP BY location_key
            ) f ON f.location_key = l.location_key
            LEFT JOIN (
                SELECT location_key, COUNT(*) AS history_count
                FROM history_daily
                GROUP BY location_key
            ) h ON h.location_key = l.location_key
            ORDER BY l.city
            """
        ).fetchall()

    print(f"Database: {db_path.resolve()}")
    print(f"Locations: {location_count}")
    print(
        f"Forecast rows: {forecast_count} | range: {forecast_range['min_date']} -> {forecast_range['max_date']} "
        f"| last scrape: {forecast_range['scraped_at']}"
    )
    print(
        f"History rows: {history_count} | range: {history_range['min_date']} -> {history_range['max_date']} "
        f"| last scrape: {history_range['scraped_at']}"
    )
    print("Per city:")
    for row in per_city:
        print(f"- {row['city']}: forecast={row['forecast_count']} history={row['history_count']}")
    return 0


def handle_export_csv(args: argparse.Namespace) -> int:
    db_path = Path(args.db)
    output_path = Path(args.output)
    date_from = parse_iso_date(args.date_from, "--date-from") if args.date_from else None
    date_to = parse_iso_date(args.date_to, "--date-to") if args.date_to else None
    if date_from and date_to and date_from > date_to:
        raise SystemExit("--date-from must be <= --date-to")

    if args.table == "forecast_daily":
        select_sql = """
            SELECT
                l.city,
                l.admin_area,
                l.country,
                f.weather_date,
                f.high_c,
                f.low_c,
                f.precip_probability,
                f.phrase,
                f.detail_url,
                f.source_url,
                f.scraped_at
            FROM forecast_daily f
            JOIN locations l ON l.location_key = f.location_key
        """
        fieldnames = [
            "city",
            "admin_area",
            "country",
            "weather_date",
            "high_c",
            "low_c",
            "precip_probability",
            "phrase",
            "detail_url",
            "source_url",
            "scraped_at",
        ]
    else:
        select_sql = """
            SELECT
                l.city,
                l.admin_area,
                l.country,
                h.weather_date,
                h.actual_high_c,
                h.actual_low_c,
                h.source_url,
                h.scraped_at
            FROM history_daily h
            JOIN locations l ON l.location_key = h.location_key
        """
        fieldnames = [
            "city",
            "admin_area",
            "country",
            "weather_date",
            "actual_high_c",
            "actual_low_c",
            "source_url",
            "scraped_at",
        ]

    conditions: list[str] = []
    params: list[str] = []
    if args.city:
        conditions.append("l.city = ?")
        params.append(args.city)
    if date_from:
        conditions.append("weather_date >= ?")
        params.append(date_from.isoformat())
    if date_to:
        conditions.append("weather_date <= ?")
        params.append(date_to.isoformat())

    query = select_sql
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    query += " ORDER BY city, weather_date"

    with open_db(db_path) as conn:
        rows = conn.execute(query, params).fetchall()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(dict(row))

    print(f"Exported {len(rows)} rows from {args.table} to {output_path.resolve()}")
    return 0


def handle_sync(args: argparse.Namespace) -> int:
    if args.history_backfill_days < 1:
        raise SystemExit("--history-backfill-days must be >= 1")
    if args.forecast_days < 1:
        raise SystemExit("--forecast-days must be >= 1")
    if args.history_backfill_days > MAX_HISTORY_BACKFILL_DAYS:
        raise SystemExit(f"--history-backfill-days must be <= {MAX_HISTORY_BACKFILL_DAYS}")
    if args.forecast_days > MAX_FORECAST_DAYS:
        raise SystemExit(f"--forecast-days must be <= {MAX_FORECAST_DAYS}")
    if args.min_request_interval < 0:
        raise SystemExit("--min-request-interval must be >= 0")

    timezone = ZoneInfo(args.timezone)
    local_today = datetime.now(timezone).date()
    history_start = local_today - timedelta(days=args.history_backfill_days)
    history_end = local_today - timedelta(days=1)
    history_dates = {
        history_start + timedelta(days=offset)
        for offset in range((history_end - history_start).days + 1)
    }
    forecast_end = local_today + timedelta(days=args.forecast_days - 1)
    forecast_dates = {
        local_today + timedelta(days=offset)
        for offset in range((forecast_end - local_today).days + 1)
    }
    scraped_at = datetime.now(timezone).isoformat()
    client = AccuWeatherClient(timeout=args.request_timeout, min_request_interval=args.min_request_interval)
    lock_path = Path(args.lock_file)

    with acquire_lock(lock_path):
        all_forecasts: list[ForecastRecord] = []
        all_history: list[HistoryRecord] = []

        for location in LOCATIONS:
            daily_html = client.fetch(location.daily_url)
            parsed_forecasts = parse_daily_forecasts(daily_html, location, local_today)
            if len(parsed_forecasts) < args.forecast_days:
                raise SyncError(
                    f"Forecast parser returned too few rows for {location.city}: "
                    f"expected at least {args.forecast_days}, got {len(parsed_forecasts)}"
                )
            kept_forecasts = filter_dates(parsed_forecasts, forecast_dates)
            logging.info("%s forecast rows parsed=%s kept=%s", location.city, len(parsed_forecasts), len(kept_forecasts))
            all_forecasts.extend(kept_forecasts)

            for target_month in collect_months(history_start, history_end):
                monthly_html = client.fetch(location.monthly_url(target_month))
                parsed_history = parse_monthly_history(monthly_html, location, target_month)
                expected_in_month = sum(1 for item in history_dates if month_start(item) == target_month)
                kept_history = filter_dates(parsed_history, history_dates)
                if len(kept_history) < expected_in_month:
                    raise SyncError(
                        f"History parser returned too few rows for {location.city} month={target_month:%Y-%m}: "
                        f"expected {expected_in_month}, got {len(kept_history)}"
                    )
                logging.info(
                    "%s history month=%s parsed=%s kept=%s",
                    location.city,
                    target_month.strftime("%Y-%m"),
                    len(parsed_history),
                    len(kept_history),
                )
                all_history.extend(kept_history)

        if args.dry_run:
            print(f"Dry run only. Forecast rows: {len(all_forecasts)}. History rows: {len(all_history)}.")
            return 0

        db_path = Path(args.db)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(db_path) as conn:
            ensure_schema(conn)
            upsert_locations(conn, LOCATIONS)
            forecast_count = upsert_forecasts(conn, scraped_at, all_forecasts)
            history_count = upsert_history(conn, scraped_at, all_history)
            conn.commit()

        print(
            f"Synced {forecast_count} forecast rows and {history_count} history rows into {db_path.resolve()} "
            f"for run date {local_today.isoformat()} ({args.timezone})."
        )
        return 0


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    configure_logging(args.log_level)
    return args.handler(args)


if __name__ == "__main__":
    raise SystemExit(main())
