import unittest
from datetime import date
from tempfile import TemporaryDirectory
from pathlib import Path
import argparse
import sqlite3

from weather_sync import (
    LOCATIONS,
    ForecastRecord,
    HistoryRecord,
    SyncError,
    ensure_schema,
    handle_export_csv,
    handle_status,
    parse_daily_forecasts,
    parse_monthly_history,
    upsert_forecasts,
    upsert_history,
    upsert_locations,
    validate_accuweather_url,
)


FORECAST_HTML = """
<div class="daily-wrapper" data-qa="dailyCard0">
  <a class="daily-forecast-card " href="/en/vn/hanoi/353412/weather-today/353412">
    <span class="module-header sub date">12/31</span>
    <div class="temp">
      <span class="high">28&#xB0;</span>
      <span class="low">/19&#xB0;</span>
    </div>
    <div class="precip">65%</div>
  </a>
  <div class="half-day-card-content ">
    <div class="phrase">Warm with scattered showers</div>
  </div>
</div>
<div class="daily-wrapper" data-qa="dailyCard1">
  <a class="daily-forecast-card " href="/en/vn/hanoi/353412/weather-tomorrow/353412">
    <span class="module-header sub date">1/1</span>
    <div class="temp">
      <span class="high">26&#xB0;</span>
      <span class="low">/17&#xB0;</span>
    </div>
    <div class="precip">40%</div>
  </a>
  <div class="half-day-card-content ">
    <div class="phrase">Sunny breaks</div>
  </div>
</div>
"""


MONTHLY_HTML = """
<div class="monthly-calendar">
  <a class="monthly-daypanel  is-past ">
    <div class="monthly-panel-top">
      <div class="date">30</div>
    </div>
    <div class="temp">
      <div class="high  ">31&#xB0;</div>
      <div class="low">22&#xB0;</div>
    </div>
  </a>
  <a class="monthly-daypanel   is-today" href="/en/vn/hanoi/353412/weather-today/353412">
    <div class="monthly-panel-top">
      <div class="date">31</div>
    </div>
    <div class="temp">
      <div class="high  ">29&#xB0;</div>
      <div class="low">21&#xB0;</div>
    </div>
  </a>
</div>
"""


class ParserTests(unittest.TestCase):
    def test_parse_daily_forecasts_handles_year_rollover(self) -> None:
        records = parse_daily_forecasts(FORECAST_HTML, LOCATIONS[0], date(2026, 12, 31))

        self.assertEqual(len(records), 2)
        self.assertEqual(records[0].weather_date.isoformat(), "2026-12-31")
        self.assertEqual(records[1].weather_date.isoformat(), "2027-01-01")
        self.assertEqual(records[0].precip_probability, 65)
        self.assertEqual(records[0].phrase, "Warm with scattered showers")

    def test_parse_monthly_history_keeps_only_past_panels(self) -> None:
        records = parse_monthly_history(MONTHLY_HTML, LOCATIONS[0], date(2026, 3, 1))

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].weather_date.isoformat(), "2026-03-30")
        self.assertEqual(records[0].actual_high_c, 31)
        self.assertEqual(records[0].actual_low_c, 22)

    def test_validate_accuweather_url_rejects_untrusted_host(self) -> None:
        with self.assertRaises(SyncError):
            validate_accuweather_url("https://evil.example.com/path")

    def test_status_and_export_csv_work_with_sample_db(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "weather.db"
            output_path = Path(tmp_dir) / "forecast.csv"
            with sqlite3.connect(db_path) as conn:
                ensure_schema(conn)
                upsert_locations(conn, LOCATIONS)
                upsert_forecasts(
                    conn,
                    "2026-03-27T00:00:00+07:00",
                    [
                        ForecastRecord(
                            location_key=LOCATIONS[0].key,
                            weather_date=date(2026, 3, 27),
                            high_c=30,
                            low_c=24,
                            precip_probability=55,
                            phrase="Cloudy",
                            detail_url=LOCATIONS[0].daily_url,
                            source_url=LOCATIONS[0].daily_url,
                        )
                    ],
                )
                upsert_history(
                    conn,
                    "2026-03-27T00:00:00+07:00",
                    [
                        HistoryRecord(
                            location_key=LOCATIONS[0].key,
                            weather_date=date(2026, 3, 26),
                            actual_high_c=29,
                            actual_low_c=22,
                            source_url=LOCATIONS[0].monthly_url(date(2026, 3, 1)),
                        )
                    ],
                )
                conn.commit()

            status_args = argparse.Namespace(db=str(db_path))
            export_args = argparse.Namespace(
                db=str(db_path),
                table="forecast_daily",
                output=str(output_path),
                city="Ha Noi",
                date_from="2026-03-27",
                date_to="2026-03-27",
            )

            self.assertEqual(handle_status(status_args), 0)
            self.assertEqual(handle_export_csv(export_args), 0)
            csv_text = output_path.read_text(encoding="utf-8")
            self.assertIn("Ha Noi", csv_text)
            self.assertIn("2026-03-27", csv_text)


if __name__ == "__main__":
    unittest.main()
