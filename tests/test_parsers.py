import unittest
from datetime import date

from weather_sync import LOCATIONS, parse_daily_forecasts, parse_monthly_history


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


if __name__ == "__main__":
    unittest.main()
