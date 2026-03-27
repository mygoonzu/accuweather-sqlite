# AccuWeather SQLite Sync

Tool Python nhỏ để lấy dữ liệu thời tiết từ AccuWeather cho:

- Hà Nội
- Hồ Chí Minh
- Đà Nẵng

Mỗi lần chạy `sync`:

- lấy forecast hằng ngày từ trang `daily-weather-forecast`
- lấy lịch sử nhiệt độ ngày trước đó từ trang `monthly`
- upsert vào SQLite

## Cài đặt

```bash
cd /home/ruby/accuweather-sqlite
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Chạy đồng bộ

```bash
python3 weather_sync.py sync --db data/weather.db
```

Tuỳ chọn hữu ích:

```bash
python3 weather_sync.py sync --db data/weather.db --history-backfill-days 7 --forecast-days 30
python3 weather_sync.py sync --db data/weather.db --dry-run --log-level DEBUG
python3 weather_sync.py list-locations
```

## Schema SQLite

Database tạo 3 bảng:

- `locations`: metadata location AccuWeather
- `history_daily`: dữ liệu quá khứ theo ngày, hiện lưu `actual_high_c`, `actual_low_c`
- `forecast_daily`: dữ liệu forecast theo ngày, lưu `high_c`, `low_c`, `precip_probability`, `phrase`

Ví dụ query:

```sql
SELECT l.city, f.weather_date, f.high_c, f.low_c, f.precip_probability, f.phrase
FROM forecast_daily f
JOIN locations l ON l.location_key = f.location_key
ORDER BY l.city, f.weather_date;
```

## Chạy hằng ngày bằng cron

Ví dụ chạy lúc `00:10` mỗi ngày theo timezone server:

```cron
10 0 * * * cd /home/ruby/accuweather-sqlite && /usr/bin/python3 weather_sync.py sync --db /home/ruby/accuweather-sqlite/data/weather.db >> /home/ruby/accuweather-sqlite/sync.log 2>&1
```

Nếu muốn ngày chạy bám theo giờ Việt Nam, giữ tham số mặc định `--timezone Asia/Ho_Chi_Minh`.

## Lưu ý

- Tool này scrape HTML của AccuWeather, không dùng API chính thức.
- Trang `monthly` hiện phù hợp để lấy lịch sử nhiệt độ ngày đã qua; forecast chi tiết lấy từ trang `daily`.
- Nếu AccuWeather đổi cấu trúc HTML, parser có thể cần cập nhật.
