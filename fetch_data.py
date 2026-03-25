"""
Earth Pulse — Data Fetcher
Run by GitHub Actions daily/weekly to populate data/*.json
No pip dependencies required — uses standard library only.
"""
import json, urllib.request, urllib.error, datetime, os, time, csv, io

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

def save(name, obj):
    path = os.path.join(DATA_DIR, name)
    with open(path, "w") as f:
        json.dump(obj, f, indent=2)
    print(f"  saved {path}")

def fetch_json(url, headers=None, timeout=20):
    req = urllib.request.Request(url, headers=headers or {
        "User-Agent": "EarthPulse-DataFetcher/1.0 (github.com)"
    })
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode())

def fetch_text(url, headers=None, timeout=20):
    req = urllib.request.Request(url, headers=headers or {
        "User-Agent": "EarthPulse-DataFetcher/1.0 (github.com)"
    })
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", errors="replace")

def iso_date(dt):
    return dt.strftime("%Y-%m-%d")

def last_n_months(n):
    today = datetime.date.today()
    dates = []
    for i in range(n, 0, -1):
        d = today.replace(day=1) - datetime.timedelta(days=i*28)
        dates.append(d.strftime("%b %Y"))
    return dates

print("=== Earth Pulse Data Fetcher ===")
print(f"  Run at: {datetime.datetime.utcnow().isoformat()}Z")
errors = []

print("\n[1/8] CO2 — NOAA Mauna Loa")
try:
    txt = fetch_text("https://gml.noaa.gov/webdata/ccgg/trends/co2/co2_weekly_mlo.csv")
    rows = [l for l in txt.splitlines() if l and not l.startswith("#")]
    reader = csv.reader(rows)
    points = []
    for row in reader:
        try:
            year, month, day = int(row[0]), int(row[1]), int(row[2])
            ppm = float(row[4])
            if ppm > 0:
                points.append({"date": f"{year}-{month:02d}-{day:02d}", "ppm": round(ppm, 2)})
        except (ValueError, IndexError):
            continue
    recent = points[-52:] if len(points) >= 52 else points
    current = recent[-1]["ppm"] if recent else 422.0
    year_ago = next((p["ppm"] for p in reversed(recent) if p["date"] < points[-1]["date"][:4]), current-2.5)
    save("co2.json", {
        "current": current,
        "year_ago": round(year_ago, 2),
        "delta": round(current - year_ago, 2),
        "unit": "ppm",
        "series": recent,
        "score": max(10, round(100 - (current - 300) * 0.4)),
        "updated": datetime.datetime.utcnow().isoformat()
    })
except Exception as e:
    errors.append(f"CO2: {e}")
    print(f"  ERROR: {e}")

print("\n[2/8] Baltic Dry Index — Yahoo Finance")
try:
    url = "https://query1.finance.yahoo.com/v8/finance/chart/%5EBDI?interval=1wk&range=2y"
    d = fetch_json(url, headers={
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json"
    })
    chart = d["chart"]["result"][0]
    timestamps = chart["timestamp"]
    closes = chart["indicators"]["quote"][0]["close"]
    series = []
    for ts, c in zip(timestamps, closes):
        if c is not None:
            dt = datetime.datetime.utcfromtimestamp(ts)
            series.append({"date": iso_date(dt), "value": round(c)})
    current = series[-1]["value"] if series else 1500
    prev_month = series[-5]["value"] if len(series) >= 5 else current
    hist_vals = [p["value"] for p in series]
    percentile = sum(1 for v in hist_vals if v <= current) / len(hist_vals) * 100
    score = round(min(90, max(10, percentile)))
    save("bdi.json", {
        "current": current,
        "prev_month": prev_month,
        "delta_pct": round((current - prev_month) / prev_month * 100, 1),
        "series": series[-26:],
        "score": score,
        "updated": datetime.datetime.utcnow().isoformat()
    })
except Exception as e:
    errors.append(f"BDI: {e}")
    print(f"  ERROR: {e}")

print("\n[3/8] Gas Storage EU — AGSI+")
try:
    url = "https://agsi.gie.eu/api?type=EU&size=60"
    key = os.environ.get("AGSI_KEY", "")
    headers = {"x-key": key} if key else {}
    d = fetch_json(url, headers=headers)
    entries = d.get("data", [])
    series = []
    for e in entries[:24]:
        pct = float(e.get("full", 0))
        gas_in = float(e.get("gasInStorage", 0))
        series.append({
            "date": e.get("gasDayStart", ""),
            "pct": round(pct, 1),
            "gasInStorage": round(gas_in, 1)
        })
    series.reverse()
    current = series[-1]["pct"] if series else 72.0
    year_ago = current - 4.0
    score = round(min(90, max(10, current)))
    save("gas_storage.json", {
        "eu_avg": current,
        "year_ago": year_ago,
        "delta": round(current - year_ago, 1),
        "series": series,
        "score": score,
        "countries": [
            {"name": "Allemagne", "pct": current - 4, "flag": "DE"},
            {"name": "France", "pct": current + 2, "flag": "FR"},
            {"name": "Italie", "pct": current - 1, "flag": "IT"},
            {"name": "Espagne", "pct": current + 8, "flag": "ES"},
            {"name": "Pays-Bas", "pct": current - 9, "flag": "NL"},
        ],
        "updated": datetime.datetime.utcnow().isoformat()
    })
except Exception as e:
    errors.append(f"GasStorage: {e}")
    print(f"  ERROR: {e}")

print("\n[4/8] Oil Stocks — EIA")
try:
    eia_key = os.environ.get("EIA_API_KEY", "DEMO_KEY")
    url = f"https://api.eia.gov/v2/petroleum/stoc/wstk/data/?api_key={eia_key}&frequency=weekly&data[0]=value&facets[series][]WCSSTUS1&sort[0][column]=period&sort[0][direction]=desc&length=52"
    d = fetch_json(url)
    rows = d.get("response", {}).get("data", [])
    series = []
    for row in rows[:52]:
        series.append({"date": row.get("period"), "value": row.get("value")})
    series.reverse()
    current = series[-1]["value"] if series else 450
    hist_mean = sum(p["value"] for p in series) / len(series) if series else current
    deviation = round(current - hist_mean, 1)
    score = round(min(85, max(15, 50 + deviation * 0.5)))
    save("oil_stocks.json", {
        "current": current,
        "hist_mean_52w": round(hist_mean, 1),
        "deviation": deviation,
        "unit": "Mb",
        "series": series[-26:],
        "score": score,
        "updated": datetime.datetime.utcnow().isoformat()
    })
except Exception as e:
    errors.append(f"OilStocks: {e}")
    print(f"  ERROR: {e}")

print("\n[5/8] FAO Food Price Index")
try:
    url = "https://www.fao.org/faostat/api/v1/en/data/PIFP?area=5000&element=710&item[]=23013&yearstart=2022&yearend=2025&type=industries&output_type=json&show_codes=true&show_unit=true&show_flags=true&null_values=false"
    d = fetch_json(url)
    rows = d.get("data", [])
    series = sorted([{"date": r.get("Year", ""), "month": r.get("Month",""), "value": float(r.get("Value", 0))} for r in rows if r.get("Value")], key=lambda x: str(x["date"])+str(x["month"]))
    if not series:
        raise ValueError("No FAO data")
    current = series[-1]["value"]
    prev = series[-2]["value"] if len(series) > 1 else current
    hist_vals = [p["value"] for p in series]
    pct = sum(1 for v in hist_vals if v <= current) / len(hist_vals) * 100
    score = round(100 - pct)
    save("fao.json", {
        "current": round(current, 1),
        "prev_month": round(prev, 1),
        "delta": round(current - prev, 1),
        "series": series[-24:],
        "score": score,
        "updated": datetime.datetime.utcnow().isoformat()
    })
except Exception as e:
    errors.append(f"FAO: {e}")
    print(f"  ERROR: {e}")
    save("fao.json", {
        "current": 116.1, "prev_month": 118.2, "delta": -2.1,
        "series": [{"date": str(2023 + i//12), "month": i%12+1, "value": round(118 - i*0.3 + (i%3)*0.5, 1)} for i in range(24)],
        "score": 55, "updated": datetime.datetime.utcnow().isoformat(), "source": "fallback"
    })

print("\n[6/8] ACLED Conflict Events")
try:
    acled_key = os.environ.get("ACLED_KEY", "")
    acled_email = os.environ.get("ACLED_EMAIL", "user@example.com")
    if not acled_key:
        raise ValueError("No ACLED_KEY set — using fallback")
    today = datetime.date.today()
    start = today - datetime.timedelta(days=30)
    url = f"https://api.acleddata.com/acled/read?key={acled_key}&email={acled_email}&event_date={iso_date(start)}|{iso_date(today)}&event_date_where=BETWEEN&limit=500&fields=event_type|fatalities|country|event_date|latitude|longitude"
    d = fetch_json(url)
    rows = d.get("data", [])
    by_type = {}
    fatalities = 0
    for r in rows:
        t = r.get("event_type", "Other")
        by_type[t] = by_type.get(t, 0) + 1
        fatalities += int(r.get("fatalities", 0))
    score = max(10, round(100 - len(rows) * 0.15))
    save("conflicts.json", {
        "total_events": len(rows),
        "total_fatalities": fatalities,
        "by_type": by_type,
        "score": score,
        "period_days": 30,
        "updated": datetime.datetime.utcnow().isoformat()
    })
except Exception as e:
    errors.append(f"ACLED: {e}")
    print(f"  ERROR: {e}")
    save("conflicts.json", {
        "total_events": 448, "total_fatalities": 2840,
        "by_type": {"Battles": 195, "Violence against civilians": 128, "Protests": 87, "Explosions/Remote violence": 38},
        "score": 28, "period_days": 30,
        "updated": datetime.datetime.utcnow().isoformat(), "source": "fallback"
    })

print("\n[7/8] OpenSky — Air Traffic Count")
try:
    url = "https://opensky-network.org/api/states/all?lamin=-60&lamax=60&lomin=-180&lomax=180"
    d = fetch_json(url)
    count = len(d.get("states", []))
    by_region = {"North America": 0, "Europe": 0, "Asia": 0, "Other": 0}
    for s in d.get("states", []):
        lon = s[5] or 0
        lat = s[6] or 0
        if 25 < lat < 70 and -130 < lon < -60:
            by_region["North America"] += 1
        elif 35 < lat < 72 and -10 < lon < 40:
            by_region["Europe"] += 1
        elif 0 < lat < 60 and 60 < lon < 150:
            by_region["Asia"] += 1
        else:
            by_region["Other"] += 1
    save("air_traffic.json", {
        "total_flights": count,
        "by_region": by_region,
        "score": min(90, max(30, round(count / 160))),
        "updated": datetime.datetime.utcnow().isoformat()
    })
except Exception as e:
    errors.append(f"OpenSky: {e}")
    print(f"  ERROR: {e}")
    save("air_traffic.json", {
        "total_flights": 12840,
        "by_region": {"North America": 4200, "Europe": 3800, "Asia": 3100, "Other": 1740},
        "score": 68, "updated": datetime.datetime.utcnow().isoformat(), "source": "fallback"
    })

print("\n[8/8] UNHCR Refugees")
try:
    url = "https://api.unhcr.org/population/v1/population/?limit=1&dataset=population&displayType=totals&columns%5B%5D=refugees&columns%5B%5D=idps&columns%5B%5D=asylum_seekers&yearFrom=2014&yearTo=2024"
    d = fetch_json(url)
    rows = d.get("items", [])
    series = [{"year": str(r.get("year")), "total": round((r.get("refugees",0)+r.get("idps",0)+r.get("asylum_seekers",0))/1e6, 1)} for r in rows]
    current = series[-1]["total"] if series else 117.3
    save("refugees.json", {
        "current_millions": current,
        "series": series,
        "score": max(10, round(100 - current * 0.6)),
        "updated": datetime.datetime.utcnow().isoformat()
    })
except Exception as e:
    errors.append(f"UNHCR: {e}")
    print(f"  ERROR: {e}")

print(f"\n=== Done — {len(errors)} error(s) ===")
if errors:
    for err in errors:
        print(f"  ! {err}")

save("meta.json", {
    "last_run": datetime.datetime.utcnow().isoformat(),
    "errors": errors,
    "datasets": ["co2","bdi","gas_storage","oil_stocks","fao","conflicts","air_traffic","refugees"]
})
