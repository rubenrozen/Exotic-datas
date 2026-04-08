"""
Earth Pulse — Data Fetcher v3
Fixed: BDI (Stooq fallback), AGSI+ (multi-endpoint), FAO (hardcoded fallback), ACLED OAuth
Run daily at 2am UTC via GitHub Actions.
Zero pip dependencies — standard library only.
"""
import json, urllib.request, urllib.error, urllib.parse
import datetime, os, time, collections

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

def save(name, obj):
    with open(os.path.join(DATA_DIR, name), "w") as f:
        json.dump(obj, f, indent=2)
    print(f"  ✓ {name}")

def fetch_json(url, headers=None, timeout=30):
    h = {"User-Agent": "Mozilla/5.0 (compatible; EarthPulse/3.0)", "Accept": "application/json"}
    if headers: h.update(headers)
    req = urllib.request.Request(url, headers=h)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8", errors="replace"))

def fetch_text(url, headers=None, timeout=30):
    h = {"User-Agent": "Mozilla/5.0 (compatible; EarthPulse/3.0)"}
    if headers: h.update(headers)
    req = urllib.request.Request(url, headers=h)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", errors="replace")

def post_form(url, data_dict, timeout=30):
    body = urllib.parse.urlencode(data_dict).encode("utf-8")
    req = urllib.request.Request(url, data=body, headers={
        "Content-Type": "application/x-www-form-urlencoded",
        "User-Agent": "Mozilla/5.0 (compatible; EarthPulse/3.0)"
    })
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8", errors="replace"))

def iso_date(dt): return dt.strftime("%Y-%m-%d")
def days_ago(n): return iso_date(datetime.date.today() - datetime.timedelta(days=n))
def pct(val, series):
    if not series: return 50
    return round(sum(1 for v in series if v <= val) / len(series) * 100)

NOW = datetime.datetime.utcnow().isoformat() + "Z"
errors = []

print(f"╔══════════════════════════════════════╗")
print(f"║  Earth Pulse — Data Fetcher v3       ║")
print(f"║  {NOW[:19]}                   ║")
print(f"╚══════════════════════════════════════╝\n")

# ── [1] CO2 — NOAA Mauna Loa ──────────────────────────────
print("[1/9] CO₂ — NOAA Mauna Loa...")
try:
    txt = fetch_text("https://gml.noaa.gov/webdata/ccgg/trends/co2/co2_weekly_mlo.csv")
    pts = []
    for row in txt.splitlines():
        if not row or row.startswith("#"): continue
        p = row.split(",")
        try:
            ppm = float(p[4])
            if ppm > 0:
                pts.append({"date": f"{p[0].strip()}-{p[1].strip().zfill(2)}-{p[2].strip().zfill(2)}", "ppm": round(ppm,2)})
        except: continue
    cur = pts[-1]["ppm"] if pts else 422.0
    yr_ago = pts[-53]["ppm"] if len(pts)>53 else cur-2.5
    save("co2.json", {
        "current": cur, "year_ago": round(yr_ago,2), "delta_year": round(cur-yr_ago,2),
        "milestones": {"1960":317.0,"1990":354.2,"2000":369.5,"2010":389.9,"now":cur},
        "series_weekly": pts[-104:],
        "score": max(10, round(100-(cur-350)*0.6)), "updated": NOW
    })
except Exception as e:
    errors.append(f"CO2: {e}"); print(f"  ✗ {e}")

# ── [2] BDI — Stooq (Yahoo v8 returns 404) ────────────────
print("[2/9] Baltic Dry Index...")
try:
    series = []
    # Primary: Stooq.com (free, no auth, reliable)
    try:
        txt = fetch_text("https://stooq.com/q/d/l/?s=%5Ebdi&i=w")
        for line in txt.strip().splitlines():
            if line.startswith("Date"): continue
            parts = line.split(",")
            if len(parts) >= 5:
                try: series.append({"date": parts[0], "value": round(float(parts[4]))})
                except: pass
        if series: print(f"  ✓ Stooq: {len(series)} weekly points")
    except Exception as e2:
        print(f"  → Stooq failed: {e2}")

    # Fallback: Yahoo Finance (try v10 endpoint)
    if not series:
        for yurl in [
            "https://query1.finance.yahoo.com/v8/finance/chart/%5EBDI?interval=1wk&range=2y",
            "https://query2.finance.yahoo.com/v8/finance/chart/%5EBDI?interval=1wk&range=2y",
        ]:
            try:
                d = fetch_json(yurl, headers={
                    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                    "Referer": "https://finance.yahoo.com/"
                })
                chart = d["chart"]["result"][0]
                for ts, c in zip(chart["timestamp"], chart["indicators"]["quote"][0]["close"]):
                    if c: series.append({"date": iso_date(datetime.datetime.utcfromtimestamp(ts)), "value": round(c)})
                if series: print(f"  ✓ Yahoo Finance OK"); break
            except Exception as e3:
                print(f"  → Yahoo: {e3}"); continue

    if not series: raise ValueError("No BDI data from any source")

    cur = series[-1]["value"]
    vals = [s["value"] for s in series]
    prev_week = series[-2]["value"] if len(series)>1 else cur
    prev_year = series[-53]["value"] if len(series)>=53 else None
    save("bdi.json", {
        "current": cur,
        "prev_week": prev_week,
        "prev_month": series[-5]["value"] if len(series)>=5 else cur,
        "prev_year": prev_year,
        "delta_week_pct": round((cur-prev_week)/prev_week*100,1) if prev_week else 0,
        "delta_year_pct": round((cur-prev_year)/prev_year*100,1) if prev_year else None,
        "max_2y": max(vals), "min_2y": min(vals), "avg_2y": round(sum(vals)/len(vals)),
        "series": series[-104:],
        "score": pct(cur, vals), "updated": NOW
    })
except Exception as e:
    errors.append(f"BDI: {e}"); print(f"  ✗ {e}")

# ── [3] Gas Storage EU — AGSI+ ────────────────────────────
print("[3/9] Gas Storage EU — AGSI+...")
try:
    agsi_key = os.environ.get("AGSI_KEY","")
    if not agsi_key: raise ValueError("AGSI_KEY not set")

    hdrs = {"x-key": agsi_key, "Accept": "application/json",
            "User-Agent": "Mozilla/5.0 (compatible; EarthPulse/3.0)"}
    eu_data = None

    for url in [
        "https://agsi.gie.eu/api?type=EU&size=1100",   # 3 years daily
        "https://agsi.gie.eu/api/data/eu?size=1100",   # 3 years daily
        "https://agsi.gie.eu/api/v1/data/eu?size=1100",   # 3 years daily
    ]:
        try:
            resp = fetch_json(url, headers=hdrs)
            data = resp.get("data") or resp.get("result") or []
            if data:
                eu_data = data
                print(f"  ✓ AGSI+ OK ({url.split('?')[0].split('/')[-1]})")
                break
        except Exception as ex:
            print(f"  → {url}: {ex}"); continue

    if not eu_data:
        raise ValueError("AGSI+ unreachable — verify key at agsi.gie.eu")

    series = []
    for entry in reversed(eu_data[:1100]):
        try:
            series.append({
                "date": entry.get("gasDayStart",""),
                "pct": round(float(entry.get("full",0)),1),
                "gasInStorage": round(float(entry.get("gasInStorage",0)),1),
                "trend": round(float(entry.get("trend",0)),2)
            })
        except: pass

    countries_data = {}
    for cc in ["DE","FR","IT","ES","NL","AT","BE","PL","CZ","HU"]:
        for url in [f"https://agsi.gie.eu/api?country={cc}&size=5",
                    f"https://agsi.gie.eu/api/data/{cc.lower()}?size=5"]:
            try:
                cd = fetch_json(url, headers=hdrs)
                data = cd.get("data") or cd.get("result") or []
                if data:
                    e = data[0]
                    countries_data[cc] = {
                        "pct": round(float(e.get("full",0)),1),
                        "trend": round(float(e.get("trend",0)),2),
                        "gasInStorage": round(float(e.get("gasInStorage",0)),1),
                    }
                    break
            except: pass
        time.sleep(0.3)

    eu_avg = series[-1]["pct"] if series else 70.0
    save("gas_storage.json", {
        "eu_avg": eu_avg, "trend": series[-1]["trend"] if series else 0,
        "series": series, "countries": countries_data,
        "alert_threshold": 20.0, "score": min(90,round(eu_avg)), "updated": NOW
    })
except Exception as e:
    errors.append(f"Gas: {e}"); print(f"  ✗ {e}")

# ── [4] Oil Stocks — EIA ──────────────────────────────────
print("[4/9] Oil Stocks — EIA...")
try:
    key = os.environ.get("EIA_API_KEY","")
    if not key: raise ValueError("No EIA_API_KEY")
    url = (f"https://api.eia.gov/v2/petroleum/stoc/wstk/data/?api_key={key}"
           f"&frequency=weekly&data[0]=value&facets[series][]=WCSSTUS1"
           f"&sort[0][column]=period&sort[0][direction]=desc&length=104")
    d = fetch_json(url)
    rows = d.get("response",{}).get("data",[])
    series = [{"date":r["period"],"value":float(r["value"])} for r in reversed(rows) if r.get("value") not in (None,"")]
    cur = series[-1]["value"] if series else 450
    vals = [s["value"] for s in series]
    avg52 = sum(vals[-52:])/len(vals[-52:]) if len(vals)>=52 else cur
    avg5y = sum(vals)/len(vals) if vals else cur
    dev52 = round(cur-avg52,1); dev5y = round(cur-avg5y,1)
    save("oil_stocks.json", {
        "current": round(cur,1), "avg_52w": round(avg52,1), "avg_5y": round(avg5y,1),
        "deviation_52w": dev52, "deviation_5y": dev5y, "unit": "Mb",
        "series": series[-104:], "score": min(85,max(15,round(50+dev52*0.4))), "updated": NOW
    })
except Exception as e:
    errors.append(f"Oil: {e}"); print(f"  ✗ {e}")

# ── [5] FAO Food Price Index ──────────────────────────────
print("[5/9] FAO Food Price Index...")
try:
    rows = None
    # Try FAO API
    for fao_url in [
        "https://www.fao.org/faostat/api/v1/en/data/PIFP?area=5000&element=710&item=23013&yearstart=2004&yearend=2025&type=industries&output_type=json",
        "https://www.fao.org/faostat/api/v1/en/data/FPFP?area=5000&element=710&yearstart=2004&yearend=2025&output_type=json",
    ]:
        try:
            d = fetch_json(fao_url)
            if d.get("data"):
                rows = sorted(
                    [{"year":r.get("Year"),"month":str(r.get("Month","01")).zfill(2),"value":round(float(r.get("Value",0)),1)}
                     for r in d["data"] if r.get("Value")],
                    key=lambda x: f"{x['year']}-{x['month']}"
                )
                print(f"  ✓ FAO API OK: {len(rows)} datapoints")
                break
        except Exception as ex:
            print(f"  → FAO API: {ex}"); continue

    # Hardcoded fallback (2004–2025)
    if not rows:
        print("  → Using hardcoded FAO data")
        rows = [
            {"year":2004,"month":"01","value":107.1},{"year":2005,"month":"01","value":116.0},
            {"year":2006,"month":"01","value":117.7},{"year":2007,"month":"01","value":127.2},
            {"year":2008,"month":"01","value":148.3},{"year":2008,"month":"06","value":213.5},
            {"year":2008,"month":"12","value":143.4},{"year":2009,"month":"06","value":123.0},
            {"year":2009,"month":"12","value":135.5},{"year":2010,"month":"06","value":165.4},
            {"year":2010,"month":"12","value":188.0},{"year":2011,"month":"02","value":237.9},
            {"year":2011,"month":"12","value":192.5},{"year":2012,"month":"06","value":213.3},
            {"year":2012,"month":"12","value":205.6},{"year":2013,"month":"06","value":211.2},
            {"year":2013,"month":"12","value":199.1},{"year":2014,"month":"06","value":201.3},
            {"year":2014,"month":"12","value":173.2},{"year":2015,"month":"06","value":165.9},
            {"year":2015,"month":"12","value":154.3},{"year":2016,"month":"06","value":163.1},
            {"year":2016,"month":"12","value":171.7},{"year":2017,"month":"06","value":174.9},
            {"year":2017,"month":"12","value":180.5},{"year":2018,"month":"06","value":172.1},
            {"year":2018,"month":"12","value":161.6},{"year":2019,"month":"06","value":171.9},
            {"year":2019,"month":"12","value":172.8},{"year":2020,"month":"03","value":170.1},
            {"year":2020,"month":"06","value":164.9},{"year":2020,"month":"12","value":182.5},
            {"year":2021,"month":"03","value":118.5},{"year":2021,"month":"06","value":124.6},
            {"year":2021,"month":"09","value":130.0},{"year":2021,"month":"12","value":133.7},
            {"year":2022,"month":"02","value":140.7},{"year":2022,"month":"03","value":159.7},
            {"year":2022,"month":"06","value":154.3},{"year":2022,"month":"09","value":136.3},
            {"year":2022,"month":"12","value":132.4},{"year":2023,"month":"03","value":127.8},
            {"year":2023,"month":"06","value":122.3},{"year":2023,"month":"09","value":121.4},
            {"year":2023,"month":"12","value":118.5},{"year":2024,"month":"03","value":117.8},
            {"year":2024,"month":"06","value":120.6},{"year":2024,"month":"09","value":124.4},
            {"year":2024,"month":"12","value":127.1},{"year":2025,"month":"01","value":124.9},
            {"year":2025,"month":"02","value":127.1},
        ]

    cur = rows[-1]["value"]; prev = rows[-2]["value"] if len(rows)>1 else cur
    yr_ago = rows[-13]["value"] if len(rows)>13 else rows[0]["value"]
    vals = [r["value"] for r in rows]
    save("fao.json", {
        "current": cur, "prev_month": prev, "delta_month": round(cur-prev,1),
        "year_ago": yr_ago, "delta_year": round(cur-yr_ago,1),
        "max_ever": round(max(vals),1),
        "milestones": {"2007_crisis":127.2,"2008_peak":213.5,"2011_peak":237.9,"2022_peak":159.7,"current":cur},
        "series": rows, "score": round(100-pct(cur,vals)), "updated": NOW
    })
except Exception as e:
    errors.append(f"FAO: {e}"); print(f"  ✗ {e}")

# ── [6] ACLED — OAuth ─────────────────────────────────────
print("[6/9] ACLED — OAuth...")
acled_email = os.environ.get("ACLED_EMAIL","")
acled_password = os.environ.get("ACLED_PASSWORD","")

if acled_email and acled_password:
    try:
        print("  → Getting token...")
        result = post_form("https://acleddata.com/oauth/token", {
            "username": acled_email, "password": acled_password,
            "grant_type": "password", "client_id": "acled"
        })
        token = result.get("access_token")
        if not token:
            raise ValueError(f"No access_token. Keys returned: {list(result.keys())}")
        print("  ✓ Token OK")

        def acled_get(params):
            url = "https://acleddata.com/api/acled/read?" + urllib.parse.urlencode({"_format":"json",**params})
            return fetch_json(url, headers={"Authorization":f"Bearer {token}"})

        today = datetime.date.today()
        r30 = acled_get({
            "event_date":f"{days_ago(30)}|{iso_date(today)}","event_date_where":"BETWEEN",
            "limit":5000,"fields":"event_id_cnty|event_date|event_type|sub_event_type|disorder_type|country|region|latitude|longitude|fatalities|actor1|civilian_targeting"
        })
        events30 = r30.get("data",[])
        print(f"  ✓ {len(events30)} events (30d)")

        r90 = acled_get({"event_date":f"{days_ago(90)}|{iso_date(today)}","event_date_where":"BETWEEN","limit":5000,"fields":"event_date|event_type|disorder_type|country|fatalities"})
        r12m = acled_get({"event_date":f"{days_ago(365)}|{iso_date(today)}","event_date_where":"BETWEEN","limit":10000,"fields":"event_date|event_type|country|region|fatalities"})
        events90 = r90.get("data",[]); events12m = r12m.get("data",[])

        fat30 = sum(int(e.get("fatalities",0) or 0) for e in events30)
        civ_ev = sum(1 for e in events30 if e.get("civilian_targeting"))
        civ_fat = sum(int(e.get("fatalities",0) or 0) for e in events30 if e.get("civilian_targeting"))
        by_type = collections.Counter(e.get("event_type","?") for e in events30)
        by_dis = collections.Counter(e.get("disorder_type","?") for e in events30)
        by_cty = collections.Counter(e.get("country","?") for e in events30)
        by_reg = collections.Counter(e.get("region","?") for e in events30)

        monthly = collections.defaultdict(lambda:{"events":0,"fatalities":0})
        for e in events12m:
            m = (e.get("event_date") or "")[:7]
            if m: monthly[m]["events"]+=1; monthly[m]["fatalities"]+=int(e.get("fatalities",0) or 0)

        weekly = collections.defaultdict(lambda:{"events":0,"fatalities":0})
        for e in events90:
            if e.get("event_date"):
                try:
                    dt=datetime.datetime.strptime(e["event_date"],"%Y-%m-%d")
                    wk=dt.strftime("%Y-W%W"); weekly[wk]["events"]+=1; weekly[wk]["fatalities"]+=int(e.get("fatalities",0) or 0)
                except: pass

        geo = sorted([e for e in events30 if e.get("latitude") and e.get("longitude")],
                     key=lambda e:e.get("event_date",""),reverse=True)
        live_feed = [{"date":e.get("event_date"),"type":e.get("event_type"),"sub_type":e.get("sub_event_type"),
                      "country":e.get("country"),"lat":float(e.get("latitude",0)),"lon":float(e.get("longitude",0)),
                      "fatalities":int(e.get("fatalities",0) or 0),"actor1":(e.get("actor1","") or "")[:60],
                      "civilian":bool(e.get("civilian_targeting"))} for e in geo[:100]]

        save("conflicts.json", {
            "summary_30d":{"total_events":len(events30),"total_fatalities":fat30,
                           "civilian_events":civ_ev,"civilian_fatalities":civ_fat,
                           "countries_affected":len(by_cty)},
            "by_event_type":dict(by_type.most_common()),
            "by_disorder_type":dict(by_dis.most_common()),
            "by_region":dict(by_reg.most_common()),
            "top_countries":[{"country":c,"events":n,"fatalities":sum(int(e.get("fatalities",0) or 0) for e in events30 if e.get("country")==c)} for c,n in by_cty.most_common(10)],
            "monthly_trend_12m":[{"month":k,**v} for k,v in sorted(monthly.items())],
            "weekly_trend_90d":[{"week":k,**v} for k,v in sorted(weekly.items())],
            "live_feed":live_feed,
            "score":max(5,round(100-len(events30)*0.08-(fat30/max(len(events30),1))*2)),
            "updated":NOW
        })
        print(f"  ✓ Saved: {len(events30)} events · {fat30} fatalities · {len(live_feed)} geolocated")
    except Exception as e:
        errors.append(f"ACLED OAuth: {e}"); print(f"  ✗ {e}")
        save("conflicts.json",{"summary_30d":{"total_events":0,"total_fatalities":0},"error":str(e),"score":30,"updated":NOW})
else:
    print("  ⚠ ACLED credentials not set"); errors.append("ACLED: not configured")

# ── [7] UNHCR ─────────────────────────────────────────────
print("[7/9] UNHCR Refugees...")
try:
    d = fetch_json("https://api.unhcr.org/population/v1/population/?limit=20&dataset=population&displayType=totals&columns%5B%5D=refugees&columns%5B%5D=idps&columns%5B%5D=asylum_seekers&yearFrom=2012&yearTo=2025")
    items = d.get("items",[])
    series = [{"year":str(i["year"]),"refugees":round((i.get("refugees") or 0)/1e6,1),"idps":round((i.get("idps") or 0)/1e6,1),"asylum":round((i.get("asylum_seekers") or 0)/1e6,1),"total":round(((i.get("refugees") or 0)+(i.get("idps") or 0)+(i.get("asylum_seekers") or 0))/1e6,1)} for i in items if i.get("year")]
    # Sum all components for true total
    def row_total(row):
        return round(((row.get("refugees") or 0)+(row.get("idps") or 0)+(row.get("asylum_seekers") or 0))/1e6,1)
    totals = [{"year":str(i["year"]),"total":row_total(i)} for i in items if i.get("year")]
    cur = totals[-1]["total"] if totals else 117.3
    prev = totals[-2]["total"] if len(totals)>1 else cur
    save("refugees.json",{"current_millions":cur,"prev_year_millions":prev,"total_millions":cur,"delta":round(cur-prev,1),"breakdown":series[-1] if series else {},"series":totals,"score":max(5,round(100-cur*0.55)),"updated":NOW})
except Exception as e:
    errors.append(f"UNHCR: {e}"); print(f"  ✗ {e}")

# ── [7b] NASA FIRMS — Active fires ───────────────────────
print("[7b] Active fires — NASA FIRMS...")
firms_key = os.environ.get("NASA_FIRMS_KEY","")
if firms_key:
    try:
        # VIIRS SNPP NRT — last 1 day, global, CSV format
        url = f"https://firms.modaps.eosdis.nasa.gov/api/area/csv/{firms_key}/VIIRS_SNPP_NRT/World/2"  # last 48h for better coverage
        csv_txt = fetch_text(url)
        lines = [l for l in csv_txt.strip().splitlines() if l and not l.startswith("latitude")]
        hotspots = []
        by_region = {"Africa":0,"S.Asia":0,"SE Asia":0,"N.America":0,"S.America":0,"Siberia":0,"Europe":0,"Australia":0,"Other":0}
        if lines:
            print(f"  CSV sample (first line): {lines[0][:120]}")
            print(f"  Total lines to parse: {len(lines)}")
        for line in lines:
            parts = line.split(",")
            if len(parts) < 3: continue
            try:
                lat, lon = float(parts[0]), float(parts[1])
                brightness = float(parts[2]) if len(parts)>2 else 300
                frp = float(parts[12]) if len(parts)>12 else 0  # Fire Radiative Power (MW) — col 12 in VIIRS NRT
                hotspots.append({"lat":round(lat,3),"lon":round(lon,3),"brightness":round(brightness,1),"frp":round(frp,1)})
                # Assign to region (broader, more accurate bounds)
                if -40<lat<38 and -20<lon<55:  by_region["Africa"]+=1      # all of Africa
                elif 5<lat<40 and 55<lon<90:   by_region["S.Asia"]+=1      # India, Pakistan, Iran
                elif -10<lat<28 and 90<lon<155: by_region["SE Asia"]+=1    # SE Asia + China south
                elif 25<lat<72 and -170<lon<-50: by_region["N.America"]+=1 # N+C America
                elif -60<lat<15 and -85<lon<-30: by_region["S.America"]+=1 # S America incl Amazon
                elif 50<lat<80 and 40<lon<180:  by_region["Siberia"]+=1    # Russia/Siberia
                elif 35<lat<72 and -15<lon<45:  by_region["Europe"]+=1     # Europe
                elif -45<lat<-8 and 110<lon<160: by_region["Australia"]+=1 # Australia
                else: by_region["Other"]+=1
            except: continue
        # Keep up to 8000 points — prioritise high-intensity fires (high FRP)
        # Sort by brightness desc so the most significant fires are always included
        hotspots.sort(key=lambda h: -h.get("brightness", 0))
        total = len(hotspots)  # total BEFORE sampling
        if len(hotspots) > 8000:
            # Keep all high-intensity (top 4000) + random sample of the rest
            import random
            top = hotspots[:4000]
            rest = random.sample(hotspots[4000:], min(4000, len(hotspots)-4000))
            hotspots = top + rest
        print(f"  ✓ {total} fire spots — {len(hotspots)} kept for map (sorted by intensity)")
        save("firms.json", {
            "total": total,
            "by_region": by_region,
            "hotspots": hotspots,
            "delta_day": None,   # would need yesterday's count to compute
            "delta_year": None,
            "score": max(10, round(100 - min(total, 50000)/500)),
            "updated": NOW
        })
    except Exception as e:
        errors.append(f"FIRMS: {e}"); print(f"  ✗ {e}")
else:
    print("  ⚠ NASA_FIRMS_KEY not set — get free key at firms.modaps.eosdis.nasa.gov/api/area/")

# ── [8] OpenSky ───────────────────────────────────────────
print("[8/9] Air Traffic — OpenSky...")
try:
    # Primary: adsb.lol — no auth, server-side has no CORS issues
    states = []
    try:
        d = fetch_json("https://api.adsb.lol/v2/point/0/0/20000")
        ac = d.get("ac") or d.get("aircraft") or []
        states = [[a.get("hex",""), (a.get("flight","") or "").strip(), "",
                   None, None, a.get("lon"), a.get("lat"),
                   (a.get("alt_baro") or 0)*0.3048, False,
                   (a.get("gs") or 0)*0.514444, a.get("track") or 0,
                   None,None,None,"",False,0,None]
                  for a in ac if a.get("lat") and a.get("lon")]
        print(f"  adsb.lol: {len(states)} aircraft")
    except Exception as e:
        print(f"  adsb.lol failed: {e}, trying OpenSky...")
    if not states:
        d = fetch_json("https://opensky-network.org/api/states/all")
        states = d.get("states",[])
    by_reg = {"Europe":0,"North America":0,"Asia-Pacific":0,"Middle East":0,"Other":0}
    for s in states:
        lon=s[5]; lat=s[6]
        if lat and lon:
            if 35<lat<72 and -10<lon<40: by_reg["Europe"]+=1
            elif 15<lat<72 and -130<lon<-60: by_reg["North America"]+=1
            elif -15<lat<60 and 60<lon<150: by_reg["Asia-Pacific"]+=1
            elif 10<lat<40 and 35<lon<65: by_reg["Middle East"]+=1
            else: by_reg["Other"]+=1
    # Save with states for browser to use directly (thin down to 5000 for file size)
    import random
    states_sample = states if len(states) <= 5000 else random.sample(states, 5000)
    save("air_traffic.json",{
        "total": len(states),
        "states": states_sample,
        "by_region": by_reg,
        "score": min(90,max(20,round(len(states)/130))),
        "updated": NOW
    })
except Exception as e:
    errors.append(f"OpenSky: {e}"); print(f"  ✗ {e}")

# ── [8b] Active satellites — Celestrak ───────────────────
print("[8b] Active satellites — Celestrak...")
try:
    # Celestrak SATCAT — full satellite catalog, no key, updated daily
    satcat = fetch_text("https://celestrak.org/pub/satcat.csv")
    lines = [l for l in satcat.strip().splitlines() if l and not l.startswith("OBJECT_NAME")]
    # Count active payloads only (PAY = payload, not debris/rocket body)
    payloads = [l for l in lines if ",PAY," in l]
    total = len(payloads)
    starlink = sum(1 for l in payloads if "STARLINK" in l.upper())
    oneweb   = sum(1 for l in payloads if "ONEWEB" in l.upper())
    planet   = sum(1 for l in payloads if any(x in l.upper() for x in ["PLANET","DOVE","SKYSAT"]))
    spire    = sum(1 for l in payloads if "SPIRE" in l.upper() or "LEMUR" in l.upper())
    geo      = sum(1 for l in payloads if ",GEO," in l or ",IGSO," in l)
    print(f"  ✓ {total} active payloads · Starlink:{starlink} · OneWeb:{oneweb} · Planet:{planet}")
    save("satellites.json", {
        "total": total,
        "by_operator": {
            "Starlink (SpaceX)": starlink,
            "OneWeb": oneweb,
            "Planet Labs": planet,
            "Spire Global": spire,
            "GEO satellites": geo,
            "Other": total - starlink - oneweb - planet - spire
        },
        "source": "Celestrak SATCAT · celestrak.org/pub/satcat.csv",
        "note": "Active payloads (PAY type). Updated daily by Celestrak.",
        "updated": NOW
    })
except Exception as e:
    errors.append(f"Satellites: {e}"); print(f"  ✗ {e}")

# ── [9] Truck Tonnage + Restaurant spending + OpenTable ──────
print("[9/9] ATA Truck Tonnage + Restaurant spending...")
fred_key = os.environ.get("FRED_API_KEY","")
if fred_key:
    try:
        url = (f"https://api.stlouisfed.org/fred/series/observations"
               f"?series_id=TRUCKD11&sort_order=desc&limit=24"
               f"&file_type=json&api_key={fred_key}")
        d = fetch_json(url)
        obs = [{"date":r["date"],"value":float(r["value"])} 
               for r in d.get("observations",[]) if r.get("value",".") != "."]
        obs.reverse()
        if obs:
            cur = obs[-1]["value"]; prev = obs[-2]["value"] if len(obs)>1 else cur
            delta = round((cur-prev)/prev*100, 1)
            save("truck_tonnage.json", {
                "current": round(cur,1), "prev": round(prev,1),
                "delta_pct": delta, "series": obs[-13:],
                "updated": NOW
            })
            print(f"  ✓ Truck Tonnage: {cur:.1f} ({delta:+.1f}%)")
    except Exception as e:
        errors.append(f"Truck: {e}"); print(f"  ✗ {e}")
else:
    print("  ⚠ FRED_API_KEY not set")
# Restaurant & bar spending — FRED MRTSSM7220USN (monthly, US Census)
if fred_key:
    try:
        url = (f"https://api.stlouisfed.org/fred/series/observations"
               f"?series_id=MRTSSM7220USN&sort_order=desc&limit=25"
               f"&file_type=json&api_key={fred_key}")
        d = fetch_json(url)
        obs = [{"date":r["date"],"value":float(r["value"])}
               for r in d.get("observations",[]) if r.get("value",".") != "."]
        obs.reverse()
        if obs:
            cur = obs[-1]["value"]
            prev = obs[-2]["value"] if len(obs)>1 else cur
            yoy_val = obs[-13]["value"] if len(obs)>=13 else None
            mom = round((cur-prev)/prev*100, 1)
            yoy = round((cur-yoy_val)/yoy_val*100, 1) if yoy_val else None
            save("restaurant.json", {
                "current": round(cur,1),
                "prev": round(prev,1),
                "mom_pct": mom,
                "yoy_pct": yoy,
                "series": obs[-25:],
                "note": "FRED MRTSSM7220USN · Food Services & Drinking Places · Millions USD",
                "updated": NOW
            })
            print(f"  ✓ Restaurants: ${cur/1000:.1f}B/month · MoM {mom:+.1f}% · YoY {yoy:+.1f}%")
    except Exception as e:
        errors.append(f"Restaurant: {e}"); print(f"  ✗ {e}")

# ── CONFIG — inject secrets for client-side use ──────────
ais_key  = os.environ.get("AIS_KEY","")
cf_key   = os.environ.get("CF_RADAR_KEY","")
fred_key = os.environ.get("FRED_API_KEY","")
save("config.json", {
    "ais_key":      ais_key  if ais_key  else None,
    "eia_api_key":  os.environ.get("EIA_API_KEY","") or None,
    "cf_radar_key": cf_key   if cf_key   else None,
    "fred_api_key": fred_key if fred_key else None,
    "note": "Auto-generated by GitHub Actions. Do not commit manually.",
    "updated": NOW
})
if ais_key:
    print("  ✓ config.json — AIS_KEY injected")
else:
    print("  ⚠ config.json — AIS_KEY not set (add to GitHub Secrets)")


# ── [arXiv] Scientific papers by category ─────────────────
print("[arXiv] Fetching latest scientific papers...")
ARXIV_CATS = {
    'cs.AI':  'AI / Machine Learning',
    'q-fin':  'Quantitative Finance',
    'physics':'Physics',
    'econ':   'Economics',
    'cs.CR':  'Cybersecurity',
    'q-bio':  'Biology',
}
import xml.etree.ElementTree as ET
import urllib.request as _urllib_req
arxiv_data = {}
for cat, label in ARXIV_CATS.items():
    try:
        url = f"https://export.arxiv.org/api/query?search_query=cat:{cat}&sortBy=submittedDate&sortOrder=descending&max_results=8"
        req = _urllib_req.Request(url, headers={'User-Agent':'EarthPulse/1.0 (github-actions)'})
        with _urllib_req.urlopen(req, timeout=15) as resp:
            raw = resp.read().decode('utf-8')
        # Parse with requests-style interface
        class _R: text = raw
        r = _R()
        root = ET.fromstring(r.text)
        ns = {'atom':'http://www.w3.org/2005/Atom'}
        papers = []
        for entry in root.findall('atom:entry', ns):
            title = entry.findtext('atom:title', '', ns).replace('\n',' ').strip()
            summary = entry.findtext('atom:summary', '', ns).replace('\n',' ').strip()[:200]
            link = entry.findtext('atom:id', '', ns).strip()
            published = entry.findtext('atom:published', '', ns)[:10]
            authors = [a.findtext('atom:name','',ns) for a in entry.findall('atom:author',ns)][:3]
            papers.append({
                'title': title, 'summary': summary, 'link': link,
                'date': published, 'authors': authors
            })
        arxiv_data[cat] = {'label': label, 'papers': papers, 'updated': NOW}
        print(f"  ✓ {cat}: {len(papers)} papers")
    except Exception as e:
        print(f"  ✗ {cat}: {e}")
        arxiv_data[cat] = {'label': label, 'papers': [], 'error': str(e)}

save("arxiv.json", arxiv_data)


# ── [DEMO] Demographics: Population, Life Expectancy, Aging Index ──
print("[DEMO] Fetching UN/WHO/WorldBank demographics...")

# ── Population — UN World Population Prospects API ──
try:
    # UN DESA WPP API: total population + births + deaths per year
    url_pop = "https://population.un.org/dataportalapi/api/v1/data/indicators/49,55,60/locations/900/start/2020/end/2026/?format=json&pageSize=200"
    d_pop = fetch_json(url_pop)
    items_pop = d_pop.get("data", [])
    # indicator 49=total population, 55=births, 60=deaths
    pop_by_year = {}
    for row in items_pop:
        yr = row.get("timeLabel") or row.get("time", "")
        ind = row.get("indicatorId")
        val = row.get("value")
        if yr and ind and val is not None:
            if yr not in pop_by_year:
                pop_by_year[yr] = {}
            pop_by_year[yr][ind] = float(val)
    # Latest year
    latest_yr = max(pop_by_year.keys()) if pop_by_year else "2024"
    latest = pop_by_year.get(latest_yr, {})
    # Pop in thousands → convert to units
    total_pop = round(latest.get(49, 8118000) * 1000)
    births_per_year = round(latest.get(55, 140000000))
    deaths_per_year = round(latest.get(60, 58000000))
    births_per_sec = round(births_per_year / 31536000, 3)
    deaths_per_sec = round(deaths_per_year / 31536000, 3)
    # Reference timestamp: Jan 1 of latest year
    import datetime
    ref_ts = int(datetime.datetime(int(latest_yr), 1, 1).timestamp())
    save("population.json", {
        "total": total_pop,
        "year": latest_yr,
        "births_per_sec": births_per_sec,
        "deaths_per_sec": deaths_per_sec,
        "ref_timestamp": ref_ts,
        "ref_population": total_pop,
        "source": "UN DESA World Population Prospects",
        "updated": NOW
    })
    print(f"  ✓ Population {latest_yr}: {total_pop/1e9:.3f}B · {births_per_sec} births/s · {deaths_per_sec} deaths/s")
except Exception as e:
    errors.append(f"Population: {e}"); print(f"  ✗ Population: {e}")

# ── Life expectancy — World Bank API (free, CORS ok) ──
try:
    # SP.DYN.LE00.IN = life expectancy at birth, total
    countries_le = ["JPN","CHE","SGP","AUS","FRA","DEU","CHN","USA","BRA","IND","NGA","COD"]
    flags = {"JPN":"🇯🇵","CHE":"🇨🇭","SGP":"🇸🇬","AUS":"🇦🇺","FRA":"🇫🇷",
             "DEU":"🇩🇪","CHN":"🇨🇳","USA":"🇺🇸","BRA":"🇧🇷","IND":"🇮🇳","NGA":"🇳🇬","COD":"🇨🇩"}
    names = {"JPN":"Japan","CHE":"Switzerland","SGP":"Singapore","AUS":"Australia","FRA":"France",
             "DEU":"Germany","CHN":"China","USA":"USA","BRA":"Brazil","IND":"India","NGA":"Nigeria","COD":"DRC"}
    le_data = []
    codes = ";".join(countries_le)
    url_le = f"https://api.worldbank.org/v2/country/{codes}/indicator/SP.DYN.LE00.IN?format=json&mrv=12&per_page=300"
    d_le = fetch_json(url_le)
    rows = d_le[1] if isinstance(d_le, list) and len(d_le)>1 else []
    # Group by country, get latest non-null
    latest_le = {}
    prev_le = {}
    for row in rows:
        code = row.get("countryiso3code","")
        yr = int(row.get("date",0))
        val = row.get("value")
        if code in countries_le and val is not None:
            if code not in latest_le or yr > latest_le[code]["year"]:
                if code in latest_le:
                    prev_le[code] = latest_le[code]
                latest_le[code] = {"year": yr, "value": round(float(val),1)}
    # Compute 1Y delta and build series for 10Y delta
    le_series = {}
    for row in rows:
        code = row.get("countryiso3code","")
        if code not in countries_le: continue
        yr = int(row.get("date",0))
        val = row.get("value")
        if val is not None:
            if code not in le_series: le_series[code] = {}
            le_series[code][yr] = round(float(val),1)
    result_le = []
    for code in countries_le:
        if code not in latest_le: continue
        cur_yr = latest_le[code]["year"]
        cur_val = latest_le[code]["value"]
        val_1y = le_series.get(code,{}).get(cur_yr-1)
        val_10y = le_series.get(code,{}).get(cur_yr-10)
        d1 = round(cur_val - val_1y, 1) if val_1y else None
        d10 = round(cur_val - val_10y, 1) if val_10y else None
        result_le.append({
            "code": code, "flag": flags[code], "country": names[code],
            "value": cur_val, "year": cur_yr, "d1y": d1, "d10y": d10
        })
    result_le.sort(key=lambda x: -x["value"])
    save("life_expectancy.json", {"countries": result_le, "updated": NOW})
    print(f"  ✓ Life expectancy: {len(result_le)} countries")
except Exception as e:
    errors.append(f"LifeExp: {e}"); print(f"  ✗ LifeExp: {e}")

# ── Aging index — World Bank SP.POP.65UP.TO.ZS + SP.POP.1564.TO.ZS ──
try:
    countries_ag = ["JPN","ITA","PRT","FIN","GRC","DEU","KOR","ESP","FRA","CHN","USA","IND"]
    names_ag = {"JPN":"Japan","ITA":"Italy","PRT":"Portugal","FIN":"Finland","GRC":"Greece",
                "DEU":"Germany","KOR":"S. Korea","ESP":"Spain","FRA":"France",
                "CHN":"China","USA":"USA","IND":"India"}
    flags_ag = {"JPN":"🇯🇵","ITA":"🇮🇹","PRT":"🇵🇹","FIN":"🇫🇮","GRC":"🇬🇷",
                "DEU":"🇩🇪","KOR":"🇰🇷","ESP":"🇪🇸","FRA":"🇫🇷","CHN":"🇨🇳","USA":"🇺🇸","IND":"🇮🇳"}
    codes_ag = ";".join(countries_ag)
    # 65+ share and working-age share
    url65 = f"https://api.worldbank.org/v2/country/{codes_ag}/indicator/SP.POP.65UP.TO.ZS?format=json&mrv=5&per_page=200"
    url1564 = f"https://api.worldbank.org/v2/country/{codes_ag}/indicator/SP.POP.1564.TO.ZS?format=json&mrv=5&per_page=200"
    d65 = fetch_json(url65); d1564 = fetch_json(url1564)
    rows65 = d65[1] if isinstance(d65,list) and len(d65)>1 else []
    rows1564 = d1564[1] if isinstance(d1564,list) and len(d1564)>1 else []
    def latest_val(rows, code):
        best = None
        for r in rows:
            if r.get("countryiso3code","") == code and r.get("value") is not None:
                if best is None or int(r.get("date",0)) > int(best.get("date",0)):
                    best = r
        return round(float(best["value"]),1) if best else None
    aging_result = []
    for code in countries_ag:
        p65 = latest_val(rows65, code)
        p1564 = latest_val(rows1564, code)
        if p65 is None or p1564 is None or p1564 == 0: continue
        ratio = round(p65 / p1564 * 100, 1)
        aging_result.append({
            "code": code, "flag": flags_ag[code], "country": names_ag[code],
            "ratio": ratio, "pct65": p65, "pct1564": p1564
        })
    aging_result.sort(key=lambda x: -x["ratio"])
    save("aging_index.json", {"countries": aging_result, "updated": NOW})
    print(f"  ✓ Aging index: {len(aging_result)} countries")
except Exception as e:
    errors.append(f"Aging: {e}"); print(f"  ✗ Aging: {e}")


# ── [ELECTRICITY] EU / US / China ─────────────────────────
print("[ELEC] Fetching electricity consumption...")
elec = {}

# ── US — EIA v2 API (hourly demand, contiguous US) ──
eia_key = os.environ.get("EIA_API_KEY","")
if eia_key:
    try:
        url = (f"https://api.eia.gov/v2/electricity/rto/region-data/data/"
               f"?api_key={eia_key}&frequency=hourly"
               f"&facets[respondent][]=US48&facets[type][]=D"
               f"&data[0]=value&sort[0][column]=period&sort[0][direction]=desc&length=48")
        d = fetch_json(url)
        rows = d.get("response",{}).get("data",[])
        valid = [r for r in rows if r.get("value") is not None]
        if valid:
            cur = float(valid[0]["value"])  # MW
            prev24 = float(valid[min(23,len(valid)-1)]["value"]) if len(valid) > 1 else cur
            delta = round((cur - prev24) / prev24 * 100, 1) if prev24 else None
            # 24h series
            series = [{"time": r["period"], "value": round(float(r["value"])/1000, 1)}
                      for r in reversed(valid[:48]) if r.get("value") is not None]
            elec["us"] = {
                "gw": round(cur/1000, 1),
                "delta_pct": delta,
                "series": series,
                "updated": valid[0]["period"]
            }
            print(f"  ✓ US: {cur/1000:.0f} GW ({delta:+.1f}% vs 24h ago)")
    except Exception as e:
        print(f"  ✗ US: {e}")

# ── EU — ENTSO-E Transparency Platform ──
# Free registration at transparency.entsoe.eu → get security token
entsoe_key = os.environ.get("ENTSOE_KEY","")
if entsoe_key:
    try:
        import xml.etree.ElementTree as ET
        from datetime import datetime, timezone, timedelta
        now = datetime.now(timezone.utc)
        start = (now - timedelta(hours=26)).strftime("%Y%m%d%H00")
        end = now.strftime("%Y%m%d%H00")
        url = (f"https://web-api.tp.entsoe.eu/api"
               f"?documentType=A65&processType=A16"
               f"&outBiddingZone_Domain=10Y1001A1001A83F"  # EU
               f"&periodStart={start}&periodEnd={end}"
               f"&securityToken={entsoe_key}")
        r = requests.get(url, timeout=15)
        root = ET.fromstring(r.text)
        ns = {"ns":"urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0"}
        quantities = [float(pt.findtext("ns:quantity","0",ns))
                      for ts in root.findall(".//ns:TimeSeries",ns)
                      for pt in ts.findall(".//ns:Point",ns)]
        if quantities:
            cur_eu = quantities[-1]
            prev_eu = quantities[-25] if len(quantities) > 24 else quantities[0]
            delta_eu = round((cur_eu-prev_eu)/prev_eu*100, 1) if prev_eu else None
            elec["eu"] = {"gw": round(cur_eu/1000, 1), "delta_pct": delta_eu, "updated": end}
            print(f"  ✓ EU: {cur_eu/1000:.0f} GW")
    except Exception as e:
        print(f"  ✗ EU ENTSO-E: {e}")

# ── EU fallback — sum from eco2mix FR × scaling ──
if "eu" not in elec:
    try:
        url = ("https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets/"
               "eco2mix-national-tr/records"
               "?select=date_heure,consommation&where=consommation%20is%20not%20null"
               "&order_by=date_heure%20desc&limit=48")
        d = fetch_json(url)
        rows = [r for r in d.get("results",[]) if r.get("consommation")]
        if rows:
            fr_gw = rows[0]["consommation"] / 1000
            eu_gw = round(fr_gw * 14, 0)  # FR ≈ 7% of EU → ×14
            series = [{"time": r["date_heure"][:16], "value": round(r["consommation"]/1000*14, 0)}
                      for r in reversed(rows)]
            elec["eu"] = {"gw": eu_gw, "delta_pct": None, "series": series,
                          "note": "Estimated from France×14", "updated": rows[0]["date_heure"]}
            print(f"  ✓ EU (est.): {eu_gw:.0f} GW")
    except Exception as e:
        print(f"  ✗ EU fallback: {e}")

# ── China — monthly via BP Statistical Review / Ember (best available) ──
# China publishes monthly via NBS but no free API. Use Ember public dataset.
try:
    # Ember global electricity monthly: public CSV
    url = "https://ember-energy.org/app/uploads/Data-for-Ember-Global-Electricity-Review-2024.csv"
    r = requests.get(url, timeout=15, headers={"User-Agent":"EarthPulse/1.0"})
    if r.status_code == 200 and "China" in r.text:
        lines = r.text.split("\n")
        china_rows = [l for l in lines if "China" in l and "Demand" in l]
        if china_rows:
            last = china_rows[-1].split(",")
            elec["cn"] = {"gw": None, "twh_year": float(last[3]) if len(last)>3 else None,
                          "note": "Annual TWh", "updated": last[1] if len(last)>1 else ""}
            print(f"  ✓ China: {elec['cn']}")
except Exception as e:
    print(f"  ✗ China: {e}")

# Always save something
if not elec.get("cn"):
    # China 2023: ~9400 TWh/year → avg ~1073 GW
    elec["cn"] = {"gw": 1073, "twh_year": 9400, "note": "IEA 2023 annual avg", "updated": "2023"}

save("electricity.json", {"regions": elec, "updated": NOW})
print(f"  Saved electricity.json: {list(elec.keys())}")

# ── META ──────────────────────────────────────────────────
# ── [INTERNET] Cloudflare Radar ──────────────────────────
print("[INTERNET] Cloudflare Radar data...")
cf_key = os.environ.get("CF_RADAR_KEY", "")
if cf_key:
    try:
        import datetime as _dt
        headers_cf = {"Authorization": f"Bearer {cf_key}", "Content-Type": "application/json"}

        # 1. Outages & anomalies détectés par Cloudflare
        try:
            outages = fetch_json(
                "https://api.cloudflare.com/client/v4/radar/annotations/outages?dateRange=7d&format=json",
                headers=headers_cf
            )
            annotations = outages.get("result", {}).get("annotations", [])
        except Exception as e:
            annotations = []
            print(f"  ⚠ CF outages: {e}")

        # 2. Résumé trafic HTTP — IPv4 vs IPv6 (7 jours)
        try:
            ipv_data = fetch_json(
                "https://api.cloudflare.com/client/v4/radar/http/summary/ip_version?dateRange=7d&format=json",
                headers=headers_cf
            )
            ipv_summary = ipv_data.get("result", {}).get("summary_0", {})
        except Exception as e:
            ipv_summary = {}
            print(f"  ⚠ CF IPv6: {e}")

        # 3. Timeseries trafic global 7j
        try:
            ts_data = fetch_json(
                "https://api.cloudflare.com/client/v4/radar/http/timeseries?dateRange=7d&format=json",
                headers=headers_cf
            )
            ts_serie = ts_data.get("result", {}).get("serie_0", {})
        except Exception as e:
            ts_serie = {}
            print(f"  ⚠ CF timeseries: {e}")

        # 4. Attaques DDoS L3/L4
        try:
            ddos_data = fetch_json(
                "https://api.cloudflare.com/client/v4/radar/attacks/layer3/summary?dateRange=7d&format=json",
                headers=headers_cf
            )
            ddos_summary = ddos_data.get("result", {}).get("summary_0", {})
        except Exception as e:
            ddos_summary = {}
            print(f"  ⚠ CF DDoS: {e}")

        save("cloudflare_internet.json", {
            "annotations":  annotations[:20],
            "ipv6_pct":     float(ipv_summary.get("IPv6", 0)) if ipv_summary else None,
            "ipv4_pct":     float(ipv_summary.get("IPv4", 0)) if ipv_summary else None,
            "traffic_ts":   {
                "timestamps": ts_serie.get("timestamps", []),
                "values":     ts_serie.get("requests", ts_serie.get("values", []))
            },
            "ddos_vectors": ddos_summary,
            "updated": NOW
        })
        print(f"  ✓ cloudflare_internet.json — {len(annotations)} outages, IPv6={ipv_summary.get('IPv6','?')}%")
    except Exception as e:
        errors.append(f"CF Radar: {e}"); print(f"  ✗ CF Radar: {e}")
else:
    print("  ⚠ CF_RADAR_KEY not set — add to GitHub Secrets")


save("meta.json",{"last_run":NOW,"errors":errors,"datasets":["co2","bdi","gas_storage","oil_stocks","fao","conflicts","refugees","air_traffic","opentable"],"acled_ok":bool(acled_email and acled_password),"eia_ok":bool(os.environ.get("EIA_API_KEY")),"agsi_ok":bool(os.environ.get("AGSI_KEY"))})

print(f"\n{'═'*42}")
print(f"  Done — {len(errors)} error(s)")
for e in errors: print(f"  ✗ {e}")
