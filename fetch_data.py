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
        "https://agsi.gie.eu/api?type=EU&size=60",
        "https://agsi.gie.eu/api/data/eu?size=60",
        "https://agsi.gie.eu/api/v1/data/eu?size=60",
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
    for entry in reversed(eu_data[:60]):
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
    d = fetch_json("https://api.unhcr.org/population/v1/population/?limit=1&dataset=population&displayType=totals&columns%5B%5D=refugees&columns%5B%5D=idps&columns%5B%5D=asylum_seekers&yearFrom=2012&yearTo=2025")
    items = d.get("items",[])
    series = [{"year":str(i["year"]),"refugees":round((i.get("refugees") or 0)/1e6,1),"idps":round((i.get("idps") or 0)/1e6,1),"asylum":round((i.get("asylum_seekers") or 0)/1e6,1),"total":round(((i.get("refugees") or 0)+(i.get("idps") or 0)+(i.get("asylum_seekers") or 0))/1e6,1)} for i in items if i.get("year")]
    cur = series[-1]["total"] if series else 117.3
    prev = series[-2]["total"] if len(series)>1 else cur
    save("refugees.json",{"current_millions":cur,"prev_year_millions":prev,"delta":round(cur-prev,1),"breakdown":series[-1] if series else {},"series":series,"score":max(5,round(100-cur*0.55)),"updated":NOW})
except Exception as e:
    errors.append(f"UNHCR: {e}"); print(f"  ✗ {e}")

# ── [7b] NASA FIRMS — Active fires ───────────────────────
print("[7b] Active fires — NASA FIRMS...")
firms_key = os.environ.get("NASA_FIRMS_KEY","")
if firms_key:
    try:
        # VIIRS SNPP NRT — last 1 day, global, CSV format
        url = f"https://firms.modaps.eosdis.nasa.gov/api/area/csv/{firms_key}/VIIRS_SNPP_NRT/World/1"
        csv_txt = fetch_text(url)
        lines = [l for l in csv_txt.strip().splitlines() if l and not l.startswith("latitude")]
        hotspots = []
        by_region = {"Amazon":0,"C.Africa":0,"S.Africa":0,"Siberia":0,"N.America":0,"SE Asia":0,"Australia":0,"Other":0}
        for line in lines:
            parts = line.split(",")
            if len(parts) < 3: continue
            try:
                lat, lon = float(parts[0]), float(parts[1])
                brightness = float(parts[2]) if len(parts)>2 else 300
                hotspots.append({"lat":round(lat,3),"lon":round(lon,3),"brightness":round(brightness,1)})
                # Assign to region
                if -20<lat<15 and -80<lon<-35: by_region["Amazon"]+=1
                elif -10<lat<15 and 10<lon<40: by_region["C.Africa"]+=1
                elif -35<lat<-10 and 10<lon<40: by_region["S.Africa"]+=1
                elif 50<lat<75 and 60<lon<140: by_region["Siberia"]+=1
                elif 25<lat<72 and -130<lon<-60: by_region["N.America"]+=1
                elif -10<lat<25 and 90<lon<140: by_region["SE Asia"]+=1
                elif -40<lat<-10 and 110<lon<155: by_region["Australia"]+=1
                else: by_region["Other"]+=1
            except: continue
        # Thin hotspots for file size — keep max 2000 points
        import random
        if len(hotspots) > 2000:
            hotspots = random.sample(hotspots, 2000)
        total = len(lines)
        print(f"  ✓ {total} fire spots — {len(hotspots)} sampled for map")
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
    save("air_traffic.json",{"total":len(states),"by_region":by_reg,"score":min(90,max(20,round(len(states)/130))),"updated":NOW})
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

# ── [9] OpenTable (static) ────────────────────────────────
print("[9/9] Restaurant bookings...")
save("opentable.json",{"cities":[{"city":"New York","vs_2019":104},{"city":"London","vs_2019":98},{"city":"Paris","vs_2019":96},{"city":"Toronto","vs_2019":101},{"city":"Sydney","vs_2019":107},{"city":"Berlin","vs_2019":103},{"city":"Tokyo","vs_2019":89},{"city":"Mexico City","vs_2019":112}],"note":"% vs 2019 baseline","score":72,"updated":NOW})

# ── CONFIG — inject secrets for client-side use ──────────
ais_key = os.environ.get("AIS_KEY","")
cf_key  = os.environ.get("CF_RADAR_KEY","")
save("config.json", {
    "ais_key": ais_key if ais_key else None,
    "cf_radar_key": cf_key if cf_key else None,
    "note": "Auto-generated by GitHub Actions. Do not commit manually.",
    "updated": NOW
})
if ais_key:
    print("  ✓ config.json — AIS_KEY injected")
else:
    print("  ⚠ config.json — AIS_KEY not set (add to GitHub Secrets)")

# ── META ──────────────────────────────────────────────────
save("meta.json",{"last_run":NOW,"errors":errors,"datasets":["co2","bdi","gas_storage","oil_stocks","fao","conflicts","refugees","air_traffic","opentable"],"acled_ok":bool(acled_email and acled_password),"eia_ok":bool(os.environ.get("EIA_API_KEY")),"agsi_ok":bool(os.environ.get("AGSI_KEY"))})

print(f"\n{'═'*42}")
print(f"  Done — {len(errors)} error(s)")
for e in errors: print(f"  ✗ {e}")
