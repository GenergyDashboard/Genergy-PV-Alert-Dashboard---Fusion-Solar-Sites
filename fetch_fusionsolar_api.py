"""
fetch_fusionsolar_api.py

Replaces the Playwright scraper with FusionSolar's Northbound API.
Logs in once, then pulls hourly KPIs for ALL sites in batched calls.

Speed: ~3 minutes total (vs 13+ min with browser scraping)
Dependencies: just `requests` (no Playwright, no browser)

Output:
  data/raw/{slug}.json - per-site JSON with hourly_kpi data

Environment variables (GitHub Secrets):
  FUSIONSOLAR_API_USER - Northbound API username
                         (NOT your portal login — created under
                          System → Company Management → Northbound Management)
  FUSIONSOLAR_API_PASS - Northbound API password (sent as "systemCode")

To discover station codes:
  Set DISCOVER=1 as env var, run once, check output for station_codes.
"""

import json
import os
import socket
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests


# =============================================================================
# DNS FIX (GitHub runners can't resolve intl.fusionsolar.huawei.com)
# =============================================================================

FUSIONSOLAR_HOST = "intl.fusionsolar.huawei.com"
FALLBACK_IP = "119.8.160.213"

def fix_dns():
    """Ensure FUSIONSOLAR_HOST resolves; patch /etc/hosts if standard DNS fails.
    Ported from the working GenergyDashboard-API fetch.py."""
    import subprocess
    print(f"🔍 Checking DNS for {FUSIONSOLAR_HOST}...")
    try:
        ip = socket.gethostbyname(FUSIONSOLAR_HOST)
        print(f"  ✅ DNS OK: {FUSIONSOLAR_HOST} → {ip}")
        return
    except socket.gaierror:
        print(f"  ⚠️  DNS failed, trying Google DNS (8.8.8.8) fallback...")

    # Try dig against Google DNS
    resolved_ip = None
    try:
        result = subprocess.run(
            ["dig", "+short", FUSIONSOLAR_HOST, "@8.8.8.8"],
            capture_output=True, text=True, timeout=10,
        )
        ips = [l.strip() for l in result.stdout.strip().split("\n")
               if l.strip() and not l.strip().endswith(".")]
        if ips:
            resolved_ip = ips[0]
            print(f"  Resolved via Google DNS: {resolved_ip}")
    except Exception as e:
        print(f"  dig lookup failed: {e}")

    if not resolved_ip:
        resolved_ip = FALLBACK_IP
        print(f"  Using fallback IP: {resolved_ip}")

    # Write to /etc/hosts via sudo (ubuntu-latest has passwordless sudo)
    hosts_entry = f"{resolved_ip} {FUSIONSOLAR_HOST}\n"
    try:
        # Check if already patched
        with open("/etc/hosts", "r") as f:
            if FUSIONSOLAR_HOST in f.read():
                print(f"  /etc/hosts already has an entry")
                return
        # Use sudo tee (works on GitHub Actions ubuntu-latest)
        result = subprocess.run(
            ["sudo", "tee", "-a", "/etc/hosts"],
            input=hosts_entry, capture_output=True, text=True, timeout=5,
        )
        if result.returncode != 0:
            raise RuntimeError(f"sudo tee failed: {result.stderr}")
        print(f"  ✅ Added to /etc/hosts: {resolved_ip} {FUSIONSOLAR_HOST}")
    except Exception as e:
        # Last resort: try direct write
        try:
            with open("/etc/hosts", "a") as f:
                f.write(hosts_entry)
            print(f"  ✅ Added to /etc/hosts (direct write)")
        except Exception as e2:
            print(f"  ❌ Cannot patch /etc/hosts: {e2}")
            sys.exit(1)

    # Verify
    try:
        ip = socket.gethostbyname(FUSIONSOLAR_HOST)
        print(f"  ✅ DNS now resolves: {FUSIONSOLAR_HOST} → {ip}")
    except socket.gaierror:
        print(f"  ❌ DNS still failing after /etc/hosts patch")
        sys.exit(1)

# =============================================================================
# CONFIG
# =============================================================================

API_BASE = "https://intl.fusionsolar.huawei.com/thirdData"

# Sleep between API calls (Huawei rate limit: ~1 req/min)
INTER_CALL_SLEEP = 61

SAST = timezone(timedelta(hours=2))
RAW_DIR = Path(__file__).parent / "data" / "raw"

# All 17 FusionSolar sites
# station_code: run with DISCOVER=1 to find these via getStationList
SITES = [
    {"search_name": "Addo Spar Smart Logger",         "slug": "addo-spar",              "station_code": "NE=51009860"},
    {"search_name": "Bel Essex(Valeo)",               "slug": "bel-essex-valeo",        "station_code": "NE=56006848"},
    {"search_name": "BIMBO NRG",                      "slug": "bimbo-nrg",              "station_code": "NE=81028504"},
    {"search_name": "BMI park",                       "slug": "bmi-park",               "station_code": "NE=63713198"},
    {"search_name": "BMI Paterson",                   "slug": "bmi-paterson",           "station_code": "NE=53238019"},
    {"search_name": "Coega Dairy",                    "slug": "coega-dairy",            "station_code": "NE=51003907"},
    {"search_name": "GM-Hasty Tasty",                 "slug": "gm-hasty-tasty",         "station_code": "NE=85483210"},
    {"search_name": "Keypak Part 2",                  "slug": "keypak-part-2",          "station_code": "NE=51089078"},
    {"search_name": "Kirkwood FNB",                   "slug": "kirkwood-fnb",           "station_code": "NE=52476257"},
    {"search_name": "kirkwood spar power meter",      "slug": "kirkwood-spar",          "station_code": "NE=64153308"},
    {"search_name": "MP The Pines",                   "slug": "mp-the-pines",           "station_code": "NE=50777603"},
    {"search_name": "Mountain View shopping centre",  "slug": "mountain-view-sc",       "station_code": "NE=51613648"},
    {"search_name": "Nautica Shopping Centre",        "slug": "nautica-sc",             "station_code": "NE=51284622"},
    {"search_name": "RDM Somerset West 5MW",          "slug": "rdm-somerset-west",      "station_code": "NE=51657840"},
    {"search_name": "Shoprite Parklands",             "slug": "shoprite-parklands",      "station_code": "NE=51316594"},
    {"search_name": "WG Group - Mdantsane SuperSpar", "slug": "wg-mdantsane-superspar", "station_code": "NE=54891660"},
    {"search_name": "WG Group - Nurture Health",      "slug": "wg-nurture-health",      "station_code": "NE=54875772"},
]


# =============================================================================
# API SESSION
# =============================================================================

class FusionSolarAPI:
    def __init__(self, username, password):
        self.session = requests.Session()
        self.username = username
        self.password = password
        self.logged_in = False

    def login(self):
        """Login and get XSRF-TOKEN cookie."""
        print("🔐 Logging in to FusionSolar Northbound API...")
        resp = self.session.post(
            f"{API_BASE}/login",
            json={"userName": self.username, "systemCode": self.password},
            timeout=30,
        )
        if resp.status_code == 200:
            # Token is set as a cookie
            token = self.session.cookies.get("XSRF-TOKEN")
            if token:
                self.session.headers.update({"XSRF-TOKEN": token})
                self.logged_in = True
                print(f"  ✅ Login successful")
                return True
        # Check for error in response
        try:
            data = resp.json()
            if data.get("failCode") == 305:
                print("  ❌ Login failed: Invalid credentials")
            elif data.get("failCode") == 407:
                print("  ❌ Login failed: Too many attempts, try again in 5 min")
            else:
                print(f"  ❌ Login failed: {data}")
        except Exception:
            print(f"  ❌ Login failed: HTTP {resp.status_code}")
        return False

    def logout(self):
        """Logout to free the session (only 1 session per account)."""
        try:
            self.session.post(f"{API_BASE}/logout", timeout=10)
            print("🔒 Logged out")
        except Exception:
            pass

    def api_call(self, endpoint, body):
        """Make an API call with rate limiting."""
        resp = self.session.post(
            f"{API_BASE}/{endpoint}",
            json=body,
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        if not data.get("success", True) and data.get("failCode"):
            raise RuntimeError(f"API error: {data}")
        return data

    def discover_stations(self):
        """List all stations visible to this account."""
        print("\n🔍 Discovering stations...")
        data = self.api_call("getStationList", {"pageNo": 1, "pageSize": 100})
        stations = data.get("data", {}).get("list", [])
        print(f"\nFound {len(stations)} station(s):")
        print(f"  {'NAME':<45} {'STATION CODE':<25} {'CAPACITY'}")
        print(f"  {'-'*45} {'-'*25} {'-'*10}")
        for s in stations:
            print(f"  {s.get('stationName','?'):<45} {s.get('stationCode','?'):<25} {s.get('capacity',0)} kWp")

        # Save for reference
        with open("fusionsolar_discovered.json", "w") as f:
            json.dump(stations, f, indent=2)
        print(f"\nSaved to fusionsolar_discovered.json")
        print(f"\nCopy station_code values into SITES list in this script.")
        return stations

    def get_hourly_data(self, station_codes, collect_time):
        """
        Get hourly KPI data for stations on a given date.
        collect_time: epoch milliseconds for the start of the day.
        Can batch multiple station codes.
        """
        body = {
            "stationCodes": ",".join(station_codes),
            "collectTime": collect_time,
        }
        return self.api_call("getKpiStationHour", body)

    def get_realtime_data(self, station_codes):
        """Get real-time KPIs for stations."""
        body = {"stationCodes": ",".join(station_codes)}
        return self.api_call("getStationRealKpi", body)


# =============================================================================
# MAIN
# =============================================================================

def main():
    username = os.environ.get("FUSIONSOLAR_API_USER", "")
    password = os.environ.get("FUSIONSOLAR_API_PASS", "")

    if not username or not password:
        print("❌ FUSIONSOLAR_API_USER and FUSIONSOLAR_API_PASS must be set")
        sys.exit(1)

    print(f"🚀 FusionSolar API Fetcher")
    print(f"🔐 Username: {username[:4]}***")
    print(f"🏢 Sites configured: {len(SITES)}")

    fix_dns()

    api = FusionSolarAPI(username, password)

    try:
        if not api.login():
            sys.exit(1)

        # Discovery mode
        if os.environ.get("DISCOVER") == "1":
            api.discover_stations()
            return

        # Check all sites have station codes
        missing = [s for s in SITES if not s["station_code"]]
        if missing:
            print(f"\n⚠️  {len(missing)} sites missing station_code!")
            print("   Run with DISCOVER=1 to find station codes.")
            print("   Missing:", [s["slug"] for s in missing])
            sys.exit(1)

        now = datetime.now(SAST)
        today_str = now.strftime("%Y-%m-%d")
        # Epoch ms for start of today (SAST midnight → UTC)
        today_midnight = datetime.strptime(today_str, "%Y-%m-%d").replace(
            tzinfo=SAST
        )
        collect_time = int(today_midnight.timestamp() * 1000)

        all_codes = [s["station_code"] for s in SITES]
        code_to_site = {s["station_code"]: s for s in SITES}

        # ── Step 1: Real-time KPIs (batched, 1 call) ──────────────
        print(f"\n📊 Fetching real-time KPIs for {len(all_codes)} stations...")
        realtime = api.get_realtime_data(all_codes)
        realtime_map = {}
        for item in realtime.get("data", []):
            code = item.get("stationCode")
            if code:
                realtime_map[code] = item.get("dataItemMap", {})

        print(f"  ✅ Got real-time data for {len(realtime_map)} stations")
        time.sleep(INTER_CALL_SLEEP)

        # ── Step 2: Hourly KPIs (batched, 1 call) ─────────────────
        print(f"\n📈 Fetching hourly data for {today_str}...")
        hourly = api.get_hourly_data(all_codes, collect_time)
        hourly_map = {}  # station_code → list of hourly dicts
        for item in hourly.get("data", []):
            code = item.get("stationCode")
            if code not in hourly_map:
                hourly_map[code] = []
            hourly_map[code].append(item)

        print(f"  ✅ Got hourly data for {len(hourly_map)} stations")

        # ── Step 3: Write per-site JSON ────────────────────────────
        RAW_DIR.mkdir(parents=True, exist_ok=True)
        saved = 0
        debug_shown = False

        for code, site in code_to_site.items():
            slug = site["slug"]
            search_name = site["search_name"]
            rt = realtime_map.get(code, {})
            hr = hourly_map.get(code, [])

            # Show dataItemMap fields once for debugging
            if not debug_shown and hr:
                sample_dim = hr[0].get("dataItemMap", {})
                print(f"\n  📋 dataItemMap fields available: {list(sample_dim.keys())}")
                debug_shown = True

            # Build hourly array (0-23)
            hourly_pv = [0.0] * 24
            for entry in hr:
                dim = entry.get("dataItemMap", {})
                ct = entry.get("collectTime", 0)
                if ct:
                    hour = datetime.fromtimestamp(ct / 1000, tz=SAST).hour
                    # ongrid_power = energy fed to grid (kWh) — primary field
                    # inverter_power = inverter output (kWh) — fallback
                    # power_profit is REVENUE (money) — do NOT use
                    ongrid_power = float(dim.get("ongrid_power", 0) or 0)
                    inv_power = float(dim.get("inverter_power", 0) or 0)
                    pv_val = ongrid_power or inv_power
                    hourly_pv[hour] += round(pv_val, 2)  # += to sum multi-inverter

            hourly_pv = [round(v, 2) for v in hourly_pv]
            total_kwh = float(rt.get("day_power", 0) or 0) or sum(hourly_pv)
            total_kwh = round(float(total_kwh), 2)

            output = {
                "search_name": search_name,
                "slug": slug,
                "station_code": code,
                "fetched_at_utc": datetime.utcnow().isoformat(),
                "date": today_str,
                "plant_name": search_name,
                "total_kwh": total_kwh,
                "day_power": rt.get("day_power", 0),
                "month_power": rt.get("month_power", 0),
                "total_power": rt.get("total_power", 0),
                "health_state": rt.get("real_health_state", 0),
                "hourly_pv": hourly_pv,
                "last_hour": max((h for h in range(24) if hourly_pv[h] > 0), default=0),
            }

            out_file = RAW_DIR / f"{slug}.json"
            with open(out_file, "w") as f:
                json.dump(output, f, indent=2)
            print(f"  ✅ {slug}: {total_kwh} kWh ({sum(1 for v in hourly_pv if v > 0)} hours)")
            saved += 1

        print(f"\n{'='*50}")
        print(f"✅ Saved: {saved}/{len(SITES)} sites")
        print(f"{'='*50}")

    finally:
        api.logout()


if __name__ == "__main__":
    main()
