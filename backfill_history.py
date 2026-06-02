"""
backfill_history.py

Re-fetches hourly KPI data for past days and patches history.json.
Also prints raw dataItemMap fields for debugging.

Usage:
    python backfill_history.py                     # backfill yesterday
    python backfill_history.py --days 3            # backfill last 3 days
    python backfill_history.py --date 2026-05-31   # specific date
    python backfill_history.py --debug             # just print fields, don't write

Requires: FUSIONSOLAR_API_USER, FUSIONSOLAR_API_PASS env vars
"""

import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

# Import the API client from fetch script
sys.path.insert(0, str(Path(__file__).parent))
from fetch_fusionsolar_api import FusionSolarAPI, SITES, fix_dns, SAST

SITES_DIR = Path(__file__).parent / "sites"
INTER_CALL_SLEEP = 61


def main():
    username = os.environ.get("FUSIONSOLAR_API_USER", "")
    password = os.environ.get("FUSIONSOLAR_API_PASS", "")
    if not username or not password:
        print("Set FUSIONSOLAR_API_USER and FUSIONSOLAR_API_PASS")
        sys.exit(1)

    # Parse args
    debug_only = "--debug" in sys.argv
    days_back = 1
    specific_date = None

    for i, arg in enumerate(sys.argv):
        if arg == "--days" and i + 1 < len(sys.argv):
            days_back = int(sys.argv[i + 1])
        if arg == "--date" and i + 1 < len(sys.argv):
            specific_date = sys.argv[i + 1]

    fix_dns()
    api = FusionSolarAPI(username, password)

    try:
        if not api.login():
            sys.exit(1)

        # Build dates to backfill
        now = datetime.now(SAST)
        if specific_date:
            dates = [specific_date]
        else:
            dates = [(now - timedelta(days=d)).strftime("%Y-%m-%d") for d in range(1, days_back + 1)]

        all_codes = [s["station_code"] for s in SITES]
        code_to_site = {s["station_code"]: s for s in SITES}

        for date_str in dates:
            print(f"\n{'='*60}")
            print(f"  Backfilling: {date_str}")
            print(f"{'='*60}")

            # Calculate collectTime (epoch ms for midnight SAST)
            dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=SAST)
            collect_time = int(dt.timestamp() * 1000)

            # Fetch hourly KPIs
            print(f"  Fetching hourly KPIs (collectTime={collect_time})...")
            hourly_data = api.get_hourly_data(all_codes, collect_time)
            hourly_map = {}
            for item in hourly_data.get("data", []):
                code = item.get("stationCode")
                if code not in hourly_map:
                    hourly_map[code] = []
                hourly_map[code].append(item)

            print(f"  Got data for {len(hourly_map)} stations")
            time.sleep(INTER_CALL_SLEEP)

            # Process each site
            for code, site in code_to_site.items():
                slug = site["slug"]
                name = site["search_name"]
                entries = hourly_map.get(code, [])

                # Debug: show available fields
                if entries and (debug_only or slug in ["wg-mdantsane-superspar", "mountain-view-sc", "nautica-sc"]):
                    sample = entries[0].get("dataItemMap", {})
                    print(f"\n  📋 {slug} dataItemMap fields:")
                    for k, v in sorted(sample.items()):
                        print(f"      {k:<30} = {v}")

                # Build hourly PV array
                hourly_pv = [0.0] * 24
                for entry in entries:
                    dim = entry.get("dataItemMap", {})
                    ct = entry.get("collectTime", 0)
                    if ct:
                        hour = datetime.fromtimestamp(ct / 1000, tz=SAST).hour
                        # Try all possible PV fields
                        inv_power = float(dim.get("inverter_power", 0) or 0)
                        pv_yield = float(dim.get("PVYield", 0) or 0)
                        ongrid = float(dim.get("ongrid_power", 0) or 0)
                        use_power = float(dim.get("use_power", 0) or 0)
                        buy_power = float(dim.get("buyPower", 0) or 0)

                        # Best PV value: inverter_power or PVYield
                        pv_val = inv_power or pv_yield
                        # If both are 0, try: use_power - buyPower (consumption - grid import = self-consumed PV)
                        if pv_val == 0 and use_power > 0:
                            pv_val = max(0, use_power - buy_power) + ongrid

                        hourly_pv[hour] += round(pv_val, 2)

                hourly_pv = [round(v, 2) for v in hourly_pv]
                total = round(sum(hourly_pv), 2)
                last_hour = max((h for h in range(24) if hourly_pv[h] > 0), default=0)

                if total > 0:
                    print(f"  ✅ {slug}: {total:.1f} kWh ({sum(1 for v in hourly_pv if v > 0)} hours)")
                else:
                    print(f"  ⚠️  {slug}: 0.0 kWh — API returned no hourly PV data")
                    if entries:
                        # Show what IS available
                        sample = entries[len(entries)//2].get("dataItemMap", {})
                        nonzero = {k: v for k, v in sample.items() if v and float(v or 0) != 0}
                        if nonzero:
                            print(f"      Non-zero fields at midday: {nonzero}")

                if debug_only:
                    continue

                # Patch history.json
                history_file = SITES_DIR / slug / "data" / "history.json"
                if not history_file.exists():
                    print(f"      ⚠️  No history.json for {slug}")
                    continue

                history = json.loads(history_file.read_text())

                old_total = history.get(date_str, {}).get("total_kwh", 0)
                if total > old_total:
                    history[date_str] = {
                        "total_kwh": total,
                        "hourly": hourly_pv,
                        "irradiation": history.get(date_str, {}).get("irradiation", [0] * 24),
                        "last_hour": last_hour,
                    }
                    history_file.write_text(json.dumps(history, indent=2))
                    print(f"      📝 History updated: {old_total:.1f} → {total:.1f} kWh")
                else:
                    print(f"      ⏭️  Existing {old_total:.1f} >= new {total:.1f} — keeping existing")

    finally:
        api.logout()

    print(f"\n{'='*60}")
    print("Done!")
    print("Push changes: git add sites/*/data/history.json && git commit && git push")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
