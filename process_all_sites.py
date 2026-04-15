"""
process_all_sites.py

Processes all FusionSolar site xlsx files and produces per-site dashboard JSON.

For each site:
- Reads the downloaded xlsx from data/raw/<slug>.xlsx
- Extracts hourly PV Yield
- Fetches irradiation from Open-Meteo API
- Maintains rolling 30-day history
- Calculates statistics (avg, min, max, percentiles)
- Sends Telegram alerts for underperformance
- Writes dashboard-ready processed.json

All thresholds are purely data-driven (no hardcoded values).
"""

import json
import math
import sys
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pandas as pd
import requests

# =============================================================================
# ✏️  SITE REGISTRY
#     slug must match download_all_sites.py and the folder under sites/
# =============================================================================
SITES = {
    "addo-spar": {
        "display_name": "Addo Spar",
        "lat": -33.5733,
        "lon": 25.7467,
    },
    "bel-essex-valeo": {
        "display_name": "Bel Essex (Valeo)",
        "lat": -33.783722863963575,
        "lon": 25.421007352846207,
    },
    "bmi-park": {
        "display_name": "BMI Park",
        "lat": -33.91606455874616,
        "lon": 25.600899466686126,
    },
    "bmi-paterson": {
        "display_name": "BMI Paterson",
        "lat": -33.91606455874616,
        "lon": 25.600899466686126,
    },
    "coega-dairy": {
        "display_name": "Coega Dairy",
        "lat": -33.91606455874616,
        "lon": 25.600899466686126,
    },
    "keypak-part-2": {
        "display_name": "Keypak Part 2",
        "lat": -33.91606455874616,
        "lon": 25.600899466686126,
    },
    "kirkwood-fnb": {
        "display_name": "Kirkwood FNB",
        "lat": -33.40047315873728,
        "lon": 25.44748345729338,
    },
    "kirkwood-spar": {
        "display_name": "Kirkwood Spar",
        "lat": -33.40047315873728,
        "lon": 25.44748345729338,
    },
    "mp-the-pines": {
        "display_name": "MP The Pines",
        "lat": -33.919655559259084,
        "lon": 18.445906035596476,
    },
    "mountain-view-sc": {
        "display_name": "Mountain View Shopping Centre",
        "lat": -33.97422273793887,
        "lon": 25.61212584301634,
    },
    "rdm-somerset-west": {
        "display_name": "RDM Somerset West 5MW",
        "lat": -34.06499500593473,
        "lon": 18.781596709489115,
    },
    "shoprite-parklands": {
        "display_name": "Shoprite Parklands",
        "lat": -33.815026895703326,
        "lon": 18.50075959819609,
    },
    "wg-mdantsane-superspar": {
        "display_name": "WG Group - Mdantsane SuperSpar",
        "lat": -32.93641148600732,
        "lon": 27.739226592352193,
    },
    "wg-nurture-health": {
        "display_name": "WG Group - Nurture Health",
        "lat": -32.948552600071515,
        "lon": 27.94150392199964,
    },
}

# =============================================================================
# CONFIG
# =============================================================================
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID",   "")

PACE_THRESHOLD_PCT = 0.30
OFFLINE_THRESHOLD  = 0.01
HISTORY_DAYS       = 30
MIN_HISTORY_DAYS   = 7

# FusionSolar reports are already in SAST — no offset needed
REPORT_UTC_OFFSET  = 0

# PV Yield column fallback (auto-detected from headers)
PV_COLUMN_INDEX    = 4

_HERE     = Path(__file__).parent
RAW_DIR   = _HERE / "data" / "raw"
SITES_DIR = _HERE / "sites"

SAST = timezone(timedelta(hours=2))


# =============================================================================
# Solar curve
# =============================================================================

def solar_window(month: int) -> tuple:
    mid_day   = (month - 1) * 30 + 15
    amplitude = 0.75
    angle     = 2 * math.pi * (mid_day - 355) / 365
    shift     = amplitude * math.cos(angle)
    return 6.0 - shift, 18.0 + shift


def solar_curve_fraction(hour: int, month: int) -> float:
    sunrise, sunset = solar_window(month)
    solar_day = sunset - sunrise
    if solar_day <= 0:
        return 0.0
    elapsed = (hour + 1) - sunrise
    if elapsed <= 0:
        return 0.0
    if elapsed >= solar_day:
        return 1.0
    return (1 - math.cos(math.pi * elapsed / solar_day)) / 2


# =============================================================================
# Irradiation
# =============================================================================

def fetch_irradiation(date_str: str, lat: float, lon: float) -> list:
    try:
        resp = requests.get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude": lat, "longitude": lon,
                "hourly": "shortwave_radiation",
                "timezone": "Africa/Johannesburg",
                "start_date": date_str, "end_date": date_str,
            },
            timeout=15,
        )
        resp.raise_for_status()
        irrad = resp.json().get("hourly", {}).get("shortwave_radiation", [])
        while len(irrad) < 24:
            irrad.append(0)
        return [round(v if v else 0, 1) for v in irrad[:24]]
    except Exception as e:
        print(f"    ⚠️  Irradiation fetch failed: {e}")
        return [0] * 24


# =============================================================================
# Parse a FusionSolar xlsx
# =============================================================================

def parse_fusionsolar_report(filepath: Path) -> dict:
    """
    Parse a FusionSolar plant report xlsx.
    Returns {"date", "total_kwh", "hourly" [24], "last_hour", "plant_name"}
    """
    df = pd.read_excel(filepath, header=None, sheet_name=0)

    # Extract plant name from A1 (e.g. "Plant Report_Coega Dairy")
    plant_name_raw = str(df.iloc[0, 0]) if not pd.isna(df.iloc[0, 0]) else ""
    plant_name = plant_name_raw.replace("Plant Report_", "").strip()

    # Find headers (row 1)
    headers = [str(h).strip() if not pd.isna(h) else "" for h in df.iloc[1].tolist()]

    pv_col = next(
        (i for i, h in enumerate(headers) if "PV Yield" in h),
        PV_COLUMN_INDEX,
    )

    hourly      = [0.0] * 24
    total       = 0.0
    last_hour   = 0
    report_date = None

    for idx in range(2, len(df)):
        row    = df.iloc[idx]
        ts_raw = row.iloc[0]
        if pd.isna(ts_raw):
            continue
        try:
            ts   = pd.Timestamp(ts_raw)
            hour = (ts.hour + REPORT_UTC_OFFSET) % 24
            if report_date is None:
                report_date = ts.strftime("%Y-%m-%d")
        except Exception:
            continue

        pv_val = float(row.iloc[pv_col]) if not pd.isna(row.iloc[pv_col]) else 0.0
        hourly[hour] = round(pv_val, 4)
        total += pv_val
        last_hour = hour

    return {
        "plant_name": plant_name,
        "date":       report_date or datetime.now(SAST).strftime("%Y-%m-%d"),
        "total_kwh":  round(total, 3),
        "hourly":     hourly,
        "last_hour":  last_hour,
    }


# =============================================================================
# History & stats
# =============================================================================

def load_history(history_file: Path) -> dict:
    if not history_file.exists():
        return {}
    try:
        with open(history_file) as f:
            return json.load(f)
    except Exception:
        return {}


def save_history(history: dict, history_file: Path):
    history_file.parent.mkdir(parents=True, exist_ok=True)
    cutoff = (datetime.now(SAST) - timedelta(days=HISTORY_DAYS)).strftime("%Y-%m-%d")
    history = {k: v for k, v in history.items() if k >= cutoff}
    with open(history_file, "w") as f:
        json.dump(history, f, indent=2)


def percentile(sorted_vals: list, p: float) -> float:
    if not sorted_vals:
        return 0
    if len(sorted_vals) == 1:
        return sorted_vals[0]
    k = (len(sorted_vals) - 1) * (p / 100.0)
    f = int(k)
    c = f + 1
    if c >= len(sorted_vals):
        return sorted_vals[-1]
    d = k - f
    return sorted_vals[f] + d * (sorted_vals[c] - sorted_vals[f])


def calculate_stats(history: dict, exclude_date: str = None) -> dict:
    empty = {
        "hourly_avg": [0]*24, "hourly_min": [0]*24, "hourly_max": [0]*24,
        "hourly_p10": [0]*24, "hourly_p25": [0]*24, "hourly_p75": [0]*24, "hourly_p90": [0]*24,
        "hourly_irrad_avg": [0]*24,
        "daily_min": 0, "daily_max": 0, "daily_avg": 0, "sample_days": 0,
    }
    if not history:
        return empty

    hourly_values = [[] for _ in range(24)]
    daily_totals = []

    for date, day in history.items():
        if date == exclude_date:
            continue
        hourly = day.get("hourly", [0]*24)
        total = day.get("total_kwh", 0)
        if total > 0:
            daily_totals.append(total)
            for h in range(24):
                if h < len(hourly):
                    hourly_values[h].append(hourly[h])

    if not daily_totals:
        return empty

    hourly_avg = [round(sum(v)/len(v), 2) if v else 0 for v in hourly_values]
    hourly_min = []
    for h in range(24):
        nz = [v for v in hourly_values[h] if v > 0]
        hourly_min.append(round(min(nz), 2) if nz else 0)
    hourly_max = [round(max(v), 2) if v else 0 for v in hourly_values]

    hourly_p10, hourly_p25, hourly_p75, hourly_p90 = [], [], [], []
    for h in range(24):
        sv = sorted(hourly_values[h])
        hourly_p10.append(round(percentile(sv, 10), 2))
        hourly_p25.append(round(percentile(sv, 25), 2))
        hourly_p75.append(round(percentile(sv, 75), 2))
        hourly_p90.append(round(percentile(sv, 90), 2))

    irrad_values = [[] for _ in range(24)]
    for date, day in history.items():
        if date == exclude_date:
            continue
        irrad = day.get("irradiation", [0]*24)
        total = day.get("total_kwh", 0)
        if total > 0:
            for h in range(24):
                if h < len(irrad):
                    irrad_values[h].append(irrad[h])

    hourly_irrad_avg = [round(sum(v)/len(v), 1) if v else 0 for v in irrad_values]

    return {
        "hourly_avg": hourly_avg, "hourly_min": hourly_min, "hourly_max": hourly_max,
        "hourly_p10": hourly_p10, "hourly_p25": hourly_p25,
        "hourly_p75": hourly_p75, "hourly_p90": hourly_p90,
        "hourly_irrad_avg": hourly_irrad_avg,
        "daily_min": round(min(daily_totals), 1),
        "daily_max": round(max(daily_totals), 1),
        "daily_avg": round(sum(daily_totals)/len(daily_totals), 1),
        "sample_days": len(daily_totals),
    }


# =============================================================================
# Status checks
# =============================================================================

def determine_status(data: dict, month: int, stats: dict, irradiation: list = None) -> tuple:
    total       = data["total_kwh"]
    hour        = data["last_hour"]
    sunrise, sunset = solar_window(month)
    alerts      = {"offline": False, "pace_low": False, "total_low": False}
    sample_days = stats.get("sample_days", 0)

    if hour < int(sunrise) or hour >= int(sunset):
        return "ok", alerts, {"reason": "nighttime", "sample_days": sample_days}

    if total < OFFLINE_THRESHOLD:
        alerts["offline"] = True
        return "offline", alerts, {"reason": "no generation during daylight", "sample_days": sample_days}

    curve_frac = solar_curve_fraction(hour, month)
    if curve_frac < 0.10:
        return "ok", alerts, {"reason": "too early", "sample_days": sample_days}

    if sample_days < MIN_HISTORY_DAYS:
        return "ok", alerts, {"reason": f"bootstrap ({sample_days}/{MIN_HISTORY_DAYS})", "sample_days": sample_days}

    effective_expected = stats["daily_avg"]
    irrad_factor = 1.0
    if irradiation and stats.get("hourly_irrad_avg"):
        avg_irrad = stats["hourly_irrad_avg"]
        today_cum = sum(irradiation[:hour+1])
        avg_cum   = sum(avg_irrad[:hour+1])
        if avg_cum > 0:
            irrad_factor = max(min(today_cum / avg_cum, 1.5), 0.1)

    expected_by_now = effective_expected * curve_frac * irrad_factor
    pace_trigger    = expected_by_now * PACE_THRESHOLD_PCT
    projected_total = total / curve_frac if curve_frac > 0 else 0

    if total < pace_trigger:
        alerts["pace_low"] = True
    daily_min = stats["daily_min"]
    adjusted_min = daily_min * irrad_factor if irrad_factor < 1.0 else daily_min
    if projected_total < adjusted_min:
        alerts["total_low"] = True

    status = "low" if (alerts["pace_low"] or alerts["total_low"]) else "ok"
    return status, alerts, {
        "curve_fraction": round(curve_frac, 3),
        "expected_by_now": round(expected_by_now, 1),
        "irrad_factor": round(irrad_factor, 3),
        "actual_kwh": round(total, 2),
        "pace_trigger": round(pace_trigger, 1),
        "projected_total": round(projected_total, 1),
        "daily_min": daily_min,
        "sunrise": round(sunrise, 2), "sunset": round(sunset, 2),
        "sample_days": sample_days,
    }


# =============================================================================
# Telegram
# =============================================================================

def send_telegram(message: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"},
            timeout=10,
        )
        return resp.status_code == 200
    except Exception:
        return False


def send_alerts(plant_name: str, status: str, alerts: dict, data: dict, debug: dict, state_file: Path):
    now_str     = datetime.now(SAST).strftime("%Y-%m-%d %H:%M SAST")
    total       = data["total_kwh"]
    hour        = data["last_hour"]
    sample_days = debug.get("sample_days", 0)

    if sample_days < MIN_HISTORY_DAYS and not alerts["offline"]:
        return

    prev_status = "ok"
    if state_file.exists():
        try:
            with open(state_file) as f:
                prev_status = json.load(f).get("last_status", "ok")
        except Exception:
            pass

    if alerts["offline"]:
        send_telegram(
            f"🔴 <b>{plant_name} — OFFLINE</b>\n"
            f"No generation detected.\n"
            f"Total today: <b>{total:.2f} kWh</b> (as of {hour:02d}:00)\n"
            f"🕐 {now_str}"
        )
    else:
        if alerts["pace_low"]:
            send_telegram(
                f"🟡 <b>{plant_name} — LOW PACE</b>\n"
                f"Actual: <b>{total:.1f} kWh</b> | Expected: <b>~{debug.get('expected_by_now',0):.0f} kWh</b>\n"
                f"Hour: {hour:02d}:00 | 🕐 {now_str}"
            )
        if alerts["total_low"]:
            send_telegram(
                f"🟠 <b>{plant_name} — POOR DAY PROJECTED</b>\n"
                f"Projected: <b>~{debug.get('projected_total',0):.0f} kWh</b> | "
                f"Min: <b>{debug.get('daily_min',0):.0f} kWh</b>\n"
                f"Hour: {hour:02d}:00 | 🕐 {now_str}"
            )
        if status == "ok" and prev_status in ("low", "offline"):
            send_telegram(
                f"✅ <b>{plant_name} — RECOVERED</b>\n"
                f"Total: <b>{total:.1f} kWh</b> (as of {hour:02d}:00)\n"
                f"🕐 {now_str}"
            )

    state_file.parent.mkdir(parents=True, exist_ok=True)
    with open(state_file, "w") as f:
        json.dump({"last_status": status, "last_checked": now_str}, f, indent=2)


# =============================================================================
# Main
# =============================================================================

def main():
    print(f"🔄 Processing FusionSolar multi-site data")

    now   = datetime.now(SAST)
    month = now.month

    processed_count = 0
    skipped_count = 0

    for slug, config in SITES.items():
        display_name = config["display_name"]
        lat          = config["lat"]
        lon          = config["lon"]
        raw_file     = RAW_DIR / f"{slug}.xlsx"
        site_dir     = SITES_DIR / slug / "data"
        history_file = site_dir / "history.json"
        output_file  = site_dir / "processed.json"
        state_file   = site_dir / "alert_state.json"

        print(f"\n  ── {display_name} ({slug}) ──")

        if not raw_file.exists():
            print(f"    ⚠️  No xlsx found: {raw_file} — skipping")
            skipped_count += 1
            continue

        # Parse xlsx
        data = parse_fusionsolar_report(raw_file)
        print(f"    📅 Date: {data['date']} | Plant: {data['plant_name']}")
        print(f"    ⚡ {data['total_kwh']:.1f} kWh | Last hour: {data['last_hour']:02d}:00")

        # Fetch irradiation
        irradiation = fetch_irradiation(data["date"], lat, lon)

        # Load & update history
        history = load_history(history_file)
        history[data["date"]] = {
            "total_kwh":    data["total_kwh"],
            "hourly":       data["hourly"],
            "irradiation":  irradiation,
            "last_updated": now.strftime("%Y-%m-%d %H:%M SAST"),
            "last_hour":    data["last_hour"],
        }
        save_history(history, history_file)

        # Stats
        stats = calculate_stats(history, exclude_date=data["date"])

        # Status
        status, alerts, debug = determine_status(data, month, stats, irradiation)

        print(f"    📈 Avg: {stats['daily_avg']:.1f} kWh | Days: {stats['sample_days']} | Status: {status.upper()}")

        # Alerts
        send_alerts(display_name, status, alerts, data, debug, state_file)

        # Write dashboard JSON
        output = {
            "plant":        display_name,
            "last_updated": now.strftime("%Y-%m-%d %H:%M SAST"),
            "date":         data["date"],
            "total_kwh":    data["total_kwh"],
            "last_hour":    data["last_hour"],
            "status":       status,
            "alerts":       alerts,
            "today": {
                "hourly_pv":   data["hourly"],
                "irradiation": irradiation,
            },
            "hourly_pv":   data["hourly"],
            "irradiation": irradiation,
            "stats_30day": stats,
            "history":     history,
            "thresholds": {
                "daily_avg":          stats["daily_avg"],
                "daily_min":          stats["daily_min"],
                "pace_threshold_pct": PACE_THRESHOLD_PCT,
                "sample_days":        stats["sample_days"],
                "min_history_days":   MIN_HISTORY_DAYS,
            },
            "debug": debug,
        }
        site_dir.mkdir(parents=True, exist_ok=True)
        with open(output_file, "w") as f:
            json.dump(output, f, indent=2)
        print(f"    ✅ Saved: {output_file}")
        processed_count += 1

    print(f"\n{'='*50}")
    print(f"✅ Processed: {processed_count} | Skipped: {skipped_count}")
    print(f"{'='*50}")


if __name__ == "__main__":
    main()
