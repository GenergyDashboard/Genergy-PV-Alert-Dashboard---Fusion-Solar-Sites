"""
wipe_all_data.py

Clears all historical and processed data across every site.

Usage:
    python wipe_all_data.py          # wipe everything
    python wipe_all_data.py --today  # only remove today's entry from each history
"""

import json
import sys
from pathlib import Path
from datetime import datetime, timezone, timedelta

SAST = timezone(timedelta(hours=2))
SITES_DIR = Path(__file__).parent / "sites"


def wipe_all():
    count = 0
    for site_dir in sorted(SITES_DIR.iterdir()):
        data_dir = site_dir / "data"
        if not data_dir.is_dir():
            continue
        for f in data_dir.glob("*.json"):
            print(f"  🗑️  Deleting {f}")
            f.unlink()
            count += 1
    print(f"\n✅ Wiped {count} files across all sites.")


def wipe_today():
    today = datetime.now(SAST).strftime("%Y-%m-%d")
    print(f"📅 Removing entries for {today}...\n")
    count = 0
    for site_dir in sorted(SITES_DIR.iterdir()):
        data_dir = site_dir / "data"
        history_file = data_dir / "history.json"
        if not history_file.exists():
            continue
        try:
            with open(history_file) as f:
                history = json.load(f)
            if today in history:
                del history[today]
                with open(history_file, "w") as f:
                    json.dump(history, f, indent=2)
                print(f"  ✅ {site_dir.name}: removed {today}")
                count += 1
            else:
                print(f"  ⏭️  {site_dir.name}: no entry for {today}")
        except Exception as e:
            print(f"  ❌ {site_dir.name}: error - {e}")

    for site_dir in sorted(SITES_DIR.iterdir()):
        data_dir = site_dir / "data"
        for fname in ["processed.json", "alert_state.json"]:
            f = data_dir / fname
            if f.exists():
                f.unlink()

    print(f"\n✅ Removed today's data from {count} sites.")


if __name__ == "__main__":
    if "--today" in sys.argv:
        wipe_today()
    else:
        print("🧹 Wiping ALL data across all FusionSolar sites...\n")
        wipe_all()
