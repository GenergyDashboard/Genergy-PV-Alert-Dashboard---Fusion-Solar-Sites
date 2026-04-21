"""
fix_irradiation_history.py

One-time migration: shifts all stored irradiation data BACK by 1 hour
in every site's history.json.

The old processors stored irradiation with a wrong UTC offset, making
the irradiation curve start ~1 hour too late. The new processors now
fetch correctly with +1 shift, but the 30-day avg is still computed
from old misaligned data.

This script fixes all historical irradiation in-place so the avg
immediately matches the new correct alignment.

Run this ONCE in each repo (FusionSolar + GoodWe) then delete it.

Usage:
    python fix_irradiation_history.py           # dry run (shows what would change)
    python fix_irradiation_history.py --apply   # actually modify the files
"""

import json
import sys
from pathlib import Path

SITES_DIR = Path(__file__).parent / "sites"
DRY_RUN = "--apply" not in sys.argv


def shift_irrad_back_one(irrad):
    """Shift irradiation array back by 1 hour (index N becomes index N-1)."""
    if not irrad or len(irrad) < 24:
        return irrad
    # Shift left by 1: old[1] → new[0], old[2] → new[1], etc.
    shifted = irrad[1:] + [0.0]
    return [round(v, 1) for v in shifted]


def process_history_file(history_file):
    """Fix irradiation in one history.json file."""
    try:
        with open(history_file) as f:
            history = json.load(f)
    except Exception as e:
        print(f"    ❌ Could not read: {e}")
        return 0

    fixed_dates = 0
    for date_str, day_data in history.items():
        irrad = day_data.get("irradiation", [])
        if not irrad or len(irrad) < 24:
            continue

        # Check if this data needs shifting:
        # If hour 6 has irradiation but hour 5 doesn't, it's likely the old offset
        # (sunrise in SA is ~06:00-07:00 SAST, so irrad should start around hour 5-6)
        old_irrad = irrad
        new_irrad = shift_irrad_back_one(old_irrad)

        day_data["irradiation"] = new_irrad
        fixed_dates += 1

    if fixed_dates > 0 and not DRY_RUN:
        with open(history_file, "w") as f:
            json.dump(history, f, indent=2)

    return fixed_dates


def main():
    if DRY_RUN:
        print("🔍 DRY RUN — no files will be modified. Use --apply to commit changes.\n")
    else:
        print("🔧 APPLYING irradiation shift to all history files.\n")

    if not SITES_DIR.exists():
        print(f"❌ Sites directory not found: {SITES_DIR}")
        sys.exit(1)

    total_files = 0
    total_dates = 0

    for site_dir in sorted(SITES_DIR.iterdir()):
        if not site_dir.is_dir():
            continue
        history_file = site_dir / "data" / "history.json"
        if not history_file.exists():
            continue

        slug = site_dir.name
        fixed = process_history_file(history_file)
        if fixed > 0:
            status = "would fix" if DRY_RUN else "✅ fixed"
            print(f"  {status}: {slug} — {fixed} dates shifted")
            total_files += 1
            total_dates += fixed
        else:
            print(f"  ⏭️  {slug} — no irradiation data to fix")

    print(f"\n{'='*50}")
    action = "Would fix" if DRY_RUN else "Fixed"
    print(f"{action}: {total_dates} date entries across {total_files} sites")
    if DRY_RUN:
        print("Run with --apply to commit changes.")
    else:
        print("✅ Done! The next scrape will compute correct 30-day averages.")
    print(f"{'='*50}")


if __name__ == "__main__":
    main()
