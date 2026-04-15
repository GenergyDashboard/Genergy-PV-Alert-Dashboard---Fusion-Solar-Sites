"""
download_all_sites.py

Downloads the daily plant report from FusionSolar for each configured site.
Logs in once, then loops through the site list: search → download → repeat.

Environment variables (set as GitHub secrets):
  FUSIONSOLAR_USERNAME  - FusionSolar username
  FUSIONSOLAR_PASSWORD  - FusionSolar password

To add a new site, add an entry to the SITES list below.
The search_name must match exactly how it appears in FusionSolar.
"""

import time
import random
import os
import sys
import subprocess
import socket
from pathlib import Path
from playwright.sync_api import sync_playwright

# =============================================================================
# ✏️  SITE LIST — Add or remove sites here
#     search_name: exactly as it appears in FusionSolar plant search
#     slug:        folder name under sites/ (used for file paths)
# =============================================================================
SITES = [
    {"search_name": "Addo Spar Smart Logger",         "slug": "addo-spar"},
    {"search_name": "Bel Essex(Valeo)",                "slug": "bel-essex-valeo"},
    {"search_name": "BMI Park",                        "slug": "bmi-park"},
    {"search_name": "BMI Paterson",                    "slug": "bmi-paterson"},
    {"search_name": "Coega Dairy",                     "slug": "coega-dairy"},
    {"search_name": "Keypak Part 2",                   "slug": "keypak-part-2"},
    {"search_name": "Kirkwood FNB",                    "slug": "kirkwood-fnb"},
    {"search_name": "Kirkwood Spar",                   "slug": "kirkwood-spar"},
    {"search_name": "MP The Pines",                    "slug": "mp-the-pines"},
    {"search_name": "Mountain View Shopping Centre",   "slug": "mountain-view-sc"},
    {"search_name": "RDM Somerset West 5MW",           "slug": "rdm-somerset-west"},
    {"search_name": "Shoprite Parklands",              "slug": "shoprite-parklands"},
    {"search_name": "WG Group - Mdantsane SuperSpar",  "slug": "wg-mdantsane-superspar"},
    {"search_name": "WG Group - Nurture Health",       "slug": "wg-nurture-health"},
]

# =============================================================================
# FusionSolar URLs
# =============================================================================
FUSIONSOLAR_HOST = "intl.fusionsolar.huawei.com"
FUSIONSOLAR_BASE = f"https://{FUSIONSOLAR_HOST}"
LOGIN_URL        = FUSIONSOLAR_BASE
PORTAL_HOME      = (
    f"{FUSIONSOLAR_BASE}/uniportal/pvmswebsite/assets/build/cloud.html"
    "?app-id=smartpvms&instance-id=smartpvms"
    "&zone-id=region-7-075ad9fd-a8fc-46e6-8d88-e829f96a09b7"
    "#/home/list"
)
FALLBACK_IP = "119.8.160.213"

_HERE    = Path(__file__).parent
RAW_DIR  = _HERE / "data" / "raw"


# =============================================================================
# Helpers
# =============================================================================

def fix_dns_resolution():
    print(f"🔍 Checking DNS for {FUSIONSOLAR_HOST}...")
    try:
        ip = socket.gethostbyname(FUSIONSOLAR_HOST)
        print(f"  ✅ DNS OK: {FUSIONSOLAR_HOST} → {ip}")
        return
    except socket.gaierror:
        print(f"  ⚠️  DNS failed, trying Google DNS fallback...")

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
            print(f"  ✅ Resolved via Google DNS: {resolved_ip}")
    except Exception:
        pass

    if not resolved_ip:
        resolved_ip = FALLBACK_IP
        print(f"  ⚠️  Using fallback IP: {resolved_ip}")

    hosts_entry = f"{resolved_ip} {FUSIONSOLAR_HOST}\n"
    try:
        with open("/etc/hosts", "r") as f:
            if FUSIONSOLAR_HOST in f.read():
                print("  ℹ️  Host entry already exists")
                return
        try:
            result = subprocess.run(
                ["sudo", "tee", "-a", "/etc/hosts"],
                input=hosts_entry, capture_output=True, text=True, timeout=5,
            )
            if result.returncode != 0:
                raise RuntimeError("sudo tee failed")
        except Exception:
            with open("/etc/hosts", "a") as f:
                f.write(hosts_entry)
        print(f"  ✅ Added to /etc/hosts: {hosts_entry.strip()}")
    except Exception as e:
        print(f"  ❌ Could not fix DNS: {e}")
        sys.exit(1)

    try:
        ip = socket.gethostbyname(FUSIONSOLAR_HOST)
        print(f"  ✅ DNS now resolves: {FUSIONSOLAR_HOST} → {ip}")
    except socket.gaierror:
        print(f"  ❌ DNS still failing after patch")
        sys.exit(1)


def human_delay(min_s=3, max_s=7):
    delay = random.uniform(min_s, max_s)
    print(f"  ⏳ Waiting {delay:.1f}s...")
    time.sleep(delay)


def random_mouse_movement(page):
    try:
        vs = page.viewport_size
        if vs:
            page.mouse.move(
                random.randint(100, vs["width"] - 100),
                random.randint(100, vs["height"] - 100),
            )
    except Exception:
        pass


def type_human_like(field, text):
    for char in text:
        field.type(char, delay=random.randint(50, 150))


def find_search_field(page):
    strategies = [
        ("role textbox 'Plant name'",    lambda: page.get_by_role("textbox", name="Plant name")),
        ("placeholder 'Plant name'",     lambda: page.locator("input[placeholder*='Plant name']").first),
        ("placeholder 'plant' (lower)",  lambda: page.locator("input[placeholder*='plant']").first),
        ("placeholder 'search' (ci)",    lambda: page.locator("input[placeholder*='search' i]").first),
        ("role searchbox",               lambda: page.get_by_role("searchbox").first),
        ("visible text input",           lambda: page.locator("input[type='text']:visible").first),
        ("any visible input",            lambda: page.locator("input:visible").first),
    ]
    for name, strategy in strategies:
        try:
            field = strategy()
            if field.is_visible(timeout=3000):
                print(f"  ✅ Search field found via: {name}")
                return field
        except Exception:
            continue
    return None


def dismiss_modals(page):
    modal_selectors = [
        ".dpdesign-modal-wrap .dpdesign-modal-close",
        ".dpdesign-modal-wrap button[aria-label='Close']",
        ".dpdesign-modal-wrap .dpdesign-icon-close",
        ".ant-modal-close", ".ant-modal-close-x",
        "button[aria-label='Close']", ".modal-close",
        "button:has-text('×')", "button:has-text('✕')",
        "button:has-text('Close')", "button:has-text('OK')",
        "button:has-text('Got it')", "button:has-text('Confirm')",
    ]
    for sel in modal_selectors:
        try:
            btn = page.locator(sel).first
            if btn.is_visible(timeout=2000):
                btn.click()
                print(f"  ✅ Dismissed modal via: {sel}")
                human_delay(1, 2)
                return
        except Exception:
            continue
    try:
        page.keyboard.press("Escape")
        human_delay(0.5, 1)
        print("  ℹ️  No modal found (or dismissed via Escape)")
    except Exception:
        pass


# =============================================================================
# Download a single site's report
# =============================================================================

def download_site_report(page, search_name, output_file):
    """
    From the portal home/list page, search for a plant, open it,
    go to Report Management, export and download the xlsx.
    Then navigate back to the portal list for the next site.
    """
    print(f"\n  ── Downloading: {search_name} ──")

    # Navigate to portal home (clean slate for each site)
    page.goto(PORTAL_HOME, wait_until="networkidle", timeout=60000)
    human_delay(4, 7)
    random_mouse_movement(page)
    dismiss_modals(page)

    # Search for plant
    print(f"  🔎 Searching for '{search_name}'...")
    search_field = find_search_field(page)
    if not search_field:
        raise RuntimeError(f"Could not find search field for '{search_name}'")

    search_field.click()
    human_delay(1, 2)
    search_field.fill("")
    human_delay(0.5, 1)
    type_human_like(search_field, search_name)
    human_delay(2, 3)

    try:
        page.get_by_role("button", name="Search").click()
    except Exception:
        try:
            page.locator("button:has-text('Search')").first.click()
        except Exception:
            search_field.press("Enter")

    page.wait_for_load_state("networkidle", timeout=30000)
    human_delay(5, 8)

    # Click on the plant
    print(f"  🏢 Selecting '{search_name}'...")
    try:
        page.get_by_role("link", name=search_name).click()
    except Exception:
        page.get_by_text(search_name).first.click()

    page.wait_for_load_state("networkidle", timeout=60000)
    human_delay(5, 8)
    random_mouse_movement(page)

    # Report Management
    print("  📊 Opening Report Management...")
    page.get_by_text("Report Management").click()
    page.wait_for_load_state("networkidle", timeout=60000)
    human_delay(5, 8)

    # Export
    print("  📤 Clicking Export...")
    page.get_by_role("button", name="Export").click()
    human_delay(5, 8)

    # Download
    print("  💾 Downloading...")
    with page.expect_download(timeout=30000) as dl_info:
        page.get_by_title("Download").first.click()
    download = dl_info.value
    output_file.parent.mkdir(parents=True, exist_ok=True)
    download.save_as(output_file)
    print(f"  ✅ Saved: {output_file}")

    # Close dialog
    try:
        page.get_by_role("button", name="Close").click()
    except Exception:
        pass
    human_delay(2, 4)


# =============================================================================
# Main
# =============================================================================

def main():
    print(f"🚀 FusionSolar Multi-Site Downloader")
    print(f"🏢 Sites to download: {len(SITES)}")
    for s in SITES:
        print(f"     • {s['search_name']} → {s['slug']}")

    fix_dns_resolution()

    username = os.environ.get("FUSIONSOLAR_USERNAME")
    password = os.environ.get("FUSIONSOLAR_PASSWORD")
    if not username or not password:
        print("❌ FUSIONSOLAR_USERNAME and FUSIONSOLAR_PASSWORD must be set")
        sys.exit(1)
    print(f"🔐 Username: {username[:4]}***")

    RAW_DIR.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as playwright:
        print("\n🌐 Launching browser...")
        browser = playwright.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-setuid-sandbox",
                  "--disable-blink-features=AutomationControlled"],
        )
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1920, "height": 1080},
            locale="en-US",
            timezone_id="Africa/Johannesburg",
        )
        context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', { get: () => undefined });"
        )
        page = context.new_page()

        try:
            # ── Login once ─────────────────────────────────────────
            print("📱 Navigating to FusionSolar login...")
            page.goto(LOGIN_URL, wait_until="networkidle", timeout=60000)
            human_delay(5, 8)

            print("👤 Entering credentials...")
            page.get_by_role("textbox", name="Username or email").fill(username)
            human_delay(1, 2)
            page.get_by_role("textbox", name="Password").click()
            page.get_by_role("textbox", name="Password").fill(password)
            human_delay(1, 2)
            page.get_by_text("Log In").click()
            page.wait_for_load_state("networkidle", timeout=60000)
            human_delay(7, 10)
            print(f"  📍 After login: {page.url[:80]}")

            # ── Download each site ─────────────────────────────────
            downloaded = []
            failed = []

            for site in SITES:
                output_file = RAW_DIR / f"{site['slug']}.xlsx"
                try:
                    download_site_report(page, site["search_name"], output_file)
                    downloaded.append(site["search_name"])
                except Exception as err:
                    print(f"  ❌ Failed to download {site['search_name']}: {err}")
                    failed.append(site["search_name"])
                    try:
                        safe = site["slug"].replace("/", "_")
                        page.screenshot(path=f"error_{safe}.png", full_page=True)
                    except Exception:
                        pass

            # ── Summary ────────────────────────────────────────────
            print(f"\n{'='*50}")
            print(f"✅ Downloaded: {len(downloaded)}/{len(SITES)}")
            for name in downloaded:
                print(f"   ✅ {name}")
            if failed:
                print(f"❌ Failed: {len(failed)}")
                for name in failed:
                    print(f"   ❌ {name}")
            print(f"{'='*50}")

            if not downloaded:
                print("❌ No sites downloaded — aborting")
                sys.exit(1)

        except Exception as err:
            print(f"❌ Fatal error: {err}")
            try:
                page.screenshot(path="error_screenshot.png", full_page=True)
            except Exception:
                pass
            raise

        finally:
            human_delay(2, 3)
            context.close()
            browser.close()
            print("🔒 Browser closed")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"❌ Script failed: {e}")
        sys.exit(1)
