# FusionSolar Sites — PV Monitoring Dashboards

Multi-site PV monitoring dashboards for all FusionSolar stations.

## Architecture

```
download_all_sites.py           ← Scraper: logs in once, downloads all sites
process_all_sites.py            ← Processor: parses xlsx → per-site JSON + alerts
wipe_all_data.py                ← Utility: clear data for fresh start
data/raw/
  bel-essex-valeo.xlsx          ← Downloaded reports (one per site)
  bmi-park.xlsx
  ...
sites/
  bel-essex-valeo/
    index.html                  ← Dashboard (GitHub Pages)
    data/
      processed.json
      history.json
      alert_state.json
  bmi-park/
    ...
  (11 sites total)
.github/workflows/
  scrape.yml                    ← Hourly cron
  wipe.yml                      ← Manual data wipe
```

## Sites

| Site | Slug | Location |
|------|------|----------|
| Bel Essex (Valeo) | bel-essex-valeo | -33.7837, 25.4210 |
| BMI Park | bmi-park | -33.9161, 25.6009 |
| BMI Paterson | bmi-paterson | -33.9161, 25.6009 |
| Coega Dairy | coega-dairy | -33.9161, 25.6009 |
| Keypak Part 2 | keypak-part-2 | -33.9161, 25.6009 |
| MP The Pines | mp-the-pines | -33.9197, 18.4459 |
| Mountain View SC | mountain-view-sc | -33.9742, 25.6121 |
| RDM Somerset West 5MW | rdm-somerset-west | -34.0650, 18.7816 |
| Shoprite Parklands | shoprite-parklands | -33.8150, 18.5008 |
| WG Mdantsane SuperSpar | wg-mdantsane-superspar | -32.9364, 27.7392 |
| WG Nurture Health | wg-nurture-health | -32.9486, 27.9415 |

## Setup

1. Create GitHub repo, push via `git init` → `git push`
2. Add secrets: `FUSIONSOLAR_USERNAME`, `FUSIONSOLAR_PASSWORD`, `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`
3. Enable GitHub Pages: Settings → Pages → Source: `main`, root
4. Enable read/write: Settings → Actions → General → Workflow permissions → Read and write

## Adding a New Site

1. Add to `SITES` list in `download_all_sites.py` (search_name + slug)
2. Add to `SITES` dict in `process_all_sites.py` (slug, display_name, lat, lon)
3. Create `sites/<slug>/data/.gitkeep`
4. Copy any existing `sites/*/index.html` to `sites/<slug>/index.html` and update the site name
5. Update the overview dashboard's `ALL_SITES` array
