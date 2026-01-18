# Keyword Analysis of Restaurant Reviews (Yandex Maps, Kazan)

This project collects reviews for restaurants/cafes from **Yandex Maps** and prepares the data for **keyword analysis** (per place and/or across the whole dataset).

## Project layout

- `Scraping/`
  - `urls.txt` — list of place review URLs (one per line), typically like: `.../org/<slug>/<id>/reviews/`
  - `scraper_for_reviews.py` — Playwright-based scraper that exports reviews to JSONL
  - `all_reviews.jsonl` — scraper output (JSONL: 1 line = 1 review)
- `Results_of_analysis/`
  - `org_quality_report.xlsx` — analysis/quality report (Excel)
  - `3org_quality_report.xlsx` — additional report/version (Excel)

## Requirements

- Python 3.9+ recommended
- Playwright (for browser automation)

## Install

```bash
pip install playwright
playwright install
```

## Usage

### 1) Prepare input URLs

Put your Yandex Maps review links into `Scraping/urls.txt` (one URL per line).

### 2) Scrape reviews into JSONL

Run from the project root:

```bash
python Scraping/scraper_for_reviews.py --headful --block-resources --urls Scraping/urls.txt --out Scraping/all_reviews.jsonl --max-reviews 600
```

**Notes**
- `--max-reviews` limits the number of reviews **per place**.
- `--block-resources` speeds things up by blocking heavy assets (images/fonts/etc.).
- If a **CAPTCHA** appears, the script pauses and asks you to solve it in the opened browser window, then continues.

## Output format (`all_reviews.jsonl`)

The output is **JSONL** (newline-delimited JSON). Each line is a single review object with fields such as:
- place name / identifier (e.g., `restaurant_name`, `org_id`)
- review text (e.g., `text`)
- rating (e.g., `rating`)
- date (e.g., `date`)
- source link (e.g., `source_url`)
(Exact field names may vary slightly depending on the scraper version.)

## Analysis results

The Excel files in `Results_of_analysis/` contain the prepared analysis outputs and/or data quality checks for the collected reviews.

## Troubleshooting

- **Playwright browser not found**: rerun `playwright install`
- **Slow scraping / timeouts**: keep `--block-resources`, reduce `--max-reviews`, or run fewer URLs per batch
- **CAPTCHA**: solve it manually in the opened browser window when prompted; the scraper should continue afterward
