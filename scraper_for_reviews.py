#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Fast Yandex Maps reviews scraper (Playwright, Python) -> JSONL (streaming)

Key features:
- Reads URLs from urls.txt (one per line)
- For each place, scrolls reviews and saves up to --max-reviews (per place) into one JSONL file
- Deduplicates by review_key across restarts
- On CAPTCHA: pauses and asks you to solve it in the browser, then continues

Fields per review:
- restaurant_name
- author_id
- author_badge, author_level
- date_iso (and date YYYY-MM-DD)
- rating (float 1..5)
- text (newlines collapsed to spaces)

Run:
  pip install playwright
  playwright install
  python scrape_yandex_reviews_fixed.py --headful --block-resources --urls urls.txt --out all_reviews.jsonl --max-reviews 600
"""

import argparse
import hashlib
import json
import re
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from playwright.sync_api import sync_playwright, TimeoutError as PwTimeoutError


# -----------------------------
# Selectors
# -----------------------------
SCROLL_CONTAINER_CANDIDATES = [
    "div.business-reviews-card-view__reviews-container",
    "div[class*='business-reviews-card-view__reviews-container']",
]
REVIEW_CARD_CANDIDATES = [
    "div.business-reviews-card-view__review[role='listitem']",
    "div[class*='business-reviews-card-view__review'][role='listitem']",
]

RESTAURANT_NAME_SEL = "h1.card-title-view__title[itemprop='name']"
RESTAURANT_NAME_FALLBACK = "h1[itemprop='name']"

# Regex
ORG_ID_RE = re.compile(r"/org/[^/]+/(\d+)")
CAPTION_RE = re.compile(r"^(?P<title>.+?)\s+(?P<level>\d+)\s*уров", re.IGNORECASE)


def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def pick_first_existing(page, selectors: List[str]) -> Optional[str]:
    for sel in selectors:
        try:
            if page.locator(sel).count() > 0:
                return sel
        except Exception:
            pass
    return None


def cleanup_title(title: str) -> str:
    t = title.strip()
    for sep in ["—", "|", "–", "-"]:
        if "яндекс" in t.lower() and sep in t:
            left = t.split(sep)[0].strip()
            if left:
                return left
    return t


def detect_captcha_or_block(page) -> bool:
    url = (page.url or "").lower()
    if "showcaptcha" in url or "captcha" in url:
        return True

    markers = ["подтвердите, что вы не робот", "я не робот", "captcha", "robot"]
    try:
        txt = (page.locator("body").inner_text(timeout=1500) or "").lower()
        return any(m in txt for m in markers)
    except Exception:
        return False


def try_click_any(page, texts: List[str], timeout_ms: int = 400) -> None:
    for t in texts:
        try:
            loc = page.get_by_text(t, exact=True)
            if loc.count() > 0:
                loc.first.click(timeout=timeout_ms)
                page.wait_for_timeout(200)
        except Exception:
            pass


def dismiss_popups(page) -> None:
    try_click_any(page, ["Закрыть", "Понятно", "Ок", "ОК"])
    try_click_any(page, ["Принять", "Согласен", "Согласна"])


def extract_restaurant_name(page) -> str:
    for sel in [RESTAURANT_NAME_SEL, RESTAURANT_NAME_FALLBACK]:
        h1 = page.locator(sel)
        if h1.count() > 0:
            try:
                name = h1.first.evaluate(
                    """
                    (el) => {
                        for (const n of el.childNodes) {
                            if (n.nodeType === Node.TEXT_NODE) {
                                const t = (n.textContent || '').trim();
                                if (t) return t;
                            }
                        }
                        return (el.textContent || '').trim();
                    }
                    """
                )
                name = (name or "").strip()
                if name and len(name) < 200:
                    return name
            except Exception:
                pass

    try:
        og = page.locator("meta[property='og:title']")
        if og.count() > 0:
            t = (og.first.get_attribute("content") or "").strip()
            if t:
                return cleanup_title(t)
    except Exception:
        pass

    try:
        t = (page.title() or "").strip()
        if t:
            return cleanup_title(t)
    except Exception:
        pass

    return ""


def wait_reviews_ready(page, card_sel: str, debug: bool) -> None:
    page.wait_for_selector(card_sel, timeout=30_000)
    ready_selectors = [
        f"{card_sel} meta[itemprop='datePublished']",
        f"{card_sel} meta[itemprop='ratingValue']",
        f"{card_sel} .business-review-view__body",
        f"{card_sel} .business-review-view__text",
    ]
    for rs in ready_selectors:
        try:
            page.wait_for_selector(rs, timeout=20_000)
            if debug:
                print("DEBUG: reviews ready by selector:", rs)
            return
        except Exception:
            continue
    if debug:
        print("DEBUG: reviews ready wait timed out; continue anyway")


def parse_caption(caption: str) -> Tuple[str, str, Optional[int]]:
    caption = (caption or "").strip()
    m = CAPTION_RE.match(caption)
    if not m:
        return caption, "", None
    return caption, m.group("title").strip(), int(m.group("level"))


def to_float(x: str) -> Optional[float]:
    x = (x or "").strip()
    if not x:
        return None
    try:
        return float(x.replace(",", "."))
    except Exception:
        return None


def block_heavy_resources(page, enabled: bool) -> None:
    if not enabled:
        return

    def handler(route, request):
        rtype = request.resource_type
        if rtype in ("image", "font", "media"):
            route.abort()
        else:
            route.continue_()

    page.route("**/*", handler)


def scroll_real_container(container, page, step_ratio: float, debug: bool) -> Dict:
    info = container.evaluate(
        """
        (el, r) => {
            function findScrollable(x){
                while (x && x !== document.body) {
                    const st = getComputedStyle(x);
                    const oy = st.overflowY;
                    if ((oy === 'auto' || oy === 'scroll') && x.scrollHeight > x.clientHeight + 5) {
                        return x;
                    }
                    x = x.parentElement;
                }
                return document.scrollingElement || document.documentElement;
            }
            const sc = findScrollable(el);
            const before = sc.scrollTop;
            const step = Math.floor(sc.clientHeight * r);
            sc.scrollTop = before + step;
            return {
                moved: sc.scrollTop !== before,
                before,
                after: sc.scrollTop,
                clientHeight: sc.clientHeight,
                scrollHeight: sc.scrollHeight
            };
        }
        """,
        step_ratio,
    )

    if not info.get("moved", False):
        try:
            container.hover(timeout=1500)
            page.mouse.wheel(0, int(1600 * step_ratio))
        except Exception:
            pass

    if debug:
        print(
            f"DEBUG: scroll moved={info.get('moved', False)} "
            f"scrollTop {info.get('before', 0)}->{info.get('after', 0)} "
            f"clientH={info.get('clientHeight', 0)} scrollH={info.get('scrollHeight', 0)}"
        )

    return info


def handle_captcha_interactive(page, url: str, headful: bool, debug: bool) -> str:
    if not headful:
        print(f"\nCAPTCHA detected on: {url}")
        print("Headless mode can't be solved. Re-run with --headful. Skipping URL.\n")
        return "skip"

    try:
        page.bring_to_front()
    except Exception:
        pass

    print("\n" + "=" * 72)
    print("CAPTCHA detected.")
    print("1) Solve it in the opened browser window.")
    print("2) Then come back here and press Enter.")
    print("Type 's' + Enter to SKIP this URL, or 'q' + Enter to QUIT.")
    print("=" * 72)

    while True:
        cmd = input("CAPTCHA> ").strip().lower()
        if cmd == "q":
            return "quit"
        if cmd == "s":
            return "skip"
        if not detect_captcha_or_block(page):
            if debug:
                print("DEBUG: CAPTCHA seems cleared, continuing.")
            return "continue"
        print("Still on CAPTCHA page. Solve it in browser, then press Enter again.")
        time.sleep(0.5)


def load_urls(path: Path) -> List[str]:
    if not path.exists():
        raise FileNotFoundError(f"Не найден {path}")
    return [
        l.strip()
        for l in path.read_text(encoding="utf-8").splitlines()
        if l.strip() and not l.strip().startswith("#")
    ]


def load_seen_keys(out_path: Path) -> set:
    if not out_path.exists():
        return set()
    seen = set()
    with out_path.open("r", encoding="utf-8") as f:
        for line in f:
            try:
                obj = json.loads(line)
                k = obj.get("review_key")
                if k:
                    seen.add(k)
            except Exception:
                pass
    return seen


def stream_scroll_collect(
    page,
    out_f,
    seen_keys: set,
    scroll_sel: str,
    card_sel: str,
    org_id: str,
    restaurant_name: str,
    source_url: str,
    max_rounds: int,
    delay_ms: int,
    step_ratio: float,
    no_progress_limit: int,
    max_reviews: int,
    debug: bool,
    headful: bool,
) -> int:
    """
    The critical part: expand "Ещё" reliably, then read the text.

    Why your previous streaming version lost text:
    - "Ещё" is often a <span> or <a>, not a button.
    - The "Ещё" word is often OUTSIDE .business-review-view__text innerText,
      so you only saw trailing "…" and thought it was the full text.
    """

    container = page.locator(scroll_sel)

    max_pos_parsed = 0
    no_progress = 0
    total_written = 0

    expanded_clicks_total = 0
    still_ellipsis_new_cards = 0

    js_extract_new = r"""
    async (els, maxPos) => {
        const expandTexts = new Set(["Показать ещё","Читать полностью","Показать полностью","Ещё"]);

        const norm = (s) => (s || "").replace(/\s+/g, " ").trim();

        const isEllipsisEnd = (s) => {
            const t = (s || "").trim();
            return t.endsWith("…") || t.endsWith("...");
        };

        function clickExpandWithin(card) {
            let clicks = 0;
            const cands = card.querySelectorAll("button,[role='button'],a,span");
            for (const el of cands) {
                const t = (el.textContent || "").trim();
                if (!expandTexts.has(t)) continue;

                const aria = (el.getAttribute("aria-label") || "").toLowerCase();
                if (aria.includes("ответ")) continue;

                const cls = ((el.className || "") + " " + (el.parentElement?.className || "")).toLowerCase();
                if (cls.includes("answer") || cls.includes("reply") || cls.includes("comment")) continue;

                try { el.click(); clicks++; } catch(e) {}
            }
            return clicks;
        }

        function readText(card) {
            const textEl = card.querySelector(".business-review-view__text") || card.querySelector(".business-review-view__body");
            return norm(textEl?.innerText || "");
        }

        let maxSeen = maxPos;
        const items = [];

        for (const card of els) {
            const pos = parseInt(card.getAttribute("aria-posinset") || "0", 10);
            if (!pos || pos <= maxPos) continue;
            if (pos > maxSeen) maxSeen = pos;

            // 1) Expand (per-card), wait a tick, read
            let clicks = clickExpandWithin(card);
            if (clicks > 0) await new Promise(r => setTimeout(r, 70));
            let text = readText(card);

            // 2) If still suspicious, try again (React sometimes needs another tick)
            if (isEllipsisEnd(text)) {
                clicks += clickExpandWithin(card);
                if (clicks > 0) await new Promise(r => setTimeout(r, 110));
                text = readText(card);
            }

            const href = card.querySelector("a.business-review-view__link[href*='/maps/user/']")?.getAttribute("href") || "";
            const m = href.match(/\/maps\/user\/([^/?#]+)/);
            const author_id = m ? m[1] : "";

            const caption = (card.querySelector(".business-review-view__author-caption")?.innerText || "").trim();
            const rating_raw = card.querySelector("meta[itemprop='ratingValue']")?.getAttribute("content") || "";
            const date_iso = card.querySelector("meta[itemprop='datePublished']")?.getAttribute("content") || "";

            items.push({
                pos, author_id, caption, rating_raw, date_iso, text,
                expandClicks: clicks,
                stillEllipsis: isEllipsisEnd(text)
            });
        }

        return { items, maxSeen };
    }
    """

    for round_idx in range(max_rounds):
        if detect_captcha_or_block(page):
            action = handle_captcha_interactive(page, source_url, headful=headful, debug=debug)
            if action in ("quit", "skip"):
                return total_written

        try:
            res = page.eval_on_selector_all(card_sel, js_extract_new, max_pos_parsed)
        except PwTimeoutError:
            no_progress += 1
            if debug:
                print(f"DEBUG: eval timeout at round={round_idx} no_progress={no_progress}")
            if no_progress >= no_progress_limit:
                break
            scroll_real_container(container, page, step_ratio, debug=debug)
            page.wait_for_timeout(delay_ms)
            continue

        items = res.get("items", []) if isinstance(res, dict) else []
        max_seen = res.get("maxSeen", max_pos_parsed) if isinstance(res, dict) else max_pos_parsed

        new_written = 0
        for it in items:
            text = (it.get("text") or "").strip()
            if not text:
                continue

            expanded_clicks_total += int(it.get("expandClicks") or 0)
            if it.get("stillEllipsis"):
                still_ellipsis_new_cards += 1

            author_id = it.get("author_id") or ""
            rating_raw = it.get("rating_raw") or ""
            date_iso = it.get("date_iso") or ""
            date_ymd = date_iso.split("T")[0] if "T" in date_iso else date_iso

            caption, badge, level = parse_caption(it.get("caption") or "")
            rating = to_float(rating_raw)

            review_key = sha1(f"{org_id}|{author_id}|{date_iso}|{rating_raw}|{text}")
            if review_key in seen_keys:
                continue

            row = {
                "review_key": review_key,
                "org_id": org_id,
                "restaurant_name": restaurant_name,
                "author_id": author_id,
                "author_caption": caption,
                "author_badge": badge,
                "author_level": level,
                "date_iso": date_iso,
                "date": date_ymd,
                "rating_raw": rating_raw,
                "rating": rating,
                "text": text,
                "source_url": source_url,
                "scraped_at_unix": int(time.time()),
            }

            out_f.write(json.dumps(row, ensure_ascii=False) + "\n")
            new_written += 1
            total_written += 1
            seen_keys.add(review_key)

            if max_reviews > 0 and total_written >= max_reviews:
                out_f.flush()
                if debug:
                    print(f"DEBUG: expanded_clicks_total={expanded_clicks_total} stillEllipsis_newCards={still_ellipsis_new_cards}")
                return total_written

        out_f.flush()

        progressed = max_seen > max_pos_parsed
        if progressed:
            max_pos_parsed = max_seen
            no_progress = 0
        else:
            no_progress += 1

        if debug:
            print(
                f"DEBUG: round={round_idx} max_pos={max_pos_parsed} +{new_written} "
                f"total_written={total_written} no_progress={no_progress} "
                f"expanded_clicks_total={expanded_clicks_total} stillEllipsis_newCards={still_ellipsis_new_cards}"
            )

        if no_progress >= no_progress_limit:
            break

        scroll_real_container(container, page, step_ratio, debug=debug)
        page.wait_for_timeout(delay_ms)

    if debug:
        print(f"DEBUG: expanded_clicks_total={expanded_clicks_total} stillEllipsis_newCards={still_ellipsis_new_cards}")
    return total_written


def build_argparser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Fast Yandex Maps reviews scraper -> JSONL (streaming).")
    ap.add_argument("--urls", default="urls.txt", help="Path to urls.txt (one URL per line).")
    ap.add_argument("--out", default="reviews.jsonl", help="Output JSONL file (all URLs go here).")
    ap.add_argument("--headful", action="store_true", help="Run browser with UI (needed to solve CAPTCHA).")
    ap.add_argument("--debug", action="store_true", help="Print debug info.")
    ap.add_argument("--block-resources", action="store_true", help="Block images/fonts/media for speed.")
    ap.add_argument("--max-scroll-rounds", type=int, default=20000, help="Max scroll iterations per URL.")
    ap.add_argument("--scroll-delay-ms", type=int, default=250, help="Delay after each scroll step, ms.")
    ap.add_argument("--scroll-step-ratio", type=float, default=2.0, help="Scroll step = clientHeight * ratio.")
    ap.add_argument("--no-progress-limit", type=int, default=200, help="Stop after N rounds without progress.")
    ap.add_argument("--max-reviews", type=int, default=0, help="Stop after N reviews per URL (0 = unlimited).")
    ap.add_argument("--page-delay-sec", type=float, default=1.0, help="Delay between URLs, seconds.")
    return ap


def main() -> None:
    args = build_argparser().parse_args()

    urls = load_urls(Path(args.urls))
    out_path = Path(args.out)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.open("a", encoding="utf-8").close()
    seen_keys = load_seen_keys(out_path)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=(not args.headful))
        ctx = browser.new_context(locale="ru-RU")
        page = ctx.new_page()

        page.set_default_timeout(120_000)
        page.set_default_navigation_timeout(120_000)

        if args.block_resources:
            block_heavy_resources(page, True)

        with out_path.open("a", encoding="utf-8") as out_f:
            for idx, url in enumerate(urls, start=1):
                try:
                    page.goto(url, wait_until="domcontentloaded", timeout=120_000)
                    page.wait_for_timeout(1200)

                    dismiss_popups(page)

                    if detect_captcha_or_block(page):
                        action = handle_captcha_interactive(page, url, headful=args.headful, debug=args.debug)
                        if action == "quit":
                            print("Quit requested. Progress already saved.")
                            break
                        if action == "skip":
                            print(f"[{idx}/{len(urls)}] SKIP (CAPTCHA) {url}")
                            continue

                    scroll_sel = pick_first_existing(page, SCROLL_CONTAINER_CANDIDATES)
                    card_sel = pick_first_existing(page, REVIEW_CARD_CANDIDATES)

                    if args.debug:
                        print("DEBUG: chosen scroll_sel =", scroll_sel)
                        print("DEBUG: chosen card_sel   =", card_sel)
                        print("DEBUG: page url          =", page.url)

                    if not scroll_sel or not card_sel:
                        print(f"[{idx}/{len(urls)}] ERROR: container/cards not found | {url}")
                        continue

                    org_id_m = ORG_ID_RE.search(url)
                    org_id = org_id_m.group(1) if org_id_m else "unknown"

                    restaurant_name = extract_restaurant_name(page)
                    if args.debug:
                        print("DEBUG: restaurant_name   =", restaurant_name)

                    wait_reviews_ready(page, card_sel, debug=args.debug)

                    written = stream_scroll_collect(
                        page=page,
                        out_f=out_f,
                        seen_keys=seen_keys,
                        scroll_sel=scroll_sel,
                        card_sel=card_sel,
                        org_id=org_id,
                        restaurant_name=restaurant_name,
                        source_url=url,
                        max_rounds=args.max_scroll_rounds,
                        delay_ms=args.scroll_delay_ms,
                        step_ratio=args.scroll_step_ratio,
                        no_progress_limit=args.no_progress_limit,
                        max_reviews=args.max_reviews,
                        debug=args.debug,
                        headful=args.headful,
                    )

                    print(f"[{idx}/{len(urls)}] {url} -> +{written} записано")

                    time.sleep(args.page_delay_sec)

                except PwTimeoutError:
                    print(f"[{idx}/{len(urls)}] TIMEOUT: {url} (часть данных уже сохранена)")
                except KeyboardInterrupt:
                    print("\nInterrupted by user. Progress already saved.")
                    break
                except Exception as e:
                    print(f"[{idx}/{len(urls)}] ERROR: {e} | {url}")

        try:
            ctx.close()
            browser.close()
        except Exception:
            pass

    print(f"Done. Output: {out_path.resolve()}")


if __name__ == "__main__":
    main()
