"""Google Maps scraper via Playwright Chromium — "Ferrari" stealth engine.

Ported from the `mega scrapper` legacy project which achieved:
  - Zero bot detection (Chromium with real Chrome UA, SLOW_MO humanization)
  - Fast extraction (query_selector_all + networkidle vs locator + domcontentloaded)
  - Context isolation (fresh BrowserContext per session, no fingerprint bleed)

Stealth stack:
  - Playwright **Chromium** (not Firefox — better Google Maps JS compat)
  - Chrome 124 User-Agent string
  - SLOW_MO = 300ms (built-in inter-action humanization)
  - networkidle wait strategy (ensures all XHR/fetch complete before extraction)
  - query_selector_all for direct DOM access (faster than Playwright Locators)
  - 15s hard timeout cap per entity

Key fix (v3): Switched from Firefox+Locators to Chromium+query_selector_all.
This matches the engine that "ran incredibly fast" in the mega scrapper project.

Usage (from discovery.py):
    async with PlaywrightMapsScraper() as scraper:
        result = await scraper.search("ABDEL KHAMASSI", "66", siren="400643128")
        phone = result.get("phone")         # e.g. "04 68 38 59 59"
        website = result.get("website")     # e.g. "https://example.com"
        address = result.get("address")     # e.g. "12 Rue de la Paix, 75002 Paris"
        rating  = result.get("rating")      # e.g. 4.5
        reviews = result.get("review_count") # e.g. 42
"""

from __future__ import annotations

import asyncio
import json
import random
import re
from typing import Any

import structlog

from fortress.config.sector_relevance import is_irrelevant_name
from fortress.config.settings import settings

log = structlog.get_logger(__name__)

# ── Stealth config (ported from mega scrapper/settings.py) ────────────────
_STEALTH_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
_SLOW_MO = 50           # ms between Playwright actions (was 300; reduced for
_HARD_TIMEOUT = 15.0    # seconds — absolute max per entity search
_PAGE_TIMEOUT = 10000   # ms — max for page.goto / networkidle
_SELECTOR_TIMEOUT = 5000  # ms — max wait for DOM element

# ── Legal forms to strip during name comparison ───────────────────────────
_LEGAL_FORMS = frozenset({
    "sarl", "sas", "sasu", "eurl", "sa", "sci", "snc",
    "scs", "sca", "ei", "eirl", "asso", "association",
    "et", "cie", "fils", "freres", "groupe", "holding",
})


def _name_similarity(maps_name: str, denomination: str) -> float:
    """Quick name similarity score (0.0 to 1.0) for pre-click filtering.

    Normalizes both names (lowercase, strip legal forms, accents),
    then checks containment and token overlap.
    """
    if not maps_name or not denomination:
        return 0.0

    def _tokens(name: str) -> list[str]:
        import unicodedata
        nfkd = unicodedata.normalize('NFKD', name.lower())
        ascii_name = ''.join(c for c in nfkd if not unicodedata.combining(c))
        t = re.sub(r'[^a-z0-9\s]', '', ascii_name).split()
        filtered = [w for w in t if w not in _LEGAL_FORMS]
        if not filtered:
            return []
        # Keep single-char tokens for acronym names (>50% are single chars)
        single_chars = sum(1 for w in filtered if len(w) == 1)
        if single_chars > len(filtered) / 2:
            return filtered
        return [w for w in filtered if len(w) > 1]

    m_tok = _tokens(maps_name)
    d_tok = _tokens(denomination)
    if not m_tok or not d_tok:
        return 0.0

    m_joined = " ".join(m_tok)
    d_joined = " ".join(d_tok)

    # Full containment → strong match
    if d_joined in m_joined or m_joined in d_joined:
        return 1.0

    # Token overlap ratio
    overlap = sum(1 for t in d_tok if t in m_tok)
    return overlap / max(len(d_tok), 1)

# ── Phone patterns ────────────────────────────────────────────────────────
_TEL_LINK_RE = re.compile(r'href="tel:([^"]+)"')
_DATA_ITEM_PHONE_RE = re.compile(r'data-item-id="phone:tel:([^"]+)"')
_FRENCH_PHONE_RE = re.compile(
    r'(?:\+33|0)\s*[1-9](?:[\s.\-]?[0-9]){8}',
)
_SCAFFOLD_PHONES = {"0999999977", "0261676083", "0261676921"}


def _clean_phone(raw: str) -> str | None:
    """Clean and validate a phone number. Returns None if fake/scaffold."""
    cleaned = raw.strip().replace(" ", "").replace(".", "").replace("-", "")
    if cleaned in _SCAFFOLD_PHONES:
        return None
    if not (cleaned.startswith("0") or cleaned.startswith("+33")):
        return None
    digits_only = re.sub(r'[^0-9]', '', cleaned)
    if len(digits_only) < 9 or len(digits_only) > 13:
        return None
    return raw.strip()


class PlaywrightMapsScraper:
    """Async Chromium stealth scraper for Google Maps business listings.

    Engine: Playwright Chromium (ported from mega scrapper).
    Thread safety: protected by asyncio.Lock() — one search at a time.
    Browser lifecycle: call start()/close(), or use as async context manager.
    """

    def __init__(self) -> None:
        self._playwright: Any = None
        self._browser: Any = None
        self._context: Any = None
        self._page: Any = None
        self._lock = asyncio.Lock()
        self._consent_done = False
        self._search_count = 0  # Track searches to force reload

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Start Playwright Chromium with stealth configuration.

        Uses the proven mega scrapper config:
          - Chromium engine (not Firefox)
          - Chrome 124 User-Agent
          - SLOW_MO = 300ms for human-like pacing
          - Context isolation with fr-FR locale
        """
        from playwright.async_api import async_playwright

        self._playwright = await async_playwright().start()

        try:
            # ── Chromium stealth launch (mega scrapper engine) ────────
            self._browser = await self._playwright.chromium.launch(
                headless=True,
                slow_mo=_SLOW_MO,
                args=[
                    "--no-sandbox",
                    "--disable-gpu",
                    "--disable-dev-shm-usage",
                    "--disable-software-rasterizer",
                    "--disable-extensions",
                    "--disable-background-networking",
                    "--disable-blink-features=AutomationControlled",
                    # ── Memory reduction flags (Render-safe) ───────────
                    "--disable-renderer-backgrounding",
                    "--disable-backgrounding-occluded-windows",
                    "--disable-ipc-flooding-protection",
                    "--disable-features=TranslateUI,BlinkGenPropertyTrees",
                    "--disable-hang-monitor",
                    "--disable-component-update",
                    "--disable-default-apps",
                    "--disable-domain-reliability",
                    "--metrics-recording-only",
                    "--mute-audio",
                    "--no-first-run",
                ],
            )

            # ── Context isolation (fresh fingerprint per session) ─────
            self._context = await self._browser.new_context(
                user_agent=_STEALTH_UA,
                viewport={"width": 1280, "height": 900},
                locale="fr-FR",
                timezone_id="Europe/Paris",
            )
            await self._setup_page()

            # Verify search box is available
            search_box = await self._page.query_selector(
                'input[name="q"], #searchboxinput'
            )
            if search_box:
                log.info("maps_scraper.browser_started", engine="chromium_stealth",
                         consent_done=self._consent_done)
            else:
                log.warning("maps_scraper.search_box_not_found_at_start")

        except Exception as exc:
            if self._playwright:
                await self._playwright.stop()
                self._playwright = None
            raise RuntimeError(f"Chromium stealth failed to start: {exc}") from exc

    async def _setup_page(self) -> None:
        """Create a fresh Page with stealth config and navigate to Maps.

        Called once during start() and again every 50 searches to flush
        Chromium's DOM memory without pausing the Python event loop.
        The BrowserContext is reused (preserves cookies/session).
        """
        self._page = await self._context.new_page()
        self._page.set_default_timeout(_SELECTOR_TIMEOUT)

        # ── Remove webdriver flag (anti-detection) ────────────────
        await self._page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
        """)

        # ── Block heavy resources (images, fonts, media) ──────────
        # We only need DOM text for data extraction. Blocking these
        # cuts ~40% of network traffic and speeds up page loads.
        # NOTE: stylesheets MUST remain — Google Maps uses CSS z-index
        # to position the search box above the canvas. Without CSS,
        # the canvas intercepts all clicks.
        _BLOCKED_TYPES = frozenset({"image", "media", "font", "ping"})
        _BLOCKED_URL_PARTS = (
            "google-analytics.com",
            "googletagmanager.com",
            "doubleclick.net",
            "googlesyndication.com",
            "google.com/pagead",
            "cbk0.google.com",           # Street View tiles
            "streetviewpixels",           # Street View pixels
        )

        async def _intercept(route):
            url = route.request.url
            if route.request.resource_type in _BLOCKED_TYPES:
                await route.abort()
            elif any(p in url for p in _BLOCKED_URL_PARTS):
                await route.abort()
            else:
                await route.continue_()

        await self._page.route("**/*", _intercept)

        # ── Navigate to Maps homepage and handle consent ──────────
        await self._page.goto(
            "https://www.google.com/maps?hl=fr",
            wait_until="domcontentloaded",
            timeout=_PAGE_TIMEOUT,
        )
        await self._page.wait_for_timeout(800)
        await self._handle_consent()

        # Verify we're on Maps after consent
        current_url = self._page.url
        if "maps" not in current_url:
            log.info("maps_scraper.re_navigating_after_consent")
            await self._page.goto(
                "https://www.google.com/maps?hl=fr",
                wait_until="domcontentloaded",
                timeout=_PAGE_TIMEOUT,
            )
            await self._page.wait_for_timeout(2000)

    async def close(self) -> None:
        """Stop Chromium and release Playwright resources."""
        for resource in (self._page, self._context, self._browser):
            try:
                if resource:
                    await resource.close()
            except Exception as exc:
                log.debug("maps_scraper.close_resource_error", error=str(exc))
        try:
            if self._playwright:
                await self._playwright.stop()
        except Exception as exc:
            log.debug("maps_scraper.playwright_stop_error", error=str(exc))
        self._page = None
        self._context = None
        self._browser = None
        self._playwright = None
        self._consent_done = False
        log.info("maps_scraper.browser_stopped", engine="chromium_stealth")

    async def __aenter__(self) -> "PlaywrightMapsScraper":
        await self.start()
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.close()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def search(
        self,
        denomination: str,
        department: str,
        *,
        siren: str = "",
        query_hint: str = "",
    ) -> dict[str, Any]:
        """Search Google Maps for a company and return contact data.

        Only one search runs at a time (protected by asyncio.Lock).
        Returns an empty dict on timeout or any error.
        """
        if self._page is None:
            raise RuntimeError(
                "PlaywrightMapsScraper not started — use 'async with'"
            )

        async with self._lock:
            try:
                self._search_count += 1
                if self._search_count % 10 == 0:
                    log.info("maps_scraper.memory_flush",
                             count=self._search_count, method="page_cycle")
                    await self._page.close()
                    await self._setup_page()

                return await asyncio.wait_for(
                    self._do_search(denomination, department, siren, query_hint),
                    timeout=_HARD_TIMEOUT,
                )
            except asyncio.TimeoutError:
                log.warning(
                    "maps_scraper.timeout",
                    siren=siren,
                    denomination=denomination,
                    timeout=_HARD_TIMEOUT,
                )
                return {}
            except Exception as exc:
                log.warning(
                    "maps_scraper.error",
                    siren=siren,
                    denomination=denomination,
                    error=str(exc),
                )
                return {}

    # ------------------------------------------------------------------
    # Maps-first discovery — search_all
    # ------------------------------------------------------------------

    async def search_all(
        self,
        query: str,
        *,
        on_result: Any = None,
        dept_code: str | None = None,
        max_results: int = 0,
        sector_word: str = "",
        should_skip: Any = None,
    ) -> list[dict[str, Any]]:
        """Search Google Maps with a generic query and extract ALL results.

        Unlike search() which looks for a specific company, this discovers
        all businesses matching a generic query like "camping Perpignan".

        Args:
            query:     Generic search term, e.g. "camping Perpignan" or
                       "transport logistique 66".
            on_result: Optional async callback(result_dict) called after
                       each business is extracted. Enables real-time DB
                       persistence. If the callback returns False, the
                       scraper stops extracting more cards (early stop).

        Returns:
            List of dicts, each with: maps_name, phone, website, address,
            rating, review_count, maps_url.  Duplicates are removed by
            maps_name + address.
        """
        if self._page is None:
            raise RuntimeError(
                "PlaywrightMapsScraper not started — call start() first"
            )

        async with self._lock:
            try:
                return await asyncio.wait_for(
                    self._do_search_all(query, on_result, max_results=max_results, dept_code=dept_code, sector_word=sector_word, should_skip=should_skip),
                    timeout=600.0,  # 10 min max per search_all query
                )
            except asyncio.TimeoutError:
                log.warning(
                    "maps_discovery.timeout",
                    query=query,
                    timeout=600,
                )
                return []
            except Exception as exc:
                log.error(
                    "maps_discovery.error",
                    query=query,
                    error=str(exc),
                )
                return []

    DEPT_COORDINATES: dict[str, str] = {
        "01": "46.2,5.3,10z", "02": "49.5,3.6,10z", "03": "46.3,3.2,10z",
        "04": "44.1,6.2,10z", "05": "44.7,6.3,10z", "06": "43.8,7.2,10z",
        "07": "44.7,4.6,10z", "08": "49.6,4.6,10z", "09": "42.9,1.5,10z",
        "10": "48.3,4.1,10z", "11": "43.1,2.4,10z", "12": "44.3,2.6,9z",
        "13": "43.5,5.1,10z", "14": "49.1,-0.4,10z", "15": "45.0,2.7,10z",
        "16": "45.7,0.2,10z", "17": "45.9,-0.8,10z", "18": "47.0,2.5,10z",
        "19": "45.3,1.8,10z", "2A": "41.9,9.0,10z", "2B": "42.4,9.2,10z",
        "21": "47.3,4.7,9z", "22": "48.5,-3.0,10z", "23": "46.1,2.1,10z",
        "24": "45.0,0.7,9z", "25": "47.2,6.4,10z", "26": "44.7,5.2,10z",
        "27": "49.1,1.2,10z", "28": "48.3,1.5,10z", "29": "48.4,-4.2,10z",
        "30": "44.0,4.1,10z", "31": "43.3,1.2,10z", "32": "43.7,0.6,10z",
        "33": "44.8,-0.6,9z", "34": "43.6,3.5,10z", "35": "48.1,-1.7,10z",
        "36": "46.8,1.6,10z", "37": "47.3,0.7,10z", "38": "45.2,5.7,9z",
        "39": "46.7,5.7,10z", "40": "43.9,-0.8,9z", "41": "47.6,1.3,10z",
        "42": "45.7,4.2,10z", "43": "45.1,3.7,10z", "44": "47.3,-1.8,10z",
        "45": "47.9,2.2,10z", "46": "44.6,1.7,10z", "47": "44.3,0.5,10z",
        "48": "44.5,3.5,10z", "49": "47.4,-0.6,10z", "50": "48.9,-1.3,10z",
        "51": "48.9,4.0,9z", "52": "48.1,5.3,10z", "53": "48.1,-0.8,10z",
        "54": "48.7,6.2,10z", "55": "49.0,5.4,10z", "56": "47.7,-2.8,10z",
        "57": "49.0,6.7,10z", "58": "47.1,3.5,10z", "59": "50.4,3.2,10z",
        "60": "49.4,2.5,10z", "61": "48.6,0.1,10z", "62": "50.5,2.3,9z",
        "63": "45.7,3.1,10z", "64": "43.3,-0.8,10z", "65": "43.0,0.2,10z",
        "66": "42.6,2.5,10z", "67": "48.6,7.5,10z", "68": "47.9,7.2,10z",
        "69": "45.8,4.7,10z", "70": "47.6,6.2,10z", "71": "46.6,4.5,9z",
        "72": "47.9,0.2,10z", "73": "45.5,6.5,10z", "74": "46.0,6.3,10z",
        "75": "48.86,2.35,12z", "76": "49.6,1.1,10z", "77": "48.6,2.9,10z",
        "78": "48.8,1.9,10z", "79": "46.5,-0.3,10z", "80": "49.9,2.3,10z",
        "81": "43.8,2.2,10z", "82": "44.0,1.3,10z", "83": "43.5,6.3,10z",
        "84": "44.0,5.1,10z", "85": "46.7,-1.3,10z", "86": "46.6,0.5,10z",
        "87": "45.9,1.3,10z", "88": "48.2,6.5,10z", "89": "47.8,3.6,10z",
        "90": "47.6,6.9,11z", "91": "48.5,2.2,10z", "92": "48.84,2.25,12z",
        "93": "48.91,2.48,12z", "94": "48.78,2.47,12z", "95": "49.1,2.2,10z",
        "971": "16.2,-61.5,9z", "972": "14.6,-61.0,9z", "973": "3.9,-53.2,7z",
        "974": "-21.1,55.5,9z", "975": "46.8,-56.2,10z", "976": "-12.8,45.2,10z",
    }

    async def _do_search_all(
        self,
        query: str,
        on_result: Any = None,
        max_results: int = 0,
        *,
        dept_code: str | None = None,
        sector_word: str = "",
        should_skip: Any = None,
    ) -> list[dict[str, Any]]:
        """Internal: perform generic Maps search and extract all results.

        Uses direct URL navigation (proven approach from local headed test)
        instead of search box interaction which fails on Render.
        max_results: stop after collecting this many results (0=unlimited).
        """
        import urllib.parse
        from pathlib import Path

        page = self._page
        results: list[dict[str, Any]] = []
        seen_keys: set[str] = set()  # Dedup by name+address

        # ── Step 1: Navigate directly to search URL ──────────────────
        # gl=fr restricts results to France.
        # If a department code is provided and known, zoom to that dept center.
        # Otherwise fall back to France-wide center @46.6,2.3,6z.
        france_query = query if "france" in query.lower() else f"{query}, France"
        map_center = self.DEPT_COORDINATES.get(dept_code, "46.6,2.3,6z") if dept_code else "46.6,2.3,6z"
        search_url = (
            f"https://www.google.com/maps/search/"
            f"{urllib.parse.quote_plus(france_query)}"
            f"/@{map_center}"
            f"?hl=fr&gl=fr"
        )
        log.info("maps_discovery.navigating", query=query, url=search_url)

        await page.goto(
            search_url,
            wait_until="domcontentloaded",
            timeout=30000,
        )
        await page.wait_for_timeout(3000)

        # ── Step 2: Handle consent dialog (may appear on Render) ───────
        _CONSENT_SELECTORS = [
            'button:has-text("Tout accepter")',
            'button:has-text("Accept all")',
            'form[action*="consent"] button',
        ]
        for sel in _CONSENT_SELECTORS:
            try:
                btn = await page.query_selector(sel)
                if btn:
                    log.info("maps_discovery.consent_found", selector=sel)
                    await btn.click()
                    await page.wait_for_timeout(2000)
                    # Re-navigate after consent redirect
                    await page.goto(
                        search_url,
                        wait_until="domcontentloaded",
                        timeout=30000,
                    )
                    await page.wait_for_timeout(3000)
                    break
            except Exception:
                continue

        # ── Step 3: CAPTCHA / error detection ──────────────────────────
        current_url = page.url
        if "sorry" in current_url or "recaptcha" in current_url:
            log.warning("maps_discovery.captcha_detected", query=query, url=current_url)
            # Save diagnostic screenshot
            try:
                debug_dir = Path("data/logs")
                debug_dir.mkdir(parents=True, exist_ok=True)
                safe_name = query.replace(" ", "_")[:30]
                await page.screenshot(
                    path=str(debug_dir / f"maps_debug_{safe_name}.png"),
                )
                log.info("maps_discovery.debug_screenshot_saved", query=query)
            except Exception:
                pass
            return []

        # ── Step 4: Wait for result cards ──────────────────────────────
        try:
            await page.wait_for_selector('a.hfpxzc', timeout=10000)
        except Exception:
            log.info("maps_discovery.no_results", query=query)
            # Save diagnostic screenshot for debugging
            try:
                debug_dir = Path("data/logs")
                debug_dir.mkdir(parents=True, exist_ok=True)
                safe_name = query.replace(" ", "_")[:30]
                await page.screenshot(
                    path=str(debug_dir / f"maps_debug_{safe_name}.png"),
                )
                log.info(
                    "maps_discovery.debug_screenshot_saved",
                    query=query,
                    current_url=page.url,
                )
            except Exception:
                pass
            # Maybe it landed on a single business panel — try extracting
            phone_btn = await page.query_selector('button[data-item-id^="phone"]')
            if phone_btn:
                data = await self._extract_from_page(query, "", "discovery")
                if data.get("maps_name"):
                    data["search_query"] = query
                    results.append(data)
                    if on_result:
                        try:
                            await on_result(data)
                        except Exception:
                            pass
            return results

        # ── Step 4: Scroll to load ALL results ────────────────────────
        # Google Maps lazy-loads results as you scroll the sidebar panel.
        # The scrollable container is div[role="feed"] or the results div.
        _FEED_SELECTOR = 'div[role="feed"]'
        feed = await page.query_selector(_FEED_SELECTOR)
        if not feed:
            # Fallback: find the scrollable results container
            feed = await page.query_selector('div.m6QErb.DxyBCb')

        if feed:
            prev_count = 0
            stale_rounds = 0
            max_scrolls = 25  # Safety cap: max 25 scroll rounds

            for scroll_round in range(max_scrolls):
                # Count current results
                current_results = await page.query_selector_all('a.hfpxzc')
                current_count = len(current_results)

                # Stop scrolling if we have enough cards
                if max_results > 0 and current_count >= max_results:
                    log.info(
                        "maps_discovery.scroll_enough",
                        loaded=current_count,
                        max_results=max_results,
                        query=query,
                    )
                    break

                log.debug(
                    "maps_discovery.scroll",
                    round=scroll_round + 1,
                    results_loaded=current_count,
                    query=query,
                )

                if current_count == prev_count:
                    stale_rounds += 1
                else:
                    stale_rounds = 0
                    prev_count = current_count

                # Gated end-of-list sentinel: only trust the marker after at
                # least 2 stale rounds. Google Maps renders this footer as a
                # placeholder during lazy-load transitions, so checking it
                # before any stall means we exit prematurely (the Apr-8 bug
                # that lost ~48% of cards). After 2+ stale rounds it is a
                # legitimate "you've seen all results" signal — small-city
                # queries can still fast-exit via this path.
                if stale_rounds >= 2:
                    end_marker = await page.query_selector(
                        'span.HlvSq, '       # "Vous avez vu tous les résultats"
                        'p.fontBodyMedium:has-text("résultat")'
                    )
                    if end_marker:
                        try:
                            end_text = await end_marker.inner_text()
                            if "tous les" in end_text.lower() or "no more" in end_text.lower():
                                log.debug("maps_discovery.end_of_list", query=query)
                                break
                        except Exception:
                            pass

                # Stall recovery: after 6 consecutive rounds with no new cards,
                # do ONE recovery pass before giving up. Google Maps' lazy-loader
                # is edge-triggered: scrolling up then back down re-activates the
                # "near bottom" observer that fetches the next batch of ~10 cards.
                # On slow Render/Neon hops, this recovers cards that the plain
                # stall break was missing (~48% data loss measured on Apr 8).
                if stale_rounds >= 6:
                    log.debug(
                        "maps_discovery.stall_detected",
                        query=query,
                        count=current_count,
                        round=scroll_round + 1,
                    )
                    try:
                        await feed.evaluate(
                            "el => { el.scrollTop = Math.max(0, el.scrollTop - 500); }"
                        )
                        await page.wait_for_timeout(500)
                        await feed.evaluate(
                            "el => el.scrollTo(0, el.scrollHeight)"
                        )
                        await page.wait_for_timeout(3000)
                    except Exception as exc:
                        log.debug("maps_discovery.recovery_scroll_error", error=str(exc))

                    recovered_results = await page.query_selector_all('a.hfpxzc')
                    recovered_count = len(recovered_results)
                    if recovered_count > current_count:
                        log.info(
                            "maps_discovery.stall_recovered",
                            query=query,
                            before=current_count,
                            after=recovered_count,
                        )
                        stale_rounds = 0
                        prev_count = recovered_count
                        # Fall through to normal scroll + wait below — gives
                        # the lazy-loader more time to render the batch we
                        # just triggered before the next count.
                    else:
                        log.debug(
                            "maps_discovery.stall_confirmed",
                            query=query,
                            final_count=current_count,
                        )
                        break

                # Scroll the feed container down
                await feed.evaluate(
                    "el => el.scrollTo(0, el.scrollHeight)"
                )
                await page.wait_for_timeout(random.randint(2000, 3000))

        # ── Step 5: Collect all card hrefs FIRST ────────────────────────
        # Each card is an <a> with an href to the business page.
        # We collect all hrefs upfront so we can navigate to each one
        # directly, getting a FULL fresh page load per business.
        all_cards = await page.query_selector_all('a.hfpxzc')
        card_data: list[tuple[str, str, str]] = []  # (href, label, category)
        for card in all_cards:
            href = await card.get_attribute("href") or ""
            label = (await card.get_attribute("aria-label")) or ""
            # Extract category from card's W4Efsd spans
            card_cat = ""
            try:
                card_cat = await page.evaluate("""(cardEl) => {
                    const container = cardEl.closest('[jsaction]')?.parentElement || cardEl.parentElement;
                    if (!container) return '';
                    const label = cardEl.getAttribute('aria-label') || '';
                    const spans = container.querySelectorAll('.W4Efsd span');
                    for (const sp of spans) {
                        const t = sp.textContent.trim();
                        if (!t || t.length < 3 || t.length > 50) continue;
                        if (/^[0-9,.(]/.test(t)) continue;
                        if (/[\\u20AA\\u20AC$]/.test(t)) continue;
                        if (t === '\\u00b7' || t.startsWith('\\u00b7')) continue;
                        if (t === label || label.startsWith(t)) continue;
                        let cat = t;
                        while (cat.endsWith('\\u00b7') || cat.endsWith(' ')) cat = cat.slice(0, -1);
                        if (cat) return cat;
                    }
                    return '';
                }""", card) or ""
            except Exception:
                pass
            if href:
                card_data.append((href, label, card_cat))

        # Truncate to max_results to avoid processing excess cards
        if max_results > 0 and len(card_data) > max_results:
            card_data = card_data[:max_results]

        total_cards = len(card_data)
        log.info(
            "maps_discovery.cards_found",
            total=total_cards,
            query=query,
        )

        # Save the search results URL so we can return to it
        search_url = page.url

        # ── Step 6: Navigate to each business → extract → go back ──────
        for idx, (card_href, card_label, card_category) in enumerate(card_data):
            try:
                # ── FIX C: Skip "Sponsorisé" results ──────────────────
                if "sponsoris" in card_label.lower() or "\ue5d4" in card_label:
                    log.debug(
                        "maps_discovery.sponsored_skipped",
                        name=card_label,
                    )
                    continue

                # ── Name-based pre-filter (before expensive page nav) ──
                if sector_word and card_label:
                    if is_irrelevant_name(sector_word, card_label):
                        log.info(
                            "maps_discovery.name_filtered",
                            name=card_label,
                            sector=sector_word,
                        )
                        continue

                # ── Pre-dedup: skip cards already in workspace ──
                if should_skip and card_label and should_skip(card_label):
                    log.debug(
                        "maps_discovery.pre_dedup_skip",
                        name=card_label,
                    )
                    continue

                # Check if we've collected enough results
                if max_results > 0 and len(results) >= max_results:
                    log.info(
                        "maps_discovery.batch_size_reached",
                        collected=len(results),
                        target=max_results,
                        query=query,
                    )
                    break

                # Navigate directly to the business page (full fresh load)
                await page.goto(card_href, wait_until="domcontentloaded", timeout=15000)

                # Wait for the business panel h1 to appear
                try:
                    await page.wait_for_selector('h1.DUwDvf', timeout=5000)
                except Exception:
                    await page.wait_for_timeout(1000)

                # Small settle for all data elements to load
                # NOTE: 1500ms needed because Maps JS renders website/phone
                # buttons AFTER domcontentloaded, especially for hospitality
                # businesses that show availability widgets first.
                await page.wait_for_timeout(1500)

                # Extract all data from the fully loaded business page
                data = await self._extract_from_page(card_label, "", "discovery")
                data["search_query"] = query

                # Card category from list view is more reliable than detail page
                if card_category:
                    data["category"] = card_category

                extracted_name = data.get("maps_name") or card_label

                # Dedup check: name + address
                dedup_key = (
                    extracted_name.lower()
                    + "|"
                    + (data.get("address") or "").lower()
                )
                if dedup_key not in seen_keys:
                    seen_keys.add(dedup_key)
                    results.append(data)

                    log.info(
                        "maps_discovery.extracted",
                        idx=idx + 1,
                        total=total_cards,
                        name=extracted_name,
                        has_phone=bool(data.get("phone")),
                        has_website=bool(data.get("website")),
                        query=query,
                    )

                    # Real-time callback for per-entity persistence
                    if on_result:
                        try:
                            cb_result = await on_result(data)
                            if cb_result is False:
                                log.info(
                                    "maps_discovery.early_stop",
                                    reason="callback_signal",
                                    extracted=len(results),
                                    total_cards=len(card_data),
                                    query=query,
                                )
                                break
                        except Exception as cb_exc:
                            log.warning(
                                "maps_discovery.on_result_error",
                                error=str(cb_exc),
                            )
                else:
                    log.debug(
                        "maps_discovery.duplicate_skipped",
                        name=extracted_name,
                    )

                # Navigate back to the search results
                await page.goto(search_url, wait_until="domcontentloaded", timeout=15000)
                await page.wait_for_timeout(500)

                # Memory flush every 20 results
                if (idx + 1) % 20 == 0:
                    log.info(
                        "maps_discovery.progress",
                        processed=idx + 1,
                        total=total_cards,
                        found=len(results),
                        query=query,
                    )

            except Exception as exc:
                log.warning(
                    "maps_discovery.card_error",
                    idx=idx,
                    error=str(exc),
                    query=query,
                )
                # Try to recover back to the result list
                try:
                    await page.keyboard.press("Escape")
                    await page.wait_for_timeout(500)
                except Exception:
                    pass
                continue

        log.info(
            "maps_discovery.search_complete",
            query=query,
            total_cards=total_cards,
            unique_results=len(results),
            with_phone=sum(1 for r in results if r.get("phone")),
            with_website=sum(1 for r in results if r.get("website")),
        )

        return results

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _handle_consent(self) -> None:
        """Click through Google's GDPR consent banner."""
        if self._consent_done:
            return

        page = self._page
        current_url = page.url
        page_html = await page.content()
        has_consent = (
            "consent.google" in current_url
            or "Tout accepter" in page_html
            or "Accept all" in page_html
            or "Avant d" in page_html
        )

        if not has_consent:
            self._consent_done = True
            log.debug("maps_scraper.no_consent_needed")
            return

        # ── Direct DOM click (mega scrapper style — query_selector) ───
        consent_selectors = [
            'button:has-text("Tout accepter")',
            'button:has-text("Accept all")',
            '#L2AGLb',
            'button.tHlp8d',
            'form button:last-of-type',
        ]

        for selector in consent_selectors:
            try:
                btn = await page.query_selector(selector)
                if btn:
                    await btn.click()
                    await page.wait_for_timeout(2000)
                    self._consent_done = True
                    log.info("maps_scraper.consent_accepted", selector=selector)
                    return
            except Exception:
                continue

        # Fallback: try Locator API for :has-text pseudo
        for text in ["Tout accepter", "Accept all"]:
            try:
                loc = page.locator(f'button:has-text("{text}")')
                if await loc.count() > 0:
                    await loc.first.click()
                    await page.wait_for_timeout(2000)
                    self._consent_done = True
                    log.info("maps_scraper.consent_accepted_locator", text=text)
                    return
            except Exception:
                continue

        log.warning("maps_scraper.consent_button_not_found")
        self._consent_done = True  # Don't retry forever

    async def _do_search(
        self, denomination: str, department: str, siren: str,
        query_hint: str = "",
    ) -> dict[str, Any]:
        """Search Maps and extract data — mega scrapper strategy.

        Strategy (v3 — Chromium stealth):
          1. Type query into search box (natural input, not URL nav)
          2. Press Enter, wait for networkidle
          3. CAPTCHA fast-fail check
          4. Detect: business panel vs result list vs geographic entity
          5. Extract via query_selector_all (direct DOM, no Locator overhead)
        """
        # Query construction: denomination + location (city/postal/dept)
        # NOTE: Adding domain hints (e.g. "camping") or "France" was tested and
        # caused 0% hit rates — Maps interprets "CAMPING BOUIX ... camping France"
        # as a category search (all campings in France) instead of the specific business.
        query = f"{denomination} {department}"
        page = self._page

        # ── Step 1: Focus and type into search box (natural) ──────────
        search_box = await page.query_selector('input[name="q"], #searchboxinput')
        if search_box:
            try:
                await search_box.click()
                await page.wait_for_timeout(200)
                # Select all + type (replace previous query)
                await page.keyboard.press("Control+a")
                await page.keyboard.press("Meta+a")  # macOS
                await search_box.fill(query)
                await page.wait_for_timeout(300)
            except Exception as exc:
                log.debug("maps_scraper.search_box_failed", error=str(exc), siren=siren)
                # Fallback: URL navigation
                import urllib.parse
                url = f"https://www.google.com/maps/search/{urllib.parse.quote_plus(query)}?hl=fr"
                await page.goto(url, wait_until="domcontentloaded", timeout=_PAGE_TIMEOUT)
                await page.wait_for_timeout(700)
                return await self._extract_from_page(denomination, department, siren)
        else:
            # No search box — direct URL navigation
            import urllib.parse
            url = f"https://www.google.com/maps/search/{urllib.parse.quote_plus(query)}?hl=fr"
            await page.goto(url, wait_until="domcontentloaded", timeout=_PAGE_TIMEOUT)
            await page.wait_for_timeout(700)
            return await self._extract_from_page(denomination, department, siren)

        # ── Step 2: Submit search ─────────────────────────────────────
        await page.keyboard.press("Enter")
        await page.wait_for_timeout(700)

        # ── Step 3: CAPTCHA fast-fail ─────────────────────────────────
        current_url = page.url
        if "sorry" in current_url or "recaptcha" in current_url:
            log.warning(
                "maps_scraper.captcha_detected",
                siren=siren, denomination=denomination, url=current_url,
            )
            return {}

        # ── Step 3b: Wait for Maps JS to render (smart wait) ─────────
        # Google Maps is a heavy SPA — the business panel with phone/website
        # buttons takes 1-3s AFTER the load event to render, especially for
        # hospitality businesses (campings, hotels) that show a hotel preview
        # carousel before the contact info. We wait for actual DOM elements.
        try:
            await page.wait_for_load_state("load", timeout=5000)
        except Exception as exc:
            log.debug("maps_scraper.load_state_timeout", error=str(exc), siren=siren)
        # Smart wait: look for ANY sign of a rendered business panel or result list
        _PANEL_SELECTOR = (
            'button[data-item-id^="phone"], '  # Phone button (direct panel)
            'a.hfpxzc, '                        # Result list item
            'h1.DUwDvf, '                       # Business name heading
            'button[data-item-id="address"]'    # Address button (panel loaded)
        )
        try:
            await page.wait_for_selector(_PANEL_SELECTOR, timeout=5000)
        except Exception:
            log.debug("maps_scraper.panel_not_detected", siren=siren, denomination=denomination)

        # ── Step 4: Detect result type (query_selector — fast) ────────
        # Check for direct business panel with phone button
        phone_btn = await page.query_selector('button[data-item-id^="phone"]')
        if phone_btn:
            # Layer 1: verify the h1 name before extracting
            try:
                h1 = await page.query_selector('h1.DUwDvf') or await page.query_selector('h1')
                if h1:
                    h1_text = (await h1.inner_text()).strip()
                    score = _name_similarity(h1_text, denomination)
                    if h1_text and score < 0.3:
                        log.info("maps_scraper.direct_hit_name_mismatch",
                                 h1=h1_text, denomination=denomination,
                                 score=round(score, 2), siren=siren)
                        return {"maps_name": h1_text}  # Return name for logging only
            except Exception:
                pass  # If h1 check fails, proceed with extraction anyway
            log.debug("maps_scraper.direct_business_hit",
                      denomination=denomination, siren=siren)
            return await self._extract_from_page(denomination, department, siren)

        # Check for result list — scan aria-labels for best name match
        all_results = await page.query_selector_all(
            'a.hfpxzc, [class*="hfpxzc"], [class*="Nv2PK"] a'
        )
        if all_results:
            best_result = None
            best_score = 0.0
            best_label = ""
            for r in all_results[:5]:  # Check top 5 results
                try:
                    label = (await r.get_attribute("aria-label")) or ""
                    score = _name_similarity(label, denomination)
                    if score > best_score:
                        best_score = score
                        best_result = r
                        best_label = label
                except Exception:
                    continue

            if best_result and best_score >= 0.4:
                log.debug("maps_scraper.smart_click",
                          matched_name=best_label, score=round(best_score, 2),
                          denomination=denomination, siren=siren)
                try:
                    await best_result.click()
                    # Wait for panel to render after click (not blind 600ms)
                    try:
                        await page.wait_for_selector('h1.DUwDvf', timeout=3000)
                    except Exception:
                        await page.wait_for_timeout(600)  # Fallback
                except Exception as exc:
                    log.debug("maps_scraper.click_result_failed",
                              error=str(exc), siren=siren)
                return await self._extract_from_page(denomination, department, siren)
            else:
                # No result matched our denomination — skip entirely
                labels = []
                for r in all_results[:3]:
                    try:
                        labels.append((await r.get_attribute("aria-label")) or "?")
                    except Exception:
                        pass
                log.info("maps_scraper.no_matching_result",
                         denomination=denomination, top_labels=labels,
                         best_score=round(best_score, 2), siren=siren)
                return {}  # Empty — will trigger replacement

        # Check for geographic entity (département/town page)
        page_text = await page.text_content("body") or ""
        if "En bref" in page_text and "Itinéraires" in page_text:
            log.debug("maps_scraper.geographic_entity_detected",
                      denomination=denomination, siren=siren)
            return {}

        # If nothing matched, try extracting anyway
        return await self._extract_from_page(denomination, department, siren)

    async def _extract_from_page(
        self, denomination: str, department: str, siren: str
    ) -> dict[str, Any]:
        """Extract phone, website, address from current Maps page.

        Uses query_selector_all (mega scrapper style) for fast DOM access.
        Falls back through 5 phone extraction strategies.
        """
        page = self._page
        result: dict[str, Any] = {}

        # ── Business name: h1 from panel header (for match validation) ──
        try:
            name_el = await page.query_selector('h1.DUwDvf') or await page.query_selector('h1')
            if name_el:
                maps_name = (await name_el.inner_text()).strip()
                if maps_name and len(maps_name) > 1:
                    result["maps_name"] = maps_name
        except Exception:
            pass  # Non-critical — used for diagnostics only

        # ── Category: business type text below the name ──────────────
        category = None

        # Strategy 1: JSON-LD structured data (most reliable — web standard)
        try:
            _cat_html = await page.content()
            ld_matches = re.findall(
                r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
                _cat_html, re.DOTALL,
            )
            for ld_raw in ld_matches:
                try:
                    ld = json.loads(ld_raw)
                    if isinstance(ld, dict):
                        cat = ld.get("@type") or ld.get("additionalType") or ""
                        if cat and cat not in ("LocalBusiness", "Place", "Organization", "WebPage"):
                            category = cat
                            break
                except (json.JSONDecodeError, TypeError):
                    pass
        except Exception:
            pass

        # Strategy 2: DOM walking from h1 (structural, class-independent)
        if not category:
            try:
                category = await page.evaluate("""() => {
                    const h1 = document.querySelector('h1.DUwDvf') || document.querySelector('h1');
                    if (!h1) return null;
                    const container = h1.closest('[role="main"]') || h1.parentElement?.parentElement;
                    if (!container) return null;
                    const candidates = container.querySelectorAll('button[jsaction], span.fontBodyMedium');
                    for (const el of candidates) {
                        const text = el.textContent.trim();
                        // Strip Material Icons unicode chars (private use area)
                        const clean = text.replace(/[\\uE000-\\uF8FF]/g, '').trim();
                        if (clean && clean.length > 1 && clean.length < 60
                            && !clean.includes('toile') && !clean.includes('avis')
                            && !/^\\d/.test(clean) && !clean.includes('Itin')
                            && !clean.includes('Voir') && !clean.includes('photo')
                            && !clean.includes('Envoyer') && !clean.includes('Partager')
                            && !clean.includes('Sauvegarder') && !clean.includes('Plus d')
                            && !clean.includes('Revendiq') && !clean.includes('Suggest')
                            && !clean.includes('sentation') && !clean.includes('Fermer')
                            && clean !== h1.textContent.trim()) {
                            return clean;
                        }
                    }
                    return null;
                }""")
            except Exception:
                pass

        # Strategy 3: button with jsaction containing "category"
        if not category:
            try:
                category = await page.evaluate("""() => {
                    const btn = document.querySelector('button[jsaction*="category"]');
                    return btn ? btn.textContent.trim() : null;
                }""")
            except Exception:
                pass

        # Strategy 4: span.DkEaL (fragile class — last resort)
        if not category:
            try:
                category = await page.evaluate("""() => {
                    const span = document.querySelector('span.DkEaL');
                    return span ? span.textContent.trim() : null;
                }""")
            except Exception:
                pass

        if category and len(category) > 1:
            result["category"] = category

        # ── Strategy 1: data-item-id phone buttons (Maps 2024+) ──────
        phone_btn = await page.query_selector('button[data-item-id^="phone"]')
        if phone_btn:
            try:
                data_id = await phone_btn.get_attribute("data-item-id")
                if data_id:
                    phone_match = re.search(r'tel:(.+)', data_id)
                    if phone_match:
                        cleaned = _clean_phone(phone_match.group(1))
                        if cleaned:
                            result["phone"] = cleaned
                            log.debug("maps_scraper.phone_data_item",
                                      phone=cleaned, siren=siren)
            except Exception as exc:
                log.debug("maps_scraper.phone_data_item_error", error=str(exc), siren=siren)

            # Also try inner text (mega scrapper: .Io6YTe class)
            if "phone" not in result:
                try:
                    io_el = await phone_btn.query_selector(".Io6YTe")
                    if io_el:
                        text = (await io_el.inner_text()).strip()
                        cleaned = _clean_phone(text)
                        if cleaned:
                            result["phone"] = cleaned
                            log.debug("maps_scraper.phone_io6yte",
                                      phone=cleaned, siren=siren)
                except Exception as exc:
                    log.debug("maps_scraper.phone_io6yte_error", error=str(exc), siren=siren)

        # ── Strategy 2: href="tel:..." links (fast regex on HTML) ─────
        if "phone" not in result:
            html = await page.content()
            tel_matches = _TEL_LINK_RE.findall(html)
            for tel in tel_matches:
                cleaned = _clean_phone(tel)
                if cleaned:
                    result["phone"] = cleaned
                    log.debug("maps_scraper.phone_tel_link",
                              phone=cleaned, siren=siren)
                    break

        # ── Strategy 3: data-item-id phone from HTML regex ────────────
        if "phone" not in result:
            html = html if "html" in dir() else await page.content()
            data_phone_matches = _DATA_ITEM_PHONE_RE.findall(html)
            for dp in data_phone_matches:
                cleaned = _clean_phone(dp)
                if cleaned:
                    result["phone"] = cleaned
                    log.debug("maps_scraper.phone_data_regex",
                              phone=cleaned, siren=siren)
                    break

        # ── Strategy 4: Raw French phone from body text ───────────────
        if "phone" not in result:
            try:
                body_text = await page.text_content("body") or ""
                raw_phones = _FRENCH_PHONE_RE.findall(body_text)
                for rp in raw_phones:
                    cleaned = _clean_phone(rp)
                    if cleaned:
                        result["phone"] = cleaned
                        log.debug("maps_scraper.phone_body_text",
                                  phone=cleaned, siren=siren)
                        break
            except Exception as exc:
                log.debug("maps_scraper.phone_body_text_error", error=str(exc), siren=siren)

        # ── Website: aria-label "Visiter le site Web de ..." ─────────────
        # Race condition fix: domcontentloaded fires before Maps JS renders
        # the website button in the side panel. We wait up to 5s for it.
        # Google Maps 2024+ uses aria-label on the <a>, not data-item-id.
        _WEBSITE_SELECTOR = (
            'a[aria-label*="site Web" i], '
            'a[aria-label*="website" i], '
            'a.lcr4fd[href^="http"], '
            'a[data-item-id="authority"], '
            'div.rogA2c a[href^="http"]'           # newer Maps layout
        )
        # Domains to exclude when extracting website URLs
        _WEBSITE_BLACKLIST = {
            "google", "gstatic", "googleapis", "youtube", "youtu.be",
            "facebook", "instagram", "twitter", "tripadvisor",
            "booking", "yelp", "maps", "play.google",
        }
        try:
            await page.wait_for_selector(_WEBSITE_SELECTOR, timeout=5000)
            website_el = await page.query_selector(_WEBSITE_SELECTOR)
            if website_el:
                href = await website_el.get_attribute("href")
                if href and href.startswith("http"):
                    domain = href.split("//")[-1].split("/")[0].lower()
                    if not any(bl in domain for bl in _WEBSITE_BLACKLIST):
                        result["website"] = href
        except Exception:
            # Timeout — try raw HTML fallback for website links
            log.debug("maps_scraper.website_selector_timeout",
                       denomination=denomination, siren=siren)

        # ── Website fallback: parse raw HTML for non-Google hrefs ─────
        if "website" not in result:
            try:
                html_content = await page.content()
                # Look for data-item-id="authority" href in raw HTML
                authority_match = re.search(
                    r'data-item-id="authority"[^>]*href="(https?://[^"]+)"',
                    html_content,
                )
                if not authority_match:
                    # Try aria-label pattern
                    authority_match = re.search(
                        r'aria-label="[^"]*(?:site [Ww]eb|website)[^"]*"[^>]*href="(https?://[^"]+)"',
                        html_content,
                    )
                if authority_match:
                    href = authority_match.group(1)
                    domain = href.split("//")[-1].split("/")[0].lower()
                    if not any(bl in domain for bl in _WEBSITE_BLACKLIST):
                        result["website"] = href
                        log.debug("maps_scraper.website_html_fallback",
                                   website=href, siren=siren)
            except Exception as exc:
                log.debug("maps_scraper.website_fallback_error",
                           error=str(exc), siren=siren)

        # ── Address: data-item-id="address" ───────────────────────────
        try:
            addr_el = await page.query_selector(
                'button[data-item-id="address"] .Io6YTe, '
                'button[data-item-id="address"]'
            )
            if addr_el:
                # Try aria-label first (cleaner, e.g. "Adresse : 12 Rue...")
                addr_text = await addr_el.get_attribute("aria-label")
                if addr_text:
                    addr = re.sub(r'^Adresse\s*:\s*', '', addr_text, flags=re.IGNORECASE)
                    if addr and addr != addr_text:
                        result["address"] = addr.strip()
                # Fallback to inner text if aria-label didn't work
                if "address" not in result:
                    try:
                        inner = (await addr_el.inner_text()).strip()
                        if inner and len(inner) > 5:
                            result["address"] = inner
                    except Exception as exc:
                        log.debug("maps_scraper.address_inner_text_error", error=str(exc), siren=siren)
        except Exception as exc:
            log.debug("maps_scraper.address_extraction_error", error=str(exc), siren=siren)

        # ── Rating + Review count ─────────────────────────────────────
        # Google Maps has TWO layouts:
        #   Detail panel: separate spans "3,9 étoiles " + "13 801 avis"
        #   Sidebar list: combined span "4,1 étoiles 1 160 avis"
        # Strategy 1: Parse all rating/review spans from aria-labels
        # Strategy 2: Body text fallback "X,Y(NNN)"
        try:
            # Wait for rating stars to appear (async after panel slide-in)
            try:
                await page.wait_for_selector(
                    'span[role="img"][aria-label*="toile"]',
                    timeout=2000,
                )
            except Exception:
                pass  # Rating stars may not exist — expected for unrated businesses

            rating_spans = await page.query_selector_all(
                'span[role="img"][aria-label*="toile"], '
                'span[role="img"][aria-label*="avis"]'
            )
            for rs in rating_spans[:10]:
                label = (await rs.get_attribute("aria-label") or "").strip()
                # Normalize non-breaking spaces (Google Maps uses \xa0)
                label = label.replace('\xa0', ' ')

                # Format A (combined): "3,0 étoiles 58 avis"
                combined = re.search(
                    r'(\d[,.]?\d?)\s*[ée]toiles?\s+([\d\s]+)\s*avis',
                    label, re.IGNORECASE,
                )
                if combined:
                    raw_r = combined.group(1).replace(',', '.')
                    raw_c = combined.group(2).replace(' ', '').strip()
                    try:
                        r_val = float(raw_r)
                        if 0 < r_val <= 5 and "rating" not in result:
                            result["rating"] = r_val
                    except ValueError:
                        log.debug("maps_scraper.rating_parse_error", raw=raw_r, siren=siren)
                    try:
                        if raw_c and "review_count" not in result:
                            result["review_count"] = int(raw_c)
                    except ValueError:
                        log.debug("maps_scraper.review_count_parse_error", raw=raw_c, siren=siren)
                    if result.get("rating"):
                        break  # Found both — done

                # Format B (rating only): "3,9 étoiles" or "4,1 étoiles "
                if "rating" not in result:
                    rating_only = re.search(
                        r'^(\d[,.]?\d?)\s*[ée]toiles?\s*$',
                        label, re.IGNORECASE,
                    )
                    if rating_only:
                        try:
                            r_val = float(rating_only.group(1).replace(',', '.'))
                            if 0 < r_val <= 5:
                                result["rating"] = r_val
                        except ValueError:
                            log.debug("maps_scraper.rating_b_parse_error", raw=rating_only.group(1), siren=siren)

                # Format C (review count only): "13 801 avis"
                if "review_count" not in result:
                    review_only = re.search(
                        r'^([\d\s]+)\s*avis$',
                        label, re.IGNORECASE,
                    )
                    if review_only:
                        try:
                            result["review_count"] = int(
                                review_only.group(1).replace(' ', '').replace('\xa0', '')
                            )
                        except ValueError:
                            log.debug("maps_scraper.review_c_parse_error", raw=review_only.group(1), siren=siren)

            if result.get("rating"):
                log.debug("maps_scraper.rating_found",
                          rating=result["rating"],
                          reviews=result.get("review_count"),
                          siren=siren)

            # Strategy 2: body text fallback — "X,Y(NNN)" pattern
            if "rating" not in result:
                body_text = await page.text_content("body") or ""
                body_match = re.search(
                    r'(\d[,.]?\d)\s*\(\s*([\d\s]+)\s*\)',
                    body_text,
                )
                if body_match:
                    try:
                        r_val = float(body_match.group(1).replace(',', '.'))
                        if 0 < r_val <= 5:
                            result["rating"] = r_val
                    except ValueError:
                        log.debug("maps_scraper.rating_body_parse_error", raw=body_match.group(1), siren=siren)
                    try:
                        rc_val = int(body_match.group(2).replace(' ', ''))
                        if rc_val > 0:  # Only store if actually > 0
                            result["review_count"] = rc_val
                    except ValueError:
                        log.debug("maps_scraper.review_body_parse_error", raw=body_match.group(2), siren=siren)
        except Exception as exc:
            log.debug("maps_scraper.rating_extraction_error", error=str(exc), siren=siren)

        # ── Business name: h1 from panel header ────────────────────────
        # Used by _match_to_sirene() in discovery.py for name-based validation.
        # Maps business panels always have an <h1> with the business name.
        try:
            name_el = await page.query_selector(
                'h1.DUwDvf, h1.fontHeadlineLarge, h1[class*="header"], div.lMbq3e h1'
            )
            if not name_el:
                # Fallback: any h1 on a Maps place page
                name_el = await page.query_selector('h1')
            if name_el:
                maps_name = (await name_el.inner_text()).strip()
                if maps_name and len(maps_name) > 1:
                    result["maps_name"] = maps_name
                    log.debug("maps_scraper.name_extracted",
                              maps_name=maps_name, siren=siren)
        except Exception as exc:
            log.debug("maps_scraper.name_extraction_error", error=str(exc), siren=siren)

        # ── Maps URL: capture the current page URL ─────────────────────
        current_url = page.url
        if current_url and "google.com/maps" in current_url:
            result["maps_url"] = current_url

        log.info(
            "maps_scraper.search_done",
            denomination=denomination,
            department=department,
            siren=siren,
            has_phone=bool(result.get("phone")),
            has_website=bool(result.get("website")),
            has_address=bool(result.get("address")),
            has_rating=bool(result.get("rating")),
            has_reviews=bool(result.get("review_count")),
            has_maps_url=bool(result.get("maps_url")),
            has_category=bool(result.get("category")),
            maps_name=result.get("maps_name"),
            maps_category=result.get("category", ""),
        )
        return result

