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

Usage (from runner.py):
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
import re
from typing import Any

import structlog

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
                    # ── Memory reduction flags (critical for Render) ──
                    "--single-process",               # Avoid multi-process overhead
                    "--disable-renderer-backgrounding",
                    "--disable-backgrounding-occluded-windows",
                    "--disable-ipc-flooding-protection",
                    "--disable-features=TranslateUI,BlinkGenPropertyTrees,IsolateOrigins,site-per-process",
                    "--js-flags=--max-old-space-size=128",  # Cap V8 heap at 128MB
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
        _BLOCKED_TYPES = frozenset({"image", "media", "font"})

        async def _intercept(route):
            if route.request.resource_type in _BLOCKED_TYPES:
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
        # Append query domain hint (e.g. "camping") + "France" for better Maps accuracy.
        # User testing showed this finds campings that were previously missed.
        hint_parts = [denomination, department]
        if query_hint:
            hint_parts.append(query_hint)
        hint_parts.append("France")
        query = " ".join(hint_parts)
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
        # the website button in the side panel. We wait up to 3s for it.
        # Google Maps 2024+ uses aria-label on the <a>, not data-item-id.
        _WEBSITE_SELECTOR = (
            'a[aria-label*="site Web" i], '
            'a[aria-label*="website" i], '
            'a.lcr4fd[href^="http"], '
            'a[data-item-id="authority"]'
        )
        try:
            await page.wait_for_selector(_WEBSITE_SELECTOR, timeout=3000)
            website_el = await page.query_selector(_WEBSITE_SELECTOR)
            if website_el:
                href = await website_el.get_attribute("href")
                if href and href.startswith("http") and "google" not in href:
                    result["website"] = href
        except Exception:
            # Timeout = business has no website, or Maps didn't render it.
            # Graceful: log and continue with the other 4 fields.
            log.debug("maps_scraper.website_not_found",
                       denomination=denomination, siren=siren)

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
                        result["review_count"] = int(
                            body_match.group(2).replace(' ', '')
                        )
                    except ValueError:
                        log.debug("maps_scraper.review_body_parse_error", raw=body_match.group(2), siren=siren)
        except Exception as exc:
            log.debug("maps_scraper.rating_extraction_error", error=str(exc), siren=siren)

        # ── Business name: h1 from panel header ────────────────────────
        # Used by _assess_match() in enricher.py for name-based validation.
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
            maps_name=result.get("maps_name"),
        )
        return result

    # ------------------------------------------------------------------
    # Website crawling via Playwright (reuses the same browser)
    # ------------------------------------------------------------------

    async def crawl_url(
        self,
        url: str,
        *,
        siren: str = "",
        max_pages: int = 3,
    ) -> dict[str, Any]:
        """Crawl a company website using the existing Playwright browser.

        Uses the same browser instance as Maps searches. This solves the
        problem where curl_cffi gets blocked/timeout but real browsers work.

        Visits homepage + contact page, extracts emails, phones, social links.

        Args:
            url:       Base URL to crawl (e.g. "https://example.fr")
            siren:     For logging context.
            max_pages: Max pages to visit (default 3: home + contact + 1 more)

        Returns:
            Dict with phones, emails, social, pages_visited.
        """
        if self._page is None:
            raise RuntimeError("PlaywrightMapsScraper not started")

        from fortress.module_b.contact_parser import (
            extract_emails,
            extract_phones,
            extract_social_links,
        )

        async with self._lock:
            try:
                return await asyncio.wait_for(
                    self._do_crawl(
                        url, siren, max_pages,
                        extract_emails, extract_phones, extract_social_links,
                    ),
                    timeout=30.0,  # 30s total timeout for crawling
                )
            except asyncio.TimeoutError:
                log.warning("maps_scraper.crawl_timeout", url=url, siren=siren)
                return {"phones": [], "emails": [], "social": {}, "pages_visited": 0}
            except Exception as exc:
                log.warning("maps_scraper.crawl_error", url=url, siren=siren, error=str(exc))
                return {"phones": [], "emails": [], "social": {}, "pages_visited": 0}

    async def _do_crawl(
        self,
        base_url: str,
        siren: str,
        max_pages: int,
        extract_emails: Any,
        extract_phones: Any,
        extract_social_links: Any,
    ) -> dict[str, Any]:
        """Internal crawl logic — visits pages and extracts contact data."""
        page = self._page
        phones: set[str] = set()
        emails: set[str] = set()
        social: dict[str, str] = {}
        pages_visited = 0

        # Contact page paths to try after the homepage
        contact_paths = ["/contact", "/nous-contacter", "/contactez-nous"]

        # Ensure base_url has scheme
        if not base_url.startswith(("http://", "https://")):
            base_url = "https://" + base_url

        # Strip trailing slash/path to get the root
        from urllib.parse import urlparse
        parsed = urlparse(base_url)
        root = f"{parsed.scheme}://{parsed.netloc}"

        urls_to_visit = [root]
        visited: set[str] = set()

        for visit_url in urls_to_visit:
            if pages_visited >= max_pages:
                break
            if visit_url in visited:
                continue

            try:
                response = await page.goto(
                    visit_url,
                    wait_until="domcontentloaded",
                    timeout=15000,  # 15s per page
                )
                await page.wait_for_timeout(500)
            except Exception as exc:
                log.debug("maps_scraper.crawl_page_error", url=visit_url, error=str(exc), siren=siren)
                visited.add(visit_url)
                continue

            visited.add(visit_url)
            pages_visited += 1

            # Get page HTML
            try:
                html = await page.content()
            except Exception:
                continue

            # Extract contact data
            page_phones = extract_phones(html)
            page_emails = extract_emails(html)
            page_social = extract_social_links(html)

            phones.update(page_phones)
            emails.update(page_emails)
            for platform, link in page_social.items():
                if platform not in social:
                    social[platform] = link

            log.debug(
                "maps_scraper.crawl_page_done",
                url=visit_url,
                phones_found=len(page_phones),
                emails_found=len(page_emails),
                siren=siren,
            )

            # After homepage: discover contact page links and add static paths
            if pages_visited == 1:
                # Look for contact links in the HTML
                contact_link = self._find_contact_link(html, root)
                if contact_link and contact_link not in visited:
                    urls_to_visit.insert(1, contact_link)
                # Add static contact paths as fallback
                for path in contact_paths:
                    candidate = root + path
                    if candidate not in visited and candidate not in urls_to_visit:
                        urls_to_visit.append(candidate)

            # Early exit if we found both phone and email
            if phones and emails:
                break

        log.info(
            "maps_scraper.crawl_done",
            url=root,
            phones=len(phones),
            emails=len(emails),
            social=list(social.keys()),
            pages_visited=pages_visited,
            siren=siren,
        )

        # Navigate back to Maps for the next search
        try:
            await page.goto(
                "https://www.google.com/maps?hl=fr",
                wait_until="domcontentloaded",
                timeout=10000,
            )
            await page.wait_for_timeout(500)
        except Exception as exc:
            log.debug("maps_scraper.return_to_maps_error", error=str(exc), siren=siren)

        return {
            "phones": sorted(phones),
            "emails": sorted(emails),
            "social": social,
            "pages_visited": pages_visited,
        }

    @staticmethod
    def _find_contact_link(html: str, base_url: str) -> str | None:
        """Find a contact page link in the HTML."""
        import re as _re
        from urllib.parse import urljoin, urlparse

        contact_keywords = ("/contact", "/nous-contacter", "/contactez-nous", "/a-propos")
        anchors = _re.findall(r'<a[^>]+href=["\']([^"\']+)["\']', html, _re.IGNORECASE)

        for href in anchors:
            href = href.strip()
            if not href or href.startswith(("#", "javascript:", "mailto:", "tel:")):
                continue

            if href.startswith("http"):
                abs_url = href
            elif href.startswith("/"):
                abs_url = base_url.rstrip("/") + href
            else:
                continue

            path = urlparse(abs_url).path.lower()
            for keyword in contact_keywords:
                if keyword in path:
                    return abs_url.split("?")[0].split("#")[0]

        return None

