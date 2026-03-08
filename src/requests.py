"""Network request helpers with conservative URL validation and backend routing."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import hmac
import json
import os
import random
import subprocess
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import ProxyHandler, Request, build_opener, urlopen

SAFE_SCHEMES = {"http", "https"}
RETRYABLE_HTTP_STATUS = {429, 500, 502, 503, 504}
USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)
LISTING_SELECTORS = (
    "[data-testid='listing-card']",
    "[data-testid='property-card']",
    "article[data-testid*='listing']",
    "a[href*='/property-']",
    "a[href*='/property/']",
)
LISTING_HTML_MARKERS = (
    "__next_data__",
    '"listingid"',
    "property-card",
    "data-testid=\"listing",
    "/property/",
)

SEARCH_INPUT_SELECTORS = (
    "input[type='search']",
    "input[placeholder*='Search' i]",
    "input[aria-label*='Search' i]",
    "input[name='q']",
)

_MANAGED_RELAY_PAGE_IDS: set[int] = set()


@dataclass(frozen=True)
class NavigationProfile:
    name: str
    start_url: str
    search_query: str
    required_url_tokens: tuple[str, ...] = ()
    forbidden_url_tokens: tuple[str, ...] = ()


NAVIGATION_PROFILES: Dict[str, NavigationProfile] = {
    "onthehouse_sale_southport": NavigationProfile(
        name="onthehouse_sale_southport",
        start_url="https://www.onthehouse.com.au",
        search_query="Southport QLD property for sale",
        required_url_tokens=("sale", "southport"),
        forbidden_url_tokens=("rent", "for-rent"),
    )
}


def load_navigation_profile(name: Optional[str]) -> Optional[NavigationProfile]:
    normalized = (name or "").strip().lower()
    if not normalized:
        return None
    if normalized not in NAVIGATION_PROFILES:
        known = ", ".join(sorted(NAVIGATION_PROFILES))
        raise ValueError(f"Unknown navigation profile: {name!r}. Known profiles: {known}")
    return NAVIGATION_PROFILES[normalized]


def _url_matches_navigation_profile(url: str, profile: NavigationProfile) -> bool:
    lowered = url.lower()
    if profile.required_url_tokens and not all(token in lowered for token in profile.required_url_tokens):
        return False
    if any(token in lowered for token in profile.forbidden_url_tokens):
        return False
    return True


def _navigate_listing_search(page: Any, profile: NavigationProfile, timeout_ms: int, ready_timeout_ms: int) -> None:
    page.goto(profile.start_url, wait_until="domcontentloaded", timeout=timeout_ms)

    for selector in SEARCH_INPUT_SELECTORS:
        try:
            page.wait_for_selector(selector, timeout=ready_timeout_ms)
            page.fill(selector, profile.search_query)
            page.press(selector, "Enter")
            page.wait_for_load_state("networkidle", timeout=timeout_ms)
            return
        except Exception:
            continue

    raise RuntimeError(f"navigation profile {profile.name!r} could not find a search input")


class BlockedSourceError(RuntimeError):
    """Raised when a source appears to be actively blocked (for example HTTP 429)."""

    def __init__(self, url: str, *, status: int, attempts: int, backend: str = "http") -> None:
        super().__init__(
            f"Blocked source after {attempts} attempts: {url} (HTTP {status}, backend={backend})"
        )
        self.url = url
        self.status = status
        self.attempts = attempts
        self.backend = backend


class ChallengeDetectedError(RuntimeError):
    def __init__(self, provider: str, backend: str) -> None:
        super().__init__(f"challenge:{provider} backend={backend}")
        self.provider = provider
        self.backend = backend


@dataclass(frozen=True)
class FetchConfig:
    """Environment/file-driven transport policy."""

    max_attempts: int = 3
    rate_limit_seconds: float = 0.5
    backoff_base: float = 0.5
    jitter_ratio: float = 0.2
    browser_domains: tuple[str, ...] = ("realestate.com.au",)
    relay_domains: tuple[str, ...] = ("realestate.com.au", "domain.com.au", "onthehouse.com.au")
    proxy_domains: tuple[str, ...] = ()
    proxy_endpoints: tuple[str, ...] = ()
    domain_backends: Dict[str, str] | None = None


@dataclass(frozen=True)
class StabilityPolicy:
    profile: str = "default"
    rate_limit_seconds: float = 0.5
    backoff_base: float = 0.5
    jitter_ratio: float = 0.2
    browser_ready_timeout_seconds: float = 3.0
    browser_settle_seconds: float = 0.0
    challenge_retry_cooldown_seconds: float = 0.0
    challenge_retry_once: bool = False
    max_backend_parallelism: int = 4


@dataclass(frozen=True)
class FetchDiagnostics:
    backend: str
    attempts: int
    outcome: str
    detail: str = ""
    stability_profile: str = "default"
    challenge_detected: str = ""
    challenge_retry_attempted: bool = False


@dataclass(frozen=True)
class FetchResult:
    text: str
    diagnostics: FetchDiagnostics


def validate_url_scheme(url: str) -> None:
    """Validate that URL uses a safe and supported scheme."""
    parsed = urlparse(url)
    if parsed.scheme not in SAFE_SCHEMES:
        raise ValueError(f"Unsupported URL scheme: {parsed.scheme!r}")


def _parse_csv_env(name: str) -> tuple[str, ...]:
    raw = os.getenv(name, "")
    items = [entry.strip() for entry in raw.split(",") if entry.strip()]
    return tuple(items)


def _load_proxy_endpoints() -> tuple[str, ...]:
    from_env = list(_parse_csv_env("SMA_FETCH_PROXY_ENDPOINTS"))
    file_path = os.getenv("SMA_FETCH_PROXY_FILE", "").strip()
    from_file: List[str] = []
    if file_path:
        with open(file_path, "r", encoding="utf-8") as handle:
            for line in handle:
                value = line.strip()
                if value and not value.startswith("#"):
                    from_file.append(value)
    combined = from_env + from_file
    seen: List[str] = []
    for endpoint in combined:
        if endpoint not in seen:
            seen.append(endpoint)
    return tuple(seen)


def load_fetch_config() -> FetchConfig:
    """Load backend routing and retry policy from environment."""
    mapping: Dict[str, str] = {}
    for item in _parse_csv_env("SMA_FETCH_DOMAIN_BACKENDS"):
        if "=" not in item:
            continue
        domain, backend = item.split("=", 1)
        if domain.strip() and backend.strip():
            mapping[domain.strip().lower()] = backend.strip()

    return FetchConfig(
        max_attempts=max(1, int(os.getenv("SMA_FETCH_MAX_ATTEMPTS", "3"))),
        rate_limit_seconds=max(0.0, float(os.getenv("SMA_FETCH_RATE_LIMIT_SECONDS", "0.5"))),
        backoff_base=max(0.0, float(os.getenv("SMA_FETCH_BACKOFF_BASE", "0.5"))),
        jitter_ratio=max(0.0, float(os.getenv("SMA_FETCH_JITTER_RATIO", "0.2"))),
        browser_domains=_parse_csv_env("SMA_FETCH_BROWSER_DOMAINS") or ("realestate.com.au",),
        relay_domains=_parse_csv_env("SMA_FETCH_RELAY_DOMAINS")
        or ("realestate.com.au", "domain.com.au", "onthehouse.com.au"),
        proxy_domains=_parse_csv_env("SMA_FETCH_PROXY_DOMAINS"),
        proxy_endpoints=_load_proxy_endpoints(),
        domain_backends=mapping,
    )


def get_stability_policy(profile: str = "default") -> StabilityPolicy:
    normalized = (profile or "default").strip().lower()
    if normalized == "slow":
        return StabilityPolicy(
            profile="slow",
            rate_limit_seconds=1.25,
            backoff_base=1.25,
            jitter_ratio=0.6,
            browser_ready_timeout_seconds=10.0,
            browser_settle_seconds=1.2,
            challenge_retry_cooldown_seconds=8.0,
            challenge_retry_once=True,
            max_backend_parallelism=1,
        )
    return StabilityPolicy()


def choose_backend(url: str, config: FetchConfig, fetch_mode: str = "auto") -> str:
    """Choose backend by explicit mapping then domain policy."""
    host = (urlparse(url).hostname or "").lower()

    if config.domain_backends:
        for domain, backend in config.domain_backends.items():
            if host == domain or host.endswith(f".{domain}"):
                return backend

    if fetch_mode == "relay":
        for domain in config.relay_domains:
            if host == domain or host.endswith(f".{domain}"):
                return "relay"

    for domain in config.browser_domains:
        if host == domain or host.endswith(f".{domain}"):
            return "browser"

    for domain in config.proxy_domains:
        if host == domain or host.endswith(f".{domain}"):
            return "proxy-http"

    return "http"


def _resolve_gateway_token() -> str:
    env_token = os.getenv("OPENCLAW_GATEWAY_TOKEN", "").strip()
    if env_token:
        return env_token

    config_path = Path.home() / ".openclaw" / "openclaw.json"
    if config_path.exists():
        try:
            payload = json.loads(config_path.read_text(encoding="utf-8"))
            token = str(payload.get("gateway", {}).get("auth", {}).get("token", "")).strip()
            if token:
                return token
        except Exception:
            return ""
    return ""


def _resolve_relay_auth_header_and_token(cdp_url: str) -> tuple[str, str]:
    header = os.getenv("SMA_RELAY_AUTH_HEADER", "x-openclaw-relay-token").strip() or "x-openclaw-relay-token"

    explicit = os.getenv("SMA_RELAY_AUTH_TOKEN", "").strip()
    if explicit:
        return header, explicit

    gateway_token = _resolve_gateway_token()
    if not gateway_token:
        return header, ""

    parsed = urlparse(cdp_url)
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    relay_token = hmac.new(
        gateway_token.encode("utf-8"),
        f"openclaw-extension-relay-v1:{port}".encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return header, relay_token


def _sleep_with_backoff(
    attempt: int,
    *,
    sleep_fn: Callable[[float], None],
    random_fn: Callable[[], float],
    config: FetchConfig,
) -> None:
    base = config.backoff_base * (2 ** (attempt - 1))
    jitter = base * config.jitter_ratio * random_fn()
    sleep_fn(config.rate_limit_seconds + base + jitter)


def _classify_challenge(html: str) -> str:
    from scrape_listings import detect_challenge_page

    detected = detect_challenge_page(html)
    return detected or ""


def _has_meaningful_listing_content(html: str) -> bool:
    lowered = html.lower()
    return any(marker in lowered for marker in LISTING_HTML_MARKERS)


def _fetch_via_http(
    url: str,
    *,
    timeout: int,
    max_attempts: int,
    sleep_fn: Callable[[float], None],
    random_fn: Callable[[], float],
    config: FetchConfig,
    opener=urlopen,
) -> FetchResult:
    validate_url_scheme(url)

    for attempt in range(1, max_attempts + 1):
        try:
            with opener(url, timeout=timeout) as response:
                payload = response.read()
            return FetchResult(
                text=payload.decode("utf-8"),
                diagnostics=FetchDiagnostics(backend="http", attempts=attempt, outcome="ok"),
            )
        except HTTPError as exc:
            should_retry = exc.code in RETRYABLE_HTTP_STATUS and attempt < max_attempts
            if should_retry:
                _sleep_with_backoff(
                    attempt,
                    sleep_fn=sleep_fn,
                    random_fn=random_fn,
                    config=config,
                )
                continue
            if exc.code == 429:
                raise BlockedSourceError(url, status=exc.code, attempts=attempt, backend="http") from exc
            raise
        except URLError:
            if attempt >= max_attempts:
                raise
            _sleep_with_backoff(
                attempt,
                sleep_fn=sleep_fn,
                random_fn=random_fn,
                config=config,
            )

    raise RuntimeError(f"Unexpected retry loop termination for URL: {url}")


def _fetch_via_proxy_http(
    url: str,
    *,
    timeout: int,
    max_attempts: int,
    sleep_fn: Callable[[float], None],
    random_fn: Callable[[], float],
    config: FetchConfig,
) -> FetchResult:
    if not config.proxy_endpoints:
        raise RuntimeError("proxy-http backend selected but no proxy endpoints are configured")

    for attempt in range(1, max_attempts + 1):
        endpoint = config.proxy_endpoints[(attempt - 1) % len(config.proxy_endpoints)]
        proxy_support = {"http": endpoint, "https": endpoint}
        opener = build_opener()
        opener.addheaders = [("User-Agent", USER_AGENT)]
        opener.add_handler(ProxyHandler(proxy_support))
        try:
            request = Request(url, headers={"User-Agent": USER_AGENT})
            with opener.open(request, timeout=timeout) as response:
                payload = response.read()
            return FetchResult(
                text=payload.decode("utf-8"),
                diagnostics=FetchDiagnostics(
                    backend="proxy-http",
                    attempts=attempt,
                    outcome="ok",
                    detail=f"proxy={endpoint}",
                ),
            )
        except HTTPError as exc:
            if exc.code == 429 and attempt >= max_attempts:
                raise BlockedSourceError(
                    url,
                    status=exc.code,
                    attempts=attempt,
                    backend="proxy-http",
                ) from exc
            if exc.code in RETRYABLE_HTTP_STATUS and attempt < max_attempts:
                _sleep_with_backoff(
                    attempt,
                    sleep_fn=sleep_fn,
                    random_fn=random_fn,
                    config=config,
                )
                continue
            raise
        except URLError:
            if attempt >= max_attempts:
                raise
            _sleep_with_backoff(
                attempt,
                sleep_fn=sleep_fn,
                random_fn=random_fn,
                config=config,
            )

    raise RuntimeError(f"Unexpected retry loop termination for URL: {url}")


def _fetch_via_browser(
    url: str,
    *,
    timeout: int,
    stability_policy: StabilityPolicy,
    sleep_fn: Callable[[float], None],
    navigation_profile: Optional[NavigationProfile] = None,
) -> FetchResult:
    try:
        from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
        from playwright.sync_api import sync_playwright
    except ImportError as exc:
        raise RuntimeError("browser backend requires playwright") from exc

    timeout_ms = max(1, int(timeout * 1000))
    ready_timeout_ms = max(1, int(stability_policy.browser_ready_timeout_seconds * 1000))
    challenge_retry_attempted = False

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent=USER_AGENT)
        page = context.new_page()

        for attempt in range(1, 3):
            if navigation_profile is not None:
                _navigate_listing_search(page, navigation_profile, timeout_ms, ready_timeout_ms)
            else:
                page.goto(url, wait_until="networkidle", timeout=timeout_ms)
            for selector in LISTING_SELECTORS:
                try:
                    page.wait_for_selector(selector, timeout=ready_timeout_ms)
                    break
                except PlaywrightTimeoutError:
                    continue
            if stability_policy.browser_settle_seconds > 0:
                sleep_fn(stability_policy.browser_settle_seconds)

            current_url = page.url
            if navigation_profile is not None and not _url_matches_navigation_profile(current_url, navigation_profile):
                raise RuntimeError(
                    f"navigation profile {navigation_profile.name} landed on unexpected url: {current_url}"
                )

            html = page.content()
            challenge = _classify_challenge(html)
            if challenge:
                if stability_policy.challenge_retry_once and attempt == 1:
                    challenge_retry_attempted = True
                    sleep_fn(stability_policy.challenge_retry_cooldown_seconds)
                    continue
                browser.close()
                raise ChallengeDetectedError(challenge, "browser")

            if _has_meaningful_listing_content(html):
                browser.close()
                return FetchResult(
                    text=html,
                    diagnostics=FetchDiagnostics(
                        backend="browser",
                        attempts=1,
                        outcome="ok",
                        stability_profile=stability_policy.profile,
                        challenge_retry_attempted=challenge_retry_attempted,
                    ),
                )

        browser.close()

    raise RuntimeError("browser returned no meaningful listing selectors/content")


def _fetch_via_relay(
    url: str,
    *,
    timeout: int,
    config: FetchConfig,
    stability_policy: StabilityPolicy,
    sleep_fn: Callable[[float], None],
    navigation_profile: Optional[NavigationProfile] = None,
) -> FetchResult:
    bridge_script = os.getenv("SMA_RELAY_BRIDGE_SCRIPT", "").strip()
    if bridge_script:
        command = ["python3", bridge_script, url]
        completed = subprocess.run(command, check=False, capture_output=True, text=True, timeout=timeout)
        if completed.returncode == 0 and completed.stdout.strip():
            html = completed.stdout
            challenge = _classify_challenge(html)
            if challenge:
                raise ChallengeDetectedError(challenge, "relay")
            if not _has_meaningful_listing_content(html):
                raise RuntimeError("relay bridge returned no meaningful listing content")
            return FetchResult(
                text=html,
                diagnostics=FetchDiagnostics(backend="relay", attempts=1, outcome="ok", detail="bridge-script"),
            )
        raise RuntimeError(f"relay bridge failed: {completed.stderr.strip() or completed.returncode}")

    try:
        from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
        from playwright.sync_api import sync_playwright
    except ImportError as exc:
        raise RuntimeError("relay backend requires playwright") from exc

    cdp_url = os.getenv("SMA_RELAY_CDP_URL", "http://127.0.0.1:9222")
    relay_auth_header, relay_auth_token = _resolve_relay_auth_header_and_token(cdp_url)
    connect_kwargs = {}
    if relay_auth_token:
        connect_kwargs["headers"] = {relay_auth_header: relay_auth_token}

    timeout_ms = max(1, int(timeout * 1000))
    ready_timeout_ms = max(1, int(stability_policy.browser_ready_timeout_seconds * 1000))

    with sync_playwright() as p:
        browser = p.chromium.connect_over_cdp(cdp_url, **connect_kwargs)

        if not browser.contexts:
            browser.close()
            raise RuntimeError("relay has no attached browser contexts")

        managed_context = browser.contexts[0]
        target_page = managed_context.new_page()
        page_id = id(target_page)
        _MANAGED_RELAY_PAGE_IDS.add(page_id)

        try:
            if navigation_profile is not None:
                _navigate_listing_search(target_page, navigation_profile, timeout_ms, ready_timeout_ms)
            else:
                target_page.goto(url, wait_until="networkidle", timeout=timeout_ms)
                target_page.wait_for_load_state("networkidle", timeout=timeout_ms)
            for selector in LISTING_SELECTORS:
                try:
                    target_page.wait_for_selector(selector, timeout=ready_timeout_ms)
                    break
                except PlaywrightTimeoutError:
                    continue
            if stability_policy.browser_settle_seconds > 0:
                sleep_fn(stability_policy.browser_settle_seconds)

            current_url = target_page.url
            if navigation_profile is not None and not _url_matches_navigation_profile(current_url, navigation_profile):
                raise RuntimeError(
                    f"navigation profile {navigation_profile.name} landed on unexpected url: {current_url}"
                )

            html = target_page.content()
            challenge = _classify_challenge(html)
        finally:
            if id(target_page) in _MANAGED_RELAY_PAGE_IDS:
                try:
                    target_page.close()
                finally:
                    _MANAGED_RELAY_PAGE_IDS.discard(id(target_page))
            browser.close()

        if challenge:
            raise ChallengeDetectedError(challenge, "relay")
        if not _has_meaningful_listing_content(html):
            raise RuntimeError("relay tab returned no meaningful listing content")
        return FetchResult(
            text=html,
            diagnostics=FetchDiagnostics(backend="relay", attempts=1, outcome="ok", detail="cdp-tab"),
        )


def fetch_with_policy(
    url: str,
    *,
    timeout: int = 10,
    config: Optional[FetchConfig] = None,
    max_attempts: Optional[int] = None,
    sleep_fn: Callable[[float], None] = time.sleep,
    random_fn: Callable[[], float] = random.random,
    browser_fetcher: Optional[Callable[[str, int], FetchResult]] = None,
    http_fetcher: Optional[Callable[..., FetchResult]] = None,
    proxy_http_fetcher: Optional[Callable[..., FetchResult]] = None,
    relay_fetcher: Optional[Callable[..., FetchResult]] = None,
    fetch_mode: str = "auto",
    stability_profile: str = "default",
    navigation_profile: Optional[str] = None,
) -> FetchResult:
    """Fetch text using domain-routed backend policy with graceful fallback."""
    config = config or load_fetch_config()
    policy = get_stability_policy(stability_profile)
    nav_profile = load_navigation_profile(navigation_profile)
    attempts = max_attempts or config.max_attempts
    tuned_config = FetchConfig(
        max_attempts=config.max_attempts,
        rate_limit_seconds=max(config.rate_limit_seconds, policy.rate_limit_seconds),
        backoff_base=max(config.backoff_base, policy.backoff_base),
        jitter_ratio=max(config.jitter_ratio, policy.jitter_ratio),
        browser_domains=config.browser_domains,
        relay_domains=config.relay_domains,
        proxy_domains=config.proxy_domains,
        proxy_endpoints=config.proxy_endpoints,
        domain_backends=config.domain_backends,
    )
    backend = choose_backend(url, tuned_config, fetch_mode=fetch_mode)

    browser_fetcher = browser_fetcher or (
        lambda u, t: _fetch_via_browser(
            u,
            timeout=t,
            stability_policy=policy,
            sleep_fn=sleep_fn,
            navigation_profile=nav_profile,
        )
    )
    http_fetcher = http_fetcher or _fetch_via_http
    proxy_http_fetcher = proxy_http_fetcher or _fetch_via_proxy_http
    relay_fetcher = relay_fetcher or (
        lambda u, t, c: _fetch_via_relay(
            u,
            timeout=t,
            config=c,
            stability_policy=policy,
            sleep_fn=sleep_fn,
            navigation_profile=nav_profile,
        )
    )

    backend_order: List[str]
    if backend == "relay":
        backend_order = ["relay", "browser", "proxy-http", "http"]
    elif backend == "browser":
        backend_order = ["browser", "proxy-http", "http"]
    elif backend == "proxy-http":
        backend_order = ["proxy-http", "http", "browser"]
    else:
        backend_order = ["http", "proxy-http", "browser"]

    errors: List[str] = []
    challenge_detected = ""
    challenge_retry_attempted = False
    for selected in backend_order:
        try:
            if selected == "http":
                result = http_fetcher(
                    url,
                    timeout=timeout,
                    max_attempts=attempts,
                    sleep_fn=sleep_fn,
                    random_fn=random_fn,
                    config=tuned_config,
                )
            elif selected == "proxy-http":
                result = proxy_http_fetcher(
                    url,
                    timeout=timeout,
                    max_attempts=attempts,
                    sleep_fn=sleep_fn,
                    random_fn=random_fn,
                    config=tuned_config,
                )
            elif selected == "browser":
                result = browser_fetcher(url, timeout)
            elif selected == "relay":
                result = relay_fetcher(url, timeout, tuned_config)
            else:
                continue

            diag = FetchDiagnostics(
                backend=selected,
                attempts=result.diagnostics.attempts,
                outcome=result.diagnostics.outcome,
                detail=result.diagnostics.detail,
                stability_profile=policy.profile,
                challenge_detected=challenge_detected or result.diagnostics.challenge_detected,
                challenge_retry_attempted=(
                    challenge_retry_attempted or result.diagnostics.challenge_retry_attempted
                ),
            )
            return FetchResult(text=result.text, diagnostics=diag)
        except ChallengeDetectedError as exc:
            challenge_detected = exc.provider
            challenge_retry_attempted = challenge_retry_attempted or policy.challenge_retry_once
            errors.append(f"{selected}:challenge:{exc.provider}")
        except BlockedSourceError:
            raise
        except Exception as exc:
            errors.append(f"{selected}:{exc}")

    raise RuntimeError("all backends failed: " + "; ".join(errors))


def fetch_text(
    url: str,
    *,
    timeout: int = 10,
    opener=urlopen,
    max_retries: int = 3,
    backoff_base: float = 0.5,
    jitter_ratio: float = 0.2,
    sleep_fn=time.sleep,
    random_fn=random.random,
) -> str:
    """Backward-compatible HTTP-only fetch helper used by existing callers/tests."""
    config = FetchConfig(
        max_attempts=max_retries + 1,
        rate_limit_seconds=0.0,
        backoff_base=backoff_base,
        jitter_ratio=jitter_ratio,
    )
    result = _fetch_via_http(
        url,
        timeout=timeout,
        max_attempts=max_retries + 1,
        sleep_fn=sleep_fn,
        random_fn=random_fn,
        config=config,
        opener=opener,
    )
    return result.text
