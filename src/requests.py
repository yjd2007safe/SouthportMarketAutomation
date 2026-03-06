"""Network request helpers with conservative URL validation and backend routing."""

from __future__ import annotations

from dataclasses import dataclass
import os
import random
import time
from typing import Callable, Dict, Iterable, List, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import ProxyHandler, Request, build_opener, urlopen

SAFE_SCHEMES = {"http", "https"}
RETRYABLE_HTTP_STATUS = {429, 500, 502, 503, 504}
USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)


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


@dataclass(frozen=True)
class FetchConfig:
    """Environment/file-driven transport policy."""

    max_attempts: int = 3
    rate_limit_seconds: float = 0.5
    backoff_base: float = 0.5
    jitter_ratio: float = 0.2
    browser_domains: tuple[str, ...] = ("realestate.com.au",)
    proxy_domains: tuple[str, ...] = ()
    proxy_endpoints: tuple[str, ...] = ()
    domain_backends: Dict[str, str] | None = None


@dataclass(frozen=True)
class FetchDiagnostics:
    backend: str
    attempts: int
    outcome: str
    detail: str = ""


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
        proxy_domains=_parse_csv_env("SMA_FETCH_PROXY_DOMAINS"),
        proxy_endpoints=_load_proxy_endpoints(),
        domain_backends=mapping,
    )


def choose_backend(url: str, config: FetchConfig) -> str:
    """Choose backend by explicit mapping then domain policy."""
    host = (urlparse(url).hostname or "").lower()

    if config.domain_backends:
        for domain, backend in config.domain_backends.items():
            if host == domain or host.endswith(f".{domain}"):
                return backend

    for domain in config.browser_domains:
        if host == domain or host.endswith(f".{domain}"):
            return "browser"

    for domain in config.proxy_domains:
        if host == domain or host.endswith(f".{domain}"):
            return "proxy-http"

    return "http"


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


def _fetch_via_browser(url: str, *, timeout: int) -> FetchResult:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as exc:
        raise RuntimeError("browser backend requires playwright") from exc

    timeout_ms = max(1, int(timeout * 1000))
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent=USER_AGENT)
        page = context.new_page()
        page.goto(url, wait_until="networkidle", timeout=timeout_ms)
        html = page.content()
        browser.close()
    return FetchResult(
        text=html,
        diagnostics=FetchDiagnostics(backend="browser", attempts=1, outcome="ok"),
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
) -> FetchResult:
    """Fetch text using domain-routed backend policy with graceful fallback."""
    config = config or load_fetch_config()
    attempts = max_attempts or config.max_attempts
    backend = choose_backend(url, config)

    browser_fetcher = browser_fetcher or (lambda u, t: _fetch_via_browser(u, timeout=t))
    http_fetcher = http_fetcher or _fetch_via_http
    proxy_http_fetcher = proxy_http_fetcher or _fetch_via_proxy_http

    backend_order: List[str]
    if backend == "browser":
        backend_order = ["browser", "proxy-http", "http"]
    elif backend == "proxy-http":
        backend_order = ["proxy-http", "http", "browser"]
    else:
        backend_order = ["http", "proxy-http", "browser"]

    errors: List[str] = []
    for selected in backend_order:
        try:
            if selected == "http":
                result = http_fetcher(
                    url,
                    timeout=timeout,
                    max_attempts=attempts,
                    sleep_fn=sleep_fn,
                    random_fn=random_fn,
                    config=config,
                )
            elif selected == "proxy-http":
                result = proxy_http_fetcher(
                    url,
                    timeout=timeout,
                    max_attempts=attempts,
                    sleep_fn=sleep_fn,
                    random_fn=random_fn,
                    config=config,
                )
            elif selected == "browser":
                result = browser_fetcher(url, timeout)
            else:
                continue

            diag = FetchDiagnostics(
                backend=selected,
                attempts=result.diagnostics.attempts,
                outcome=result.diagnostics.outcome,
                detail=result.diagnostics.detail,
            )
            return FetchResult(text=result.text, diagnostics=diag)
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
