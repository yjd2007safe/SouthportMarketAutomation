"""Tiny local requests-compatible subset for offline scaffold use."""

from __future__ import annotations

import socket
from dataclasses import dataclass
from urllib import error, request


class RequestException(Exception):
    pass


class HTTPError(RequestException):
    pass


class Timeout(RequestException):
    pass


@dataclass
class Response:
    status_code: int
    text: str

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise HTTPError(f"HTTP {self.status_code}")


class Session:
    def __enter__(self) -> "Session":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def get(self, url: str, timeout: float = 10.0) -> Response:
        try:
            with request.urlopen(url, timeout=timeout) as resp:
                body = resp.read().decode("utf-8", errors="replace")
                return Response(status_code=resp.status, text=body)
        except socket.timeout as exc:
            raise Timeout(str(exc)) from exc
        except error.HTTPError as exc:
            raise HTTPError(str(exc)) from exc
        except error.URLError as exc:
            reason = getattr(exc, "reason", exc)
            if isinstance(reason, socket.timeout):
                raise Timeout(str(reason)) from exc
            raise RequestException(str(exc)) from exc
