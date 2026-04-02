#!/usr/bin/env python3
from __future__ import annotations

import atexit
import argparse
import base64
import hashlib
import html
import io
import json
import os
import platform
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import tomllib
import zipfile
from http.client import RemoteDisconnected
from dataclasses import dataclass, field
from datetime import UTC, datetime
from html.parser import HTMLParser
from pathlib import Path
from typing import Callable, Iterable
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urljoin
from urllib.request import HTTPCookieProcessor, ProxyHandler, Request, build_opener, getproxies
import http.cookiejar

try:
    from websockets.exceptions import ConnectionClosed
    from websockets.sync.client import connect as websocket_connect
except ModuleNotFoundError:
    ConnectionClosed = RuntimeError
    websocket_connect = None


APP_DIR = Path(sys.executable).resolve().parent if getattr(sys, "frozen", False) else Path(__file__).resolve().parent


def project_venv_python() -> Path:
    if platform.system() == "Windows":
        return APP_DIR / ".venv" / "Scripts" / "python.exe"
    return APP_DIR / ".venv" / "bin" / "python"

try:
    import fitz
except ModuleNotFoundError as exc:
    if exc.name != "fitz":
        raise

    project_root = APP_DIR
    venv_python = project_venv_python()

    if venv_python.exists():
        install_cmd = f'"{venv_python}" -m pip install -r "{project_root / "requirements.txt"}"'
        run_cmd = f'"{venv_python}" "{project_root / "build_disclosure_pdf.py"}"'
    else:
        if platform.system() == "Windows":
            install_cmd = "py -m pip install -r requirements.txt"
            run_cmd = "py build_disclosure_pdf.py"
        else:
            install_cmd = "python3 -m pip install -r requirements.txt"
            run_cmd = "python3 build_disclosure_pdf.py"

    raise SystemExit(
        "Missing dependency: PyMuPDF ('fitz') is not installed for this Python interpreter.\n"
        f"Current interpreter: {sys.executable}\n"
        f"Install with: {install_cmd}\n"
        f"Then run with: {run_cmd}"
    ) from exc


BASE_URL = "https://dart.fss.or.kr"
TODAY = datetime.today()
DEFAULT_COMPANY_NAME = "삼성전자"
DEFAULT_START_DATE = TODAY.strftime("%Y-01-01")
DEFAULT_END_DATE = TODAY.strftime("%Y-%m-%d")
CONFIG_PATH = APP_DIR / "config.toml"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
)
A4_PAGE_RECT = fitz.paper_rect("a4")
A4_PAGE_WIDTH = A4_PAGE_RECT.width
A4_PAGE_HEIGHT = A4_PAGE_RECT.height
PAGE_MARGIN = 18
HEADER_BAND_HEIGHT = 14
HEADER_FONT_SIZE = 7
HEADER_TEXT_COLOR = (0.42, 0.42, 0.42)
RETRY_COUNT = 3
RETRY_DELAY_SECONDS = 1.5
HEADER_FONT_NAME = "korea"
BROWSER_START_TIMEOUT_SECONDS = 10.0
BROWSER_COMMAND_TIMEOUT_SECONDS = 30.0
BROWSER_LOCK_WAIT_SECONDS = 0.2
BROWSER_PAGE_LOAD_TIMEOUT_SECONDS = 10.0
CACHE_LOCK_WAIT_SECONDS = 0.2
PDF_SOURCE_CACHE_VERSION = 1
REPORT_DETAIL_CACHE_VERSION = 1
HTML_PDF_RENDERER_BROWSER = "browser"
HTML_PDF_RENDERER_FITZ = "fitz"
HTML_PDF_RENDERER_AUTO = "auto"
VALID_HTML_PDF_RENDERERS = {
    HTML_PDF_RENDERER_BROWSER,
    HTML_PDF_RENDERER_FITZ,
    HTML_PDF_RENDERER_AUTO,
}
HTML_PRINT_CSS = (
    b"<style>"
    b"@page { size: A4; margin: 10mm; }"
    b"html, body { margin: 0 !important; padding: 0 !important; }"
    b"body { -webkit-print-color-adjust: exact; print-color-adjust: exact; }"
    b"</style>"
)
RETRYABLE_REQUEST_ERRORS = (
    HTTPError,
    URLError,
    RemoteDisconnected,
    ConnectionResetError,
    TimeoutError,
)


class JobCancelledError(RuntimeError):
    pass


def raise_if_cancel_requested(cancel_requested: Callable[[], bool] | None) -> None:
    if cancel_requested is not None and cancel_requested():
        raise JobCancelledError("작업이 취소되었습니다.")


def sleep_with_cancellation(seconds: float, cancel_requested: Callable[[], bool] | None) -> None:
    deadline = time.time() + max(seconds, 0)
    while True:
        raise_if_cancel_requested(cancel_requested)
        remaining = deadline - time.time()
        if remaining <= 0:
            return
        time.sleep(min(0.2, remaining))


@dataclass(slots=True)
class Disclosure:
    company_name: str
    corp_key: str
    market_page: str
    page_grouping: str
    report_title: str
    presenter: str
    receipt_date: str  # YYYY.MM.DD
    rcp_no: str
    stock_code: str = ""
    attachment_docs: list["AttachmentDoc"] = field(default_factory=list)

    @property
    def receipt_date_key(self) -> str:
        return self.receipt_date.replace(".", "")

    @property
    def header_receipt_date(self) -> str:
        return datetime.strptime(self.receipt_date, "%Y.%m.%d").strftime("%y.%m.%d")


@dataclass(slots=True)
class AttachmentDoc:
    rcp_no: str
    dcm_no: str
    title: str


@dataclass(slots=True)
class PdfSource:
    bytes_data: bytes
    label: str


@dataclass(slots=True)
class DownloadAsset:
    kind: str
    path: str


@dataclass(slots=True)
class ReportDetail:
    main_dcm_no: str
    attachments: list["AttachmentDoc"]


@dataclass(slots=True)
class DisclosureBatch:
    company_name: str
    stock_code: str
    disclosures: list[Disclosure]


@dataclass(slots=True)
class AppConfig:
    company_name: str
    output_dir: str
    cache_dir: str


def default_output_dir() -> str:
    return str(Path.home() / "Documents")


def default_cache_dir() -> str:
    env_cache_dir = normalized_env_value("DART_CACHE_DIR")
    if env_cache_dir:
        return env_cache_dir

    system_name = platform.system()
    if system_name == "Windows":
        local_app_data = os.getenv("LOCALAPPDATA")
        if local_app_data:
            return str(Path(local_app_data) / "dart-disclosure-pdf-builder" / "cache")
        return str(Path.home() / "AppData" / "Local" / "dart-disclosure-pdf-builder" / "cache")
    if system_name == "Darwin":
        return str(Path.home() / "Library" / "Caches" / "dart-disclosure-pdf-builder")

    xdg_cache_home = os.getenv("XDG_CACHE_HOME")
    if xdg_cache_home:
        return str(Path(xdg_cache_home).expanduser() / "dart-disclosure-pdf-builder")
    return str(Path.home() / ".cache" / "dart-disclosure-pdf-builder")


def platform_config_section_name() -> str | None:
    system_name = platform.system()
    if system_name == "Windows":
        return "windows"
    if system_name == "Darwin":
        return "macos"
    if system_name == "Linux":
        return "linux"
    return None


def normalize_config_value(value: object, *, label: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise RuntimeError(f"config.toml의 {label} 값은 문자열이어야 합니다.")
    normalized = value.strip()
    return normalized or None


def merge_config_section(merged: dict[str, str], section: object, *, section_name: str) -> None:
    if section is None:
        return
    if not isinstance(section, dict):
        raise RuntimeError(f"config.toml의 [{section_name}] 섹션은 테이블이어야 합니다.")
    for key in ("company_name", "output_dir", "cache_dir"):
        normalized = normalize_config_value(section.get(key), label=f"[{section_name}].{key}")
        if normalized is not None:
            merged[key] = normalized


def load_app_config(config_path: Path = CONFIG_PATH) -> AppConfig:
    merged: dict[str, str] = {}
    if config_path.exists():
        with config_path.open("rb") as config_file:
            raw_config = tomllib.load(config_file)
        if not isinstance(raw_config, dict):
            raise RuntimeError("config.toml 최상위 구조가 올바르지 않습니다.")

        merge_config_section(merged, raw_config.get("default"), section_name="default")

        platform_section = platform_config_section_name()
        if platform_section is not None:
            merge_config_section(merged, raw_config.get(platform_section), section_name=platform_section)

    company_name = merged.get("company_name", DEFAULT_COMPANY_NAME)
    output_dir = merged.get("output_dir", default_output_dir())
    cache_dir = merged.get("cache_dir", default_cache_dir())
    return AppConfig(company_name=company_name, output_dir=output_dir, cache_dir=cache_dir)


def normalized_env_value(name: str) -> str | None:
    value = os.getenv(name)
    if value is None:
        return None
    normalized = value.strip()
    return normalized or None


def env_float(name: str, default: float) -> float:
    value = normalized_env_value(name)
    if value is None:
        return default
    try:
        return float(value)
    except ValueError:
        return default


def env_int(name: str, default: int) -> int:
    value = normalized_env_value(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


REQUEST_MIN_INTERVAL_SECONDS = max(0.0, env_float("DART_REQUEST_INTERVAL_SECONDS", 0.15))
REQUEST_COOLDOWN_EVERY = max(0, env_int("DART_REQUEST_COOLDOWN_EVERY", 60))
REQUEST_COOLDOWN_SECONDS = max(0.0, env_float("DART_REQUEST_COOLDOWN_SECONDS", 6.0))
REMOTE_DISCONNECT_RETRY_DELAY_SECONDS = max(
    RETRY_DELAY_SECONDS,
    env_float("DART_REMOTE_DISCONNECT_RETRY_DELAY_SECONDS", 12.0),
)


def current_html_pdf_renderer_mode() -> str:
    configured = normalized_env_value("DART_HTML_PDF_RENDERER")
    if configured:
        normalized = configured.lower()
        if normalized in VALID_HTML_PDF_RENDERERS:
            return normalized
    return HTML_PDF_RENDERER_BROWSER


def pdf_source_cache_renderer_variant() -> str:
    mode = current_html_pdf_renderer_mode()
    if mode == HTML_PDF_RENDERER_AUTO:
        return HTML_PDF_RENDERER_BROWSER
    return mode


def apply_proxy_env_aliases() -> None:
    shared_proxy = normalized_env_value("DART_PROXY_URL")
    http_proxy = normalized_env_value("DART_HTTP_PROXY") or shared_proxy
    https_proxy = normalized_env_value("DART_HTTPS_PROXY") or shared_proxy or http_proxy
    no_proxy = normalized_env_value("DART_NO_PROXY")

    if http_proxy:
        os.environ.setdefault("http_proxy", http_proxy)
        os.environ.setdefault("HTTP_PROXY", http_proxy)
    if https_proxy:
        os.environ.setdefault("https_proxy", https_proxy)
        os.environ.setdefault("HTTPS_PROXY", https_proxy)
    if no_proxy:
        os.environ.setdefault("no_proxy", no_proxy)
        os.environ.setdefault("NO_PROXY", no_proxy)


def active_proxy_map() -> dict[str, str]:
    proxy_map = getproxies()
    return {
        key: value
        for key, value in proxy_map.items()
        if key in {"http", "https", "all"} and isinstance(value, str) and value.strip()
    }


def proxy_status_message() -> str | None:
    active_keys = sorted(active_proxy_map())
    if not active_keys:
        return None
    return f"서버 프록시 사용 중 ({', '.join(active_keys)})"


apply_proxy_env_aliases()


class SimpleTableParser(HTMLParser):
    def __init__(self, table_id: str | None = None, select_id: str | None = None) -> None:
        super().__init__()
        self.table_id = table_id
        self.select_id = select_id
        self._in_target_table = False
        self._in_target_select = False
        self._table_depth = 0
        self._select_depth = 0
        self._current_row: list[str] | None = None
        self.rows: list[list[str]] = []
        self.options: list[tuple[str, str]] = []
        self._current_option_value: str | None = None
        self._buffer: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attr_map = dict(attrs)
        if tag == "table" and self.table_id and attr_map.get("id") == self.table_id:
            self._in_target_table = True
            self._table_depth = 1
            return
        if tag == "table" and self._in_target_table:
            self._table_depth += 1
        if tag == "select" and self.select_id and attr_map.get("id") == self.select_id:
            self._in_target_select = True
            self._select_depth = 1
            return
        if tag == "select" and self._in_target_select:
            self._select_depth += 1
        if self._in_target_table and tag == "tr":
            self._current_row = []
        if self._in_target_table and tag == "td":
            self._buffer = []
        if self._in_target_select and tag == "option":
            self._current_option_value = attr_map.get("value")
            self._buffer = []

    def handle_endtag(self, tag: str) -> None:
        if self._in_target_table and tag == "td":
            assert self._current_row is not None
            self._current_row.append(normalize_space("".join(self._buffer)))
            self._buffer = []
        if self._in_target_table and tag == "tr" and self._current_row is not None:
            if self._current_row:
                self.rows.append(self._current_row)
            self._current_row = None
        if self._in_target_table and tag == "table":
            self._table_depth -= 1
            if self._table_depth == 0:
                self._in_target_table = False
        if self._in_target_select and tag == "option" and self._current_option_value is not None:
            self.options.append((self._current_option_value, normalize_space("".join(self._buffer))))
            self._current_option_value = None
            self._buffer = []
        if self._in_target_select and tag == "select":
            self._select_depth -= 1
            if self._select_depth == 0:
                self._in_target_select = False

    def handle_data(self, data: str) -> None:
        if self._in_target_table and self._current_row is not None:
            self._buffer.append(data)
        if self._in_target_select and self._current_option_value is not None:
            self._buffer.append(data)


class DartSession:
    def __init__(
        self,
        *,
        verbose: bool = False,
        cancel_requested: Callable[[], bool] | None = None,
    ) -> None:
        self.verbose = verbose
        self.cancel_requested = cancel_requested
        self.cookie_jar = http.cookiejar.CookieJar()
        self.opener = build_opener(HTTPCookieProcessor(self.cookie_jar))
        self._request_started_at = 0.0
        self._request_attempts = 0

    def get_text(self, path_or_url: str, *, referer: str | None = None) -> str:
        data = self.get_bytes(path_or_url, referer=referer)
        return data.decode("utf-8", errors="ignore")

    def get_bytes(self, path_or_url: str, *, referer: str | None = None) -> bytes:
        return self._request("GET", path_or_url, referer=referer)

    def post_text(self, path_or_url: str, form: dict[str, str], *, referer: str | None = None) -> str:
        data = urlencode(form).encode("utf-8")
        response = self._request("POST", path_or_url, body=data, referer=referer)
        return response.decode("utf-8", errors="ignore")

    def _request(
        self,
        method: str,
        path_or_url: str,
        *,
        body: bytes | None = None,
        referer: str | None = None,
    ) -> bytes:
        url = path_or_url if path_or_url.startswith("http") else urljoin(BASE_URL, path_or_url)
        headers = {
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        }
        if referer:
            headers["Referer"] = referer if referer.startswith("http") else urljoin(BASE_URL, referer)
        if body is not None:
            headers["Content-Type"] = "application/x-www-form-urlencoded"
            headers["Origin"] = BASE_URL

        last_error: Exception | None = None
        for attempt in range(1, RETRY_COUNT + 1):
            raise_if_cancel_requested(self.cancel_requested)
            self._throttle_before_request(url)
            request = Request(url=url, data=body, headers=headers, method=method)
            try:
                self._request_attempts += 1
                self._request_started_at = time.monotonic()
                if self.verbose:
                    print(f"[HTTP] {method} {url}", file=sys.stderr)
                with self.opener.open(request, timeout=30) as response:
                    return response.read()
            except RETRYABLE_REQUEST_ERRORS as error:
                last_error = error
                raise_if_cancel_requested(self.cancel_requested)
                if self.verbose:
                    print(
                        f"[HTTP] retry {attempt}/{RETRY_COUNT} for {url}: {error}",
                        file=sys.stderr,
                    )
                if attempt == RETRY_COUNT:
                    break
                retry_delay = self._retry_delay_seconds(error, attempt)
                if self.verbose:
                    print(
                        f"[HTTP] cooldown {retry_delay:.1f}s before retry for {url}",
                        file=sys.stderr,
                    )
                sleep_with_cancellation(retry_delay, self.cancel_requested)
        assert last_error is not None
        raise RuntimeError(f"요청 실패: {url} ({last_error})") from last_error

    def _throttle_before_request(self, url: str) -> None:
        if self._request_started_at > 0 and REQUEST_MIN_INTERVAL_SECONDS > 0:
            next_allowed_at = self._request_started_at + REQUEST_MIN_INTERVAL_SECONDS
            remaining = next_allowed_at - time.monotonic()
            if remaining > 0:
                sleep_with_cancellation(remaining, self.cancel_requested)

        if REQUEST_COOLDOWN_EVERY > 0 and self._request_attempts > 0:
            if self._request_attempts % REQUEST_COOLDOWN_EVERY == 0:
                if self.verbose:
                    print(
                        f"[HTTP] request burst cooldown {REQUEST_COOLDOWN_SECONDS:.1f}s after "
                        f"{self._request_attempts} requests ({url})",
                        file=sys.stderr,
                    )
                sleep_with_cancellation(REQUEST_COOLDOWN_SECONDS, self.cancel_requested)

    def _retry_delay_seconds(self, error: Exception, attempt: int) -> float:
        delay = RETRY_DELAY_SECONDS * attempt
        if isinstance(error, (RemoteDisconnected, ConnectionResetError)):
            return max(delay, REMOTE_DISCONNECT_RETRY_DELAY_SECONDS * attempt)

        if isinstance(error, HTTPError) and error.code in {429, 500, 502, 503, 504}:
            return max(delay, REMOTE_DISCONNECT_RETRY_DELAY_SECONDS * attempt)

        return delay


def normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", html.unescape(text).replace("\xa0", " ")).strip()


def clean_html_text(fragment: str) -> str:
    text = re.sub(r"<br\s*/?>", " ", fragment, flags=re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    return normalize_space(text)


def date_to_search_value(date_text: str) -> str:
    return datetime.strptime(date_text, "%Y-%m-%d").strftime("%Y%m%d")


def date_to_file_value(date_text: str) -> str:
    return datetime.strptime(date_text, "%Y-%m-%d").strftime("%y%m%d")


def receipt_date_to_output_value(date_text: str) -> str:
    return datetime.strptime(date_text, "%Y.%m.%d").strftime("%Y-%m-%d")


def sanitize_filename_component(text: str) -> str:
    cleaned = re.sub(r'[\\/:*?"<>|]+', "-", text).strip()
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned or "공시"


def build_output_filename(company_name: str, start_date: str, end_date: str) -> str:
    safe_company_name = sanitize_filename_component(company_name)
    return (
        f"금감원공시-{safe_company_name}-"
        f"{date_to_file_value(start_date)}-{date_to_file_value(end_date)}.pdf"
    )


def build_output_path(company_name: str, start_date: str, end_date: str, output_dir: str) -> Path:
    return Path(output_dir).expanduser() / build_output_filename(company_name, start_date, end_date)


def build_output_path_for_disclosures(company_name: str, disclosures: list[Disclosure], output_dir: str) -> Path:
    earliest_date = min(receipt_date_to_output_value(item.receipt_date) for item in disclosures)
    latest_date = max(receipt_date_to_output_value(item.receipt_date) for item in disclosures)
    return build_output_path(company_name, earliest_date, latest_date, output_dir)


def prompt_text(prompt_label: str, default_value: str) -> str:
    while True:
        entered = input(f"{prompt_label} [{default_value}]: ").strip()
        value = entered or default_value
        if value:
            return value


def prompt_date(prompt_label: str, default_value: str) -> str:
    while True:
        value = prompt_text(f"{prompt_label} (YYYY-MM-DD)", default_value)
        try:
            datetime.strptime(value, "%Y-%m-%d")
        except ValueError:
            print("날짜 형식은 YYYY-MM-DD 여야 합니다.", file=sys.stderr)
            continue
        return value


def resolve_args(args: argparse.Namespace, config: AppConfig) -> argparse.Namespace:
    interactive = sys.stdin.isatty() and not args.no_prompt
    args.auto_output = args.output is None

    company_name = args.company_name or config.company_name
    start_date = args.start_date or DEFAULT_START_DATE
    end_date = args.end_date or DEFAULT_END_DATE
    output_dir = args.output_dir or config.output_dir

    if interactive:
        print("공시 조회 조건을 입력하세요. Enter를 누르면 기본값을 사용합니다.", file=sys.stderr)
        company_name = prompt_text("대상 회사명", company_name)
        start_date = prompt_date("조회 시작일", start_date)
        while True:
            end_date = prompt_date("조회 종료일", end_date)
            if start_date <= end_date:
                break
            print("조회 종료일은 조회 시작일보다 빠를 수 없습니다.", file=sys.stderr)
        if args.output is None:
            output_dir = prompt_text("저장 폴더", output_dir)

    args.company_name = company_name
    args.start_date = start_date
    args.end_date = end_date
    args.output_dir = output_dir

    if args.auto_output:
        args.output = str(build_output_path(company_name, start_date, end_date, output_dir))
        if interactive:
            print(f"저장 파일: {args.output}", file=sys.stderr)
    else:
        args.output = str(Path(args.output).expanduser())

    return args


def parse_total_pages(html_text: str) -> int:
    match = re.search(r'<div class="pageInfo">\[(\d+)/(\d+)\] \[총 [\d,]+건\]</div>', html_text)
    if not match:
        return 1
    return int(match.group(2))


def market_config_from_row(row_html: str) -> tuple[str, str]:
    class_match = re.search(r'<span class="([^"]*tagCom_[^"]*)"', row_html)
    class_name = class_match.group(1) if class_match else ""
    if "tagCom_kosdaq" in class_name:
        return ("/dsac001/mainK.do", "K")
    if "tagCom_konex" in class_name:
        return ("/dsac001/mainN.do", "N")
    if "tagCom_yuga" in class_name:
        return ("/dsac001/mainY.do", "Y")
    return ("/dsac001/mainAll.do", "A")


def parse_disclosure_rows(html_text: str) -> list[Disclosure]:
    row_pattern = re.compile(
        r"<tr>\s*"
        r"<td[^>]*>\s*\d+\s*</td>\s*"
        r"<td class=\"tL\">(?P<company_td>.*?)</td>\s*"
        r"<td class=\"tL\">(?P<report_td>.*?)</td>\s*"
        r"<td class=\"tL ellipsis\"[^>]*>(?P<presenter>.*?)</td>\s*"
        r"<td>(?P<date>\d{4}\.\d{2}\.\d{2})</td>\s*"
        r"<td>.*?</td>\s*"
        r"</tr>",
        flags=re.S,
    )

    disclosures: list[Disclosure] = []
    for row_match in row_pattern.finditer(html_text):
        row_html = row_match.group(0)
        company_td = row_match.group("company_td")
        report_td = row_match.group("report_td")

        corp_match = re.search(
            r"openCorpInfoNew\('(?P<corp_key>\d+)'.*?>\s*(?P<company_name>.*?)\s*</a>",
            company_td,
            flags=re.S,
        )
        report_match = re.search(
            r'/dsaf001/main\.do\?rcpNo=(?P<rcp_no>\d+)"[^>]*>(?P<title>.*?)</a>',
            report_td,
            flags=re.S,
        )
        if not corp_match or not report_match:
            continue

        market_page, page_grouping = market_config_from_row(row_html)
        disclosures.append(
            Disclosure(
                company_name=clean_html_text(corp_match.group("company_name")),
                corp_key=corp_match.group("corp_key"),
                market_page=market_page,
                page_grouping=page_grouping,
                report_title=clean_html_text(report_match.group("title")),
                presenter=clean_html_text(row_match.group("presenter")),
                receipt_date=row_match.group("date"),
                rcp_no=report_match.group("rcp_no"),
            )
        )
    return disclosures


def parse_stock_code(html_text: str) -> str:
    match = re.search(r"<th[^>]*>\s*<label[^>]*>종목코드</label>\s*</th>\s*<td>(.*?)</td>", html_text, flags=re.S)
    if not match:
        return ""
    return normalize_space(match.group(1))


def parse_main_dcm_no(html_text: str, rcp_no: str) -> str:
    match = re.search(rf"openPdfDownload\('{re.escape(rcp_no)}', '(\d+)'\)", html_text)
    if not match:
        raise RuntimeError(f"본문 dcmNo를 찾지 못했습니다: rcpNo={rcp_no}")
    return match.group(1)


def parse_attachment_docs(html_text: str) -> list[AttachmentDoc]:
    select_match = re.search(r'<select id="att"[^>]*>(.*?)</select>', html_text, flags=re.S)
    if not select_match:
        return []
    parser = SimpleTableParser(select_id="att")
    parser.feed(select_match.group(0))
    attachments: list[AttachmentDoc] = []
    for value, label in parser.options:
        if not value or value == "null":
            continue
        match = re.search(r"rcpNo=(\d+)&dcmNo=(\d+)", html.unescape(value))
        if not match:
            continue
        attachments.append(
            AttachmentDoc(
                rcp_no=match.group(1),
                dcm_no=match.group(2),
                title=label,
            )
        )
    return attachments


def parse_pdf_download_paths(html_text: str) -> list[str]:
    paths = re.findall(r'href="(/pdf/download/pdf\.do\?[^"]+)"', html_text)
    return [html.unescape(path) for path in paths]


def parse_zip_download_paths(html_text: str) -> list[str]:
    paths = re.findall(r'href="(/pdf/download/zip\.do\?[^"]+)"', html_text)
    return [html.unescape(path) for path in paths]


def parse_download_assets(html_text: str) -> list[DownloadAsset]:
    assets: list[DownloadAsset] = []
    assets.extend(DownloadAsset(kind="pdf", path=path) for path in parse_pdf_download_paths(html_text))
    assets.extend(DownloadAsset(kind="zip", path=path) for path in parse_zip_download_paths(html_text))
    return assets


def resolve_html_pdf_browser() -> str | None:
    env_candidates = [
        os.getenv("HTML_TO_PDF_BROWSER"),
        os.getenv("CHROME_BIN"),
    ]
    for candidate in env_candidates:
        if candidate and Path(candidate).exists():
            return candidate

    system_name = platform.system()
    if system_name == "Darwin":
        path_candidates = [
            "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
            "/Applications/Chromium.app/Contents/MacOS/Chromium",
        ]
    elif system_name == "Windows":
        path_candidates = [
            r"C:\Program Files\Google\Chrome\Application\chrome.exe",
            r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
            r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
            r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
        ]
    else:
        path_candidates = []

    for candidate in path_candidates:
        if Path(candidate).exists():
            return candidate

    which_candidates = [
        "chromium",
        "chromium-browser",
        "google-chrome",
        "chrome",
        "msedge",
    ]
    for candidate in which_candidates:
        resolved = shutil.which(candidate)
        if resolved:
            return resolved
    return None


def inject_print_css(html_bytes: bytes) -> bytes:
    lower_html = html_bytes.lower()
    head_end = lower_html.find(b"</head>")
    if head_end >= 0:
        return html_bytes[:head_end] + HTML_PRINT_CSS + html_bytes[head_end:]
    body_start = lower_html.find(b"<body")
    if body_start >= 0:
        return html_bytes[:body_start] + HTML_PRINT_CSS + html_bytes[body_start:]
    return HTML_PRINT_CSS + html_bytes


class BrowserPdfRenderer:
    def __init__(self, browser_path: str) -> None:
        self.browser_path = browser_path
        self._lock = threading.Lock()
        self._process: subprocess.Popen[str] | None = None
        self._debug_port: int | None = None
        self._profile_dir: tempfile.TemporaryDirectory[str] | None = None
        self._devtools_opener = build_opener(ProxyHandler({}))
        self._message_seq = 0

    def close(self) -> None:
        with self._lock:
            self._shutdown_locked()

    def prewarm(self, *, cancel_requested: Callable[[], bool] | None = None) -> None:
        self._acquire_lock(cancel_requested)
        try:
            self._ensure_started_locked(cancel_requested)
        finally:
            self._lock.release()

    def render_html_bytes(
        self,
        html_bytes: bytes,
        *,
        cancel_requested: Callable[[], bool] | None = None,
    ) -> bytes:
        with tempfile.TemporaryDirectory(prefix="dart-html-pdf-") as temp_dir_name:
            temp_dir = Path(temp_dir_name)
            html_path = temp_dir / "document.html"
            html_path.write_bytes(inject_print_css(html_bytes))
            return self.render_html_file(html_path, cancel_requested=cancel_requested)

    def render_html_file(
        self,
        html_path: Path,
        *,
        cancel_requested: Callable[[], bool] | None = None,
    ) -> bytes:
        last_error: Exception | None = None
        for _ in range(2):
            self._acquire_lock(cancel_requested)
            try:
                self._ensure_started_locked(cancel_requested)
                return self._render_html_file_locked(html_path, cancel_requested)
            except JobCancelledError:
                raise
            except Exception as error:
                last_error = error
                self._shutdown_locked()
            finally:
                self._lock.release()

        assert last_error is not None
        raise RuntimeError(f"브라우저 HTML PDF 변환 실패: {last_error}") from last_error

    def _acquire_lock(self, cancel_requested: Callable[[], bool] | None) -> None:
        while not self._lock.acquire(timeout=BROWSER_LOCK_WAIT_SECONDS):
            raise_if_cancel_requested(cancel_requested)

    def _ensure_started_locked(self, cancel_requested: Callable[[], bool] | None) -> None:
        if self._process is not None and self._process.poll() is None and self._debug_port is not None:
            try:
                self._devtools_json_locked("GET", "/json/version", timeout=1.0)
                return
            except Exception:
                self._shutdown_locked()

        self._start_browser_locked(cancel_requested)

    def _start_browser_locked(self, cancel_requested: Callable[[], bool] | None) -> None:
        self._shutdown_locked()
        raise_if_cancel_requested(cancel_requested)

        self._profile_dir = tempfile.TemporaryDirectory(
            prefix="dart-browser-profile-",
            ignore_cleanup_errors=True,
        )
        self._debug_port = self._pick_free_port()
        command = [
            self.browser_path,
            "--headless=new",
            "--disable-gpu",
            "--disable-dev-shm-usage",
            "--allow-file-access-from-files",
            f"--user-data-dir={self._profile_dir.name}",
            f"--remote-debugging-port={self._debug_port}",
            "about:blank",
        ]
        if platform.system() == "Linux":
            command.insert(1, "--no-sandbox")

        self._process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        deadline = time.time() + BROWSER_START_TIMEOUT_SECONDS
        while time.time() < deadline:
            raise_if_cancel_requested(cancel_requested)
            process = self._process
            if process is None:
                break
            if process.poll() is not None:
                stdout_text, stderr_text = process.communicate()
                self._shutdown_locked()
                error_text = (stderr_text or stdout_text or "").strip()
                raise RuntimeError(f"브라우저 시작 실패: {error_text or process.returncode}")
            try:
                self._devtools_json_locked("GET", "/json/version", timeout=1.0)
                return
            except Exception:
                time.sleep(0.1)

        self._shutdown_locked()
        raise RuntimeError("브라우저 DevTools 준비 시간 초과")

    def _shutdown_locked(self) -> None:
        process = self._process
        if process is not None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        self._process = None
        self._debug_port = None
        self._message_seq = 0
        if self._profile_dir is not None:
            self._profile_dir.cleanup()
            self._profile_dir = None

    def _pick_free_port(self) -> int:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.bind(("127.0.0.1", 0))
            return int(sock.getsockname()[1])
        finally:
            sock.close()

    def _devtools_json_locked(
        self,
        method: str,
        path: str,
        *,
        timeout: float = 5.0,
    ) -> dict[str, object]:
        if self._debug_port is None:
            raise RuntimeError("브라우저 디버그 포트가 준비되지 않았습니다.")
        request = Request(
            f"http://127.0.0.1:{self._debug_port}{path}",
            method=method,
            headers={"Accept": "application/json"},
        )
        with self._devtools_opener.open(request, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8"))

    def _render_html_file_locked(
        self,
        html_path: Path,
        cancel_requested: Callable[[], bool] | None,
    ) -> bytes:
        target = self._devtools_json_locked("PUT", "/json/new?about:blank")
        target_id = str(target.get("id") or "")
        websocket_url = target.get("webSocketDebuggerUrl")
        if not isinstance(websocket_url, str) or not websocket_url:
            raise RuntimeError("브라우저 대상 페이지를 만들지 못했습니다.")

        try:
            with websocket_connect(
                websocket_url,
                open_timeout=5,
                close_timeout=5,
                max_size=None,
                proxy=None,
            ) as websocket:
                self._send_command_and_wait_for_response_locked(
                    websocket,
                    "Page.enable",
                    cancel_requested=cancel_requested,
                )
                self._navigate_and_wait_for_load_locked(
                    websocket,
                    html_path.resolve().as_uri(),
                    cancel_requested=cancel_requested,
                )
                result = self._send_command_and_wait_for_response_locked(
                    websocket,
                    "Page.printToPDF",
                    params={
                        "printBackground": True,
                        "preferCSSPageSize": True,
                        "paperWidth": 8.27,
                        "paperHeight": 11.69,
                        "marginTop": 0,
                        "marginBottom": 0,
                        "marginLeft": 0,
                        "marginRight": 0,
                    },
                    cancel_requested=cancel_requested,
                )
        finally:
            if target_id:
                try:
                    self._devtools_json_locked("GET", f"/json/close/{target_id}")
                except Exception:
                    pass

        result_map = result.get("result")
        if not isinstance(result_map, dict):
            raise RuntimeError("브라우저 PDF 변환 응답이 올바르지 않습니다.")
        pdf_data = result_map.get("data")
        if not isinstance(pdf_data, str) or not pdf_data:
            raise RuntimeError("브라우저 PDF 데이터가 비어 있습니다.")
        return base64.b64decode(pdf_data)

    def _send_command_and_wait_for_response_locked(
        self,
        websocket,
        method: str,
        *,
        params: dict[str, object] | None = None,
        cancel_requested: Callable[[], bool] | None = None,
        timeout: float = BROWSER_COMMAND_TIMEOUT_SECONDS,
    ) -> dict[str, object]:
        self._message_seq += 1
        message_id = self._message_seq
        websocket.send(json.dumps({"id": message_id, "method": method, "params": params or {}}))
        deadline = time.time() + timeout
        while True:
            message = self._receive_websocket_message_locked(
                websocket,
                cancel_requested=cancel_requested,
                deadline=deadline,
            )
            if message.get("id") != message_id:
                continue
            if "error" in message:
                raise RuntimeError(f"브라우저 명령 실패 ({method}): {message['error']}")
            return message

    def _navigate_and_wait_for_load_locked(
        self,
        websocket,
        url: str,
        *,
        cancel_requested: Callable[[], bool] | None = None,
    ) -> None:
        self._message_seq += 1
        message_id = self._message_seq
        websocket.send(json.dumps({"id": message_id, "method": "Page.navigate", "params": {"url": url}}))

        deadline = time.time() + BROWSER_PAGE_LOAD_TIMEOUT_SECONDS
        got_response = False
        got_load_event = False
        while not (got_response and got_load_event):
            message = self._receive_websocket_message_locked(
                websocket,
                cancel_requested=cancel_requested,
                deadline=deadline,
            )
            if message.get("id") == message_id:
                if "error" in message:
                    raise RuntimeError(f"브라우저 페이지 이동 실패: {message['error']}")
                got_response = True
                continue
            if message.get("method") == "Page.loadEventFired":
                got_load_event = True

    def _receive_websocket_message_locked(
        self,
        websocket,
        *,
        cancel_requested: Callable[[], bool] | None = None,
        deadline: float | None = None,
    ) -> dict[str, object]:
        while True:
            raise_if_cancel_requested(cancel_requested)
            timeout = BROWSER_LOCK_WAIT_SECONDS
            if deadline is not None:
                remaining = deadline - time.time()
                if remaining <= 0:
                    raise RuntimeError("브라우저 응답 대기 시간 초과")
                timeout = min(timeout, remaining)
            try:
                raw_message = websocket.recv(timeout=timeout)
            except TimeoutError:
                continue
            except ConnectionClosed as error:
                raise RuntimeError(f"브라우저 WebSocket 연결이 종료되었습니다: {error}") from error
            return json.loads(raw_message)


class PdfSourceCache:
    def __init__(self, root_dir: Path) -> None:
        self.root_dir = root_dir
        self._locks: dict[str, threading.Lock] = {}
        self._locks_guard = threading.Lock()

    def load(
        self,
        *,
        rcp_no: str,
        dcm_no: str,
        label: str,
        renderer_mode: str,
    ) -> list[PdfSource] | None:
        cache_path = self._entry_path(
            rcp_no=rcp_no,
            dcm_no=dcm_no,
            label=label,
            renderer_mode=renderer_mode,
        )
        if not cache_path.exists():
            return None
        try:
            return self._read_entry(cache_path)
        except Exception:
            cache_path.unlink(missing_ok=True)
            return None

    def store(
        self,
        sources: list[PdfSource],
        *,
        rcp_no: str,
        dcm_no: str,
        label: str,
        renderer_mode: str,
    ) -> None:
        cache_path = self._entry_path(
            rcp_no=rcp_no,
            dcm_no=dcm_no,
            label=label,
            renderer_mode=renderer_mode,
        )
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            dir=cache_path.parent,
            prefix=f"{cache_path.stem}-",
            suffix=".tmp",
            delete=False,
        ) as temp_file:
            temp_path = Path(temp_file.name)
        try:
            with zipfile.ZipFile(temp_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
                manifest_items: list[dict[str, str]] = []
                for index, source in enumerate(sources, start=1):
                    filename = f"source-{index:03d}.pdf"
                    archive.writestr(filename, source.bytes_data)
                    manifest_items.append({"filename": filename, "label": source.label})
                archive.writestr(
                    "manifest.json",
                    json.dumps(
                        {
                            "cache_version": PDF_SOURCE_CACHE_VERSION,
                            "created_at": datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
                            "items": manifest_items,
                        },
                        ensure_ascii=False,
                    ).encode("utf-8"),
                )
            os.replace(temp_path, cache_path)
        finally:
            temp_path.unlink(missing_ok=True)

    def acquire_entry_lock(
        self,
        *,
        rcp_no: str,
        dcm_no: str,
        label: str,
        renderer_mode: str,
        cancel_requested: Callable[[], bool] | None = None,
    ) -> threading.Lock:
        key = self._entry_key(
            rcp_no=rcp_no,
            dcm_no=dcm_no,
            label=label,
            renderer_mode=renderer_mode,
        )
        with self._locks_guard:
            lock = self._locks.setdefault(key, threading.Lock())
        while not lock.acquire(timeout=CACHE_LOCK_WAIT_SECONDS):
            raise_if_cancel_requested(cancel_requested)
        return lock

    def _entry_path(self, *, rcp_no: str, dcm_no: str, label: str, renderer_mode: str) -> Path:
        key = self._entry_key(
            rcp_no=rcp_no,
            dcm_no=dcm_no,
            label=label,
            renderer_mode=renderer_mode,
        )
        return self.root_dir / key[:2] / f"{key}.zip"

    def _entry_key(self, *, rcp_no: str, dcm_no: str, label: str, renderer_mode: str) -> str:
        payload = json.dumps(
            {
                "cache_version": PDF_SOURCE_CACHE_VERSION,
                "rcp_no": rcp_no,
                "dcm_no": dcm_no,
                "label": label,
                "renderer_mode": renderer_mode,
            },
            ensure_ascii=False,
            sort_keys=True,
        ).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()

    def _read_entry(self, cache_path: Path) -> list[PdfSource]:
        with zipfile.ZipFile(cache_path) as archive:
            manifest = json.loads(archive.read("manifest.json").decode("utf-8"))
            if manifest.get("cache_version") != PDF_SOURCE_CACHE_VERSION:
                raise RuntimeError("캐시 버전이 일치하지 않습니다.")
            items = manifest.get("items")
            if not isinstance(items, list) or not items:
                raise RuntimeError("캐시 항목이 비어 있습니다.")

            sources: list[PdfSource] = []
            for item in items:
                if not isinstance(item, dict):
                    raise RuntimeError("캐시 항목 형식이 올바르지 않습니다.")
                filename = item.get("filename")
                label = item.get("label")
                if not isinstance(filename, str) or not isinstance(label, str):
                    raise RuntimeError("캐시 항목 메타데이터가 올바르지 않습니다.")
                pdf_bytes = archive.read(filename)
                if not pdf_bytes.startswith(b"%PDF"):
                    raise RuntimeError(f"캐시 PDF 형식이 올바르지 않습니다: {filename}")
                sources.append(PdfSource(bytes_data=pdf_bytes, label=label))
            return sources


class ReportDetailCache:
    def __init__(self, root_dir: Path) -> None:
        self.root_dir = root_dir
        self._locks: dict[str, threading.Lock] = {}
        self._locks_guard = threading.Lock()

    def load(self, *, rcp_no: str) -> ReportDetail | None:
        cache_path = self._entry_path(rcp_no=rcp_no)
        if not cache_path.exists():
            return None
        try:
            return self._read_entry(cache_path)
        except Exception:
            cache_path.unlink(missing_ok=True)
            return None

    def store(self, detail: ReportDetail, *, rcp_no: str) -> None:
        cache_path = self._entry_path(rcp_no=rcp_no)
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            dir=cache_path.parent,
            prefix=f"{cache_path.stem}-",
            suffix=".tmp",
            delete=False,
        ) as temp_file:
            temp_path = Path(temp_file.name)
        try:
            payload = {
                "cache_version": REPORT_DETAIL_CACHE_VERSION,
                "main_dcm_no": detail.main_dcm_no,
                "attachments": [
                    {
                        "rcp_no": attachment.rcp_no,
                        "dcm_no": attachment.dcm_no,
                        "title": attachment.title,
                    }
                    for attachment in detail.attachments
                ],
            }
            temp_path.write_text(
                json.dumps(payload, ensure_ascii=False),
                encoding="utf-8",
            )
            os.replace(temp_path, cache_path)
        finally:
            temp_path.unlink(missing_ok=True)

    def acquire_entry_lock(
        self,
        *,
        rcp_no: str,
        cancel_requested: Callable[[], bool] | None = None,
    ) -> threading.Lock:
        key = self._entry_key(rcp_no=rcp_no)
        with self._locks_guard:
            lock = self._locks.setdefault(key, threading.Lock())
        while not lock.acquire(timeout=CACHE_LOCK_WAIT_SECONDS):
            raise_if_cancel_requested(cancel_requested)
        return lock

    def _entry_path(self, *, rcp_no: str) -> Path:
        key = self._entry_key(rcp_no=rcp_no)
        return self.root_dir / "report-detail" / key[:2] / f"{key}.json"

    def _entry_key(self, *, rcp_no: str) -> str:
        payload = json.dumps(
            {
                "cache_version": REPORT_DETAIL_CACHE_VERSION,
                "rcp_no": rcp_no,
            },
            ensure_ascii=False,
            sort_keys=True,
        ).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()

    def _read_entry(self, cache_path: Path) -> ReportDetail:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
        if payload.get("cache_version") != REPORT_DETAIL_CACHE_VERSION:
            raise RuntimeError("detail 캐시 버전이 일치하지 않습니다.")

        main_dcm_no = payload.get("main_dcm_no")
        attachments_raw = payload.get("attachments")
        if not isinstance(main_dcm_no, str) or not main_dcm_no:
            raise RuntimeError("detail 캐시 main_dcm_no가 올바르지 않습니다.")
        if not isinstance(attachments_raw, list):
            raise RuntimeError("detail 캐시 attachments가 올바르지 않습니다.")

        attachments: list[AttachmentDoc] = []
        for item in attachments_raw:
            if not isinstance(item, dict):
                raise RuntimeError("detail 캐시 attachment 항목 형식이 올바르지 않습니다.")
            rcp_no = item.get("rcp_no")
            dcm_no = item.get("dcm_no")
            title = item.get("title")
            if not isinstance(rcp_no, str) or not isinstance(dcm_no, str) or not isinstance(title, str):
                raise RuntimeError("detail 캐시 attachment 메타데이터가 올바르지 않습니다.")
            attachments.append(AttachmentDoc(rcp_no=rcp_no, dcm_no=dcm_no, title=title))
        return ReportDetail(main_dcm_no=main_dcm_no, attachments=attachments)


_BROWSER_PDF_RENDERER: BrowserPdfRenderer | None = None
_BROWSER_PDF_RENDERER_GUARD = threading.Lock()
_PDF_SOURCE_CACHE: PdfSourceCache | None = None
_PDF_SOURCE_CACHE_GUARD = threading.Lock()
_REPORT_DETAIL_CACHE: ReportDetailCache | None = None
_REPORT_DETAIL_CACHE_GUARD = threading.Lock()


def close_browser_pdf_renderer() -> None:
    global _BROWSER_PDF_RENDERER
    with _BROWSER_PDF_RENDERER_GUARD:
        renderer = _BROWSER_PDF_RENDERER
        _BROWSER_PDF_RENDERER = None
    if renderer is not None:
        renderer.close()


def get_browser_pdf_renderer() -> BrowserPdfRenderer | None:
    global _BROWSER_PDF_RENDERER
    if websocket_connect is None:
        return None

    browser = resolve_html_pdf_browser()
    if browser is None:
        return None

    with _BROWSER_PDF_RENDERER_GUARD:
        if _BROWSER_PDF_RENDERER is None or _BROWSER_PDF_RENDERER.browser_path != browser:
            if _BROWSER_PDF_RENDERER is not None:
                _BROWSER_PDF_RENDERER.close()
            _BROWSER_PDF_RENDERER = BrowserPdfRenderer(browser)
        return _BROWSER_PDF_RENDERER


def get_report_detail_cache() -> ReportDetailCache:
    global _REPORT_DETAIL_CACHE
    with _REPORT_DETAIL_CACHE_GUARD:
        if _REPORT_DETAIL_CACHE is None:
            app_config = load_app_config()
            _REPORT_DETAIL_CACHE = ReportDetailCache(Path(app_config.cache_dir).expanduser())
        return _REPORT_DETAIL_CACHE


def prewarm_browser_pdf_renderer(
    *,
    cancel_requested: Callable[[], bool] | None = None,
) -> bool:
    mode = current_html_pdf_renderer_mode()
    if mode == HTML_PDF_RENDERER_FITZ:
        return False
    renderer = get_browser_pdf_renderer()
    if renderer is None:
        return False
    renderer.prewarm(cancel_requested=cancel_requested)
    return True


def get_pdf_source_cache() -> PdfSourceCache:
    global _PDF_SOURCE_CACHE
    with _PDF_SOURCE_CACHE_GUARD:
        if _PDF_SOURCE_CACHE is None:
            app_config = load_app_config()
            _PDF_SOURCE_CACHE = PdfSourceCache(Path(app_config.cache_dir).expanduser())
        return _PDF_SOURCE_CACHE


atexit.register(close_browser_pdf_renderer)


def html_bytes_to_pdf_bytes_via_browser_subprocess(
    html_bytes: bytes,
    *,
    cancel_requested: Callable[[], bool] | None = None,
) -> bytes:
    raise_if_cancel_requested(cancel_requested)
    browser = resolve_html_pdf_browser()
    if browser is None:
        raise RuntimeError("HTML PDF 변환용 브라우저를 찾지 못했습니다.")

    with tempfile.TemporaryDirectory(prefix="dart-html-pdf-") as temp_dir_name:
        temp_dir = Path(temp_dir_name)
        html_path = temp_dir / "document.html"
        pdf_path = temp_dir / "document.pdf"
        html_path.write_bytes(inject_print_css(html_bytes))

        command = [
            browser,
            "--headless=new",
            "--disable-gpu",
            "--disable-dev-shm-usage",
            "--allow-file-access-from-files",
            "--no-pdf-header-footer",
            f"--print-to-pdf={pdf_path}",
            html_path.resolve().as_uri(),
        ]
        if platform.system() == "Linux":
            command.insert(1, "--no-sandbox")

        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        stdout_text = ""
        stderr_text = ""
        try:
            while True:
                raise_if_cancel_requested(cancel_requested)
                return_code = process.poll()
                if return_code is not None:
                    stdout_text, stderr_text = process.communicate()
                    break
                time.sleep(0.2)
        except JobCancelledError:
            process.kill()
            stdout_text, stderr_text = process.communicate()
            raise

        if return_code != 0 or not pdf_path.exists():
            error_text = (stderr_text or stdout_text or "").strip()
            raise RuntimeError(f"브라우저 HTML PDF 변환 실패: {error_text or return_code}")

        return pdf_path.read_bytes()


def html_bytes_to_pdf_bytes_via_browser(
    html_bytes: bytes,
    *,
    cancel_requested: Callable[[], bool] | None = None,
) -> bytes:
    raise_if_cancel_requested(cancel_requested)
    renderer = get_browser_pdf_renderer()
    if renderer is not None:
        try:
            return renderer.render_html_bytes(html_bytes, cancel_requested=cancel_requested)
        except JobCancelledError:
            raise
        except RuntimeError:
            pass

    return html_bytes_to_pdf_bytes_via_browser_subprocess(
        html_bytes,
        cancel_requested=cancel_requested,
    )


def html_bytes_to_pdf_bytes(
    html_bytes: bytes,
    *,
    cancel_requested: Callable[[], bool] | None = None,
) -> bytes:
    mode = current_html_pdf_renderer_mode()
    if mode in {HTML_PDF_RENDERER_BROWSER, HTML_PDF_RENDERER_AUTO}:
        try:
            return html_bytes_to_pdf_bytes_via_browser(html_bytes, cancel_requested=cancel_requested)
        except RuntimeError:
            pass
    raise_if_cancel_requested(cancel_requested)
    html_doc = fitz.open(stream=html_bytes, filetype="html")
    try:
        html_doc.layout(rect=A4_PAGE_RECT)
        raise_if_cancel_requested(cancel_requested)
        return html_doc.convert_to_pdf()
    finally:
        html_doc.close()


def extract_pdf_sources_from_zip(
    zip_bytes: bytes,
    *,
    label: str,
    cancel_requested: Callable[[], bool] | None = None,
) -> list[PdfSource]:
    sources: list[PdfSource] = []
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as archive:
        for info in archive.infolist():
            raise_if_cancel_requested(cancel_requested)
            if info.is_dir():
                continue

            filename = Path(info.filename).name or info.filename
            extension = Path(filename).suffix.lower()
            file_bytes = archive.read(info)

            if extension == ".pdf":
                if not file_bytes.startswith(b"%PDF"):
                    raise RuntimeError(f"ZIP 안의 PDF 형식이 올바르지 않습니다: {filename}")
                sources.append(PdfSource(bytes_data=file_bytes, label=f"{label} ({filename})"))
            elif extension in {".html", ".htm", ".xhtml"}:
                sources.append(
                    PdfSource(
                        bytes_data=html_bytes_to_pdf_bytes(
                            file_bytes,
                            cancel_requested=cancel_requested,
                        ),
                        label=f"{label} ({filename})",
                    )
                )

    if not sources:
        raise RuntimeError("ZIP 안에서 PDF 또는 HTML 문서를 찾지 못했습니다.")
    return sources


def fetch_company_stock_code(session: DartSession, corp_key: str) -> str:
    popup_html = session.get_text(f"/dsae001/selectPopup.ax?selectKey={corp_key}")
    stock_code = parse_stock_code(popup_html)
    if not stock_code:
        raise RuntimeError(f"종목코드를 찾지 못했습니다: corp_key={corp_key}")
    return stock_code


def collect_disclosure_batch(
    session: DartSession,
    *,
    company_name: str,
    start_date: str,
    end_date: str,
    limit: int | None = None,
    cancel_requested: Callable[[], bool] | None = None,
) -> DisclosureBatch:
    raise_if_cancel_requested(cancel_requested)
    disclosures = fetch_disclosures(
        session,
        company_name=company_name,
        start_date=start_date,
        end_date=end_date,
        cancel_requested=cancel_requested,
    )
    if not disclosures:
        raise RuntimeError("조건에 맞는 공시가 없습니다.")

    raise_if_cancel_requested(cancel_requested)
    stock_code = fetch_company_stock_code(session, disclosures[0].corp_key)
    for disclosure in disclosures:
        raise_if_cancel_requested(cancel_requested)
        disclosure.stock_code = stock_code

    disclosures.sort(key=lambda item: (item.receipt_date_key, item.rcp_no), reverse=True)

    if limit is not None:
        disclosures = disclosures[:limit]

    return DisclosureBatch(
        company_name=company_name,
        stock_code=stock_code,
        disclosures=disclosures,
    )


def fetch_disclosures(
    session: DartSession,
    *,
    company_name: str,
    start_date: str,
    end_date: str,
    cancel_requested: Callable[[], bool] | None = None,
) -> list[Disclosure]:
    start_value = date_to_search_value(start_date)
    end_value = date_to_search_value(end_date)
    referer = "/dsab007/main.do"
    raise_if_cancel_requested(cancel_requested)
    session.get_text(referer)
    disclosures: list[Disclosure] = []
    current_page = 1
    total_pages = 1

    while current_page <= total_pages:
        raise_if_cancel_requested(cancel_requested)
        html_text = session.post_text(
            "/dsab007/detailSearch.ax",
            {
                "currentPage": str(current_page),
                "maxResults": "100",
                "maxLinks": "10",
                "sort": "",
                "series": "",
                "textCrpCik": "",
                "lateKeyword": "",
                "keyword": "",
                "reportNamePopYn": "",
                "textkeyword": "",
                "businessCode": "all",
                "autoSearch": "Y",
                "option": "corp",
                "textCrpNm": company_name,
                "textCrpNm2": company_name,
                "textPresenterNm": "",
                "startDate": start_value,
                "endDate": end_value,
                "reportName": "",
                "reportName2": "",
                "tocSrch": "",
                "tocSrch2": "",
                "searchGubun": "SEARCH",
            },
            referer=referer,
        )
        disclosures.extend(parse_disclosure_rows(html_text))
        total_pages = parse_total_pages(html_text)
        current_page += 1
    return disclosures


def fetch_report_detail(session: DartSession, disclosure: Disclosure) -> tuple[str, list[AttachmentDoc], str]:
    raise_if_cancel_requested(session.cancel_requested)
    detail_url = f"/dsaf001/main.do?rcpNo={disclosure.rcp_no}"
    cache = get_report_detail_cache()
    cached_detail = cache.load(rcp_no=disclosure.rcp_no)
    if cached_detail is not None:
        if session.verbose:
            print(f"[CACHE] detail hit {disclosure.rcp_no}", file=sys.stderr)
        return detail_url, cached_detail.attachments, cached_detail.main_dcm_no

    cache_lock = cache.acquire_entry_lock(
        rcp_no=disclosure.rcp_no,
        cancel_requested=session.cancel_requested,
    )
    try:
        cached_detail = cache.load(rcp_no=disclosure.rcp_no)
        if cached_detail is not None:
            if session.verbose:
                print(f"[CACHE] detail hit-after-wait {disclosure.rcp_no}", file=sys.stderr)
            return detail_url, cached_detail.attachments, cached_detail.main_dcm_no

        html_text = session.get_text(detail_url)
        main_dcm_no = parse_main_dcm_no(html_text, disclosure.rcp_no)
        attachments = parse_attachment_docs(html_text)
        cache.store(
            ReportDetail(main_dcm_no=main_dcm_no, attachments=attachments),
            rcp_no=disclosure.rcp_no,
        )
        if session.verbose:
            print(f"[CACHE] detail stored {disclosure.rcp_no}", file=sys.stderr)
        return detail_url, attachments, main_dcm_no
    finally:
        cache_lock.release()


def fetch_pdf_sources(
    session: DartSession,
    *,
    detail_url: str,
    rcp_no: str,
    dcm_no: str,
    label: str,
    cancel_requested: Callable[[], bool] | None = None,
) -> list[PdfSource]:
    raise_if_cancel_requested(cancel_requested)
    cache = get_pdf_source_cache()
    renderer_mode = pdf_source_cache_renderer_variant()
    cached_sources = cache.load(
        rcp_no=rcp_no,
        dcm_no=dcm_no,
        label=label,
        renderer_mode=renderer_mode,
    )
    if cached_sources is not None:
        if session.verbose:
            print(f"[CACHE] hit {rcp_no}/{dcm_no} {label} ({renderer_mode})", file=sys.stderr)
        return cached_sources

    cache_lock = cache.acquire_entry_lock(
        rcp_no=rcp_no,
        dcm_no=dcm_no,
        label=label,
        renderer_mode=renderer_mode,
        cancel_requested=cancel_requested,
    )
    try:
        cached_sources = cache.load(
            rcp_no=rcp_no,
            dcm_no=dcm_no,
            label=label,
            renderer_mode=renderer_mode,
        )
        if cached_sources is not None:
            if session.verbose:
                print(
                    f"[CACHE] hit-after-wait {rcp_no}/{dcm_no} {label} ({renderer_mode})",
                    file=sys.stderr,
                )
            return cached_sources

        download_main_url = f"/pdf/download/main.do?rcp_no={rcp_no}&dcm_no={dcm_no}"
        download_main_html = session.get_text(download_main_url, referer=detail_url)
        download_assets = parse_download_assets(download_main_html)
        if not download_assets:
            raise RuntimeError(f"다운로드 링크를 찾지 못했습니다: {download_main_url}")

        sources: list[PdfSource] = []
        for asset in download_assets:
            raise_if_cancel_requested(cancel_requested)
            if asset.kind == "pdf":
                pdf_bytes = session.get_bytes(asset.path, referer=download_main_url)
                if not pdf_bytes.startswith(b"%PDF"):
                    raise RuntimeError(f"PDF 다운로드 실패: {asset.path}")
                sources.append(PdfSource(bytes_data=pdf_bytes, label=label))
                continue

            if asset.kind == "zip":
                zip_bytes = session.get_bytes(asset.path, referer=download_main_url)
                sources.extend(
                    extract_pdf_sources_from_zip(
                        zip_bytes,
                        label=label,
                        cancel_requested=cancel_requested,
                    )
                )
                continue

            raise RuntimeError(f"지원하지 않는 다운로드 형식입니다: {asset.kind}")

        cache.store(
            sources,
            rcp_no=rcp_no,
            dcm_no=dcm_no,
            label=label,
            renderer_mode=renderer_mode,
        )
        if session.verbose:
            print(f"[CACHE] stored {rcp_no}/{dcm_no} {label} ({renderer_mode})", file=sys.stderr)
        return sources
    finally:
        cache_lock.release()


def header_text(disclosure: Disclosure) -> str:
    stock_code = disclosure.stock_code or "UNKNOWN"
    return f"{disclosure.company_name}({stock_code}) · {disclosure.header_receipt_date} · {disclosure.report_title}"


def fit_rect_within(
    source_rect: fitz.Rect,
    bounds: fitz.Rect,
    *,
    allow_scale_up: bool = False,
    vertical_align: str = "top",
) -> fitz.Rect:
    scale = min(bounds.width / source_rect.width, bounds.height / source_rect.height)
    if not allow_scale_up:
        scale = min(scale, 1.0)
    fitted_width = source_rect.width * scale
    fitted_height = source_rect.height * scale
    x0 = bounds.x0 + (bounds.width - fitted_width) / 2
    if vertical_align == "center":
        y0 = bounds.y0 + (bounds.height - fitted_height) / 2
    else:
        y0 = bounds.y0
    return fitz.Rect(x0, y0, x0 + fitted_width, y0 + fitted_height)


def insert_page_header(page: fitz.Page, header: str) -> None:
    page.draw_rect(
        fitz.Rect(0, 0, A4_PAGE_WIDTH, HEADER_BAND_HEIGHT),
        fill=(1, 1, 1),
        color=(1, 1, 1),
    )
    header_html = (
        "<div style="
        "'font-family: sans-serif;"
        " font-size:7pt;"
        " color:#6b6b6b;"
        " white-space:nowrap;'>"
        f"{html.escape(header)}"
        "</div>"
    )
    spare_height, scale = page.insert_htmlbox(
        fitz.Rect(PAGE_MARGIN, 4, A4_PAGE_WIDTH - PAGE_MARGIN, HEADER_BAND_HEIGHT - 1),
        header_html,
        scale_low=0.75,
    )
    if spare_height < 0:
        page.insert_textbox(
            fitz.Rect(PAGE_MARGIN, 4, A4_PAGE_WIDTH - PAGE_MARGIN, HEADER_BAND_HEIGHT - 1),
            header,
            fontsize=HEADER_FONT_SIZE,
            fontname=HEADER_FONT_NAME,
            color=HEADER_TEXT_COLOR,
            align=fitz.TEXT_ALIGN_LEFT,
        )


def append_pdf_with_header(
    output_doc: fitz.Document,
    pdf_bytes: bytes,
    *,
    header: str,
    cancel_requested: Callable[[], bool] | None = None,
) -> None:
    with fitz.open(stream=pdf_bytes, filetype="pdf") as source_doc:
        for page_index in range(source_doc.page_count):
            raise_if_cancel_requested(cancel_requested)
            source_page = source_doc.load_page(page_index)
            source_rect = source_page.rect
            target_page = output_doc.new_page(
                width=A4_PAGE_WIDTH,
                height=A4_PAGE_HEIGHT,
            )
            source_bounds = fitz.Rect(
                0,
                0,
                A4_PAGE_WIDTH,
                A4_PAGE_HEIGHT,
            )
            target_page.show_pdf_page(
                fit_rect_within(
                    source_rect,
                    source_bounds,
                    allow_scale_up=False,
                    vertical_align="top",
                ),
                source_doc,
                page_index,
            )
            insert_page_header(target_page, header)


def populate_output_pdf(
    output_doc: fitz.Document,
    session: DartSession,
    *,
    disclosures: list[Disclosure],
    log_stream=sys.stderr,
    cancel_requested: Callable[[], bool] | None = None,
) -> None:
    total = len(disclosures)
    for index, disclosure in enumerate(disclosures, start=1):
        raise_if_cancel_requested(cancel_requested)
        if log_stream is not None:
            print(
                f"[{index}/{total}] {disclosure.receipt_date} {disclosure.report_title}",
                file=log_stream,
            )
        detail_url, attachments, main_dcm_no = fetch_report_detail(session, disclosure)
        disclosure.attachment_docs = attachments
        if log_stream is not None:
            if attachments:
                print(f"    본문 1건 + 첨부 {len(attachments)}건 처리 예정", file=log_stream)
            else:
                print("    본문만 처리 예정", file=log_stream)
            print("    본문 다운로드/병합 중...", file=log_stream)

        sources = fetch_pdf_sources(
            session,
            detail_url=detail_url,
            rcp_no=disclosure.rcp_no,
            dcm_no=main_dcm_no,
            label="본문",
            cancel_requested=cancel_requested,
        )
        if log_stream is not None and len(sources) > 1:
            print(f"    본문 소스 {len(sources)}건 병합 중...", file=log_stream)
        for source in sources:
            append_pdf_with_header(
                output_doc,
                source.bytes_data,
                header=header_text(disclosure),
                cancel_requested=cancel_requested,
            )

        for attachment_index, attachment in enumerate(attachments, start=1):
            raise_if_cancel_requested(cancel_requested)
            attachment_detail_url = f"/dsaf001/main.do?rcpNo={attachment.rcp_no}&dcmNo={attachment.dcm_no}"
            if log_stream is not None:
                print(
                    f"    첨부 {attachment_index}/{len(attachments)} 다운로드/병합 중: {attachment.title}",
                    file=log_stream,
                )
            attachment_sources = fetch_pdf_sources(
                session,
                detail_url=attachment_detail_url,
                rcp_no=attachment.rcp_no,
                dcm_no=attachment.dcm_no,
                label=attachment.title,
                cancel_requested=cancel_requested,
            )
            if log_stream is not None and len(attachment_sources) > 1:
                print(
                    f"    첨부 {attachment_index}/{len(attachments)} 소스 {len(attachment_sources)}건 병합 중...",
                    file=log_stream,
                )
            for source in attachment_sources:
                append_pdf_with_header(
                    output_doc,
                    source.bytes_data,
                    header=header_text(disclosure),
                    cancel_requested=cancel_requested,
                )


def build_output_pdf(
    session: DartSession,
    *,
    disclosures: list[Disclosure],
    output_path: Path,
    log_stream=sys.stderr,
    cancel_requested: Callable[[], bool] | None = None,
) -> None:
    raise_if_cancel_requested(cancel_requested)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_doc = fitz.open()
    try:
        populate_output_pdf(
            output_doc,
            session,
            disclosures=disclosures,
            log_stream=log_stream,
            cancel_requested=cancel_requested,
        )
        raise_if_cancel_requested(cancel_requested)
        if log_stream is not None:
            print(f"[저장] 최종 PDF 저장 중... (총 {output_doc.page_count}페이지)", file=log_stream)
        output_doc.save(
            output_path,
            garbage=4,
            deflate=True,
            clean=True,
        )
    finally:
        output_doc.close()


def build_output_pdf_bytes(
    session: DartSession,
    *,
    disclosures: list[Disclosure],
    log_stream=sys.stderr,
    cancel_requested: Callable[[], bool] | None = None,
) -> bytes:
    raise_if_cancel_requested(cancel_requested)
    output_doc = fitz.open()
    try:
        populate_output_pdf(
            output_doc,
            session,
            disclosures=disclosures,
            log_stream=log_stream,
            cancel_requested=cancel_requested,
        )
        raise_if_cancel_requested(cancel_requested)
        if log_stream is not None:
            print(f"[저장] 최종 PDF 바이트 생성 중... (총 {output_doc.page_count}페이지)", file=log_stream)
        return output_doc.tobytes(
            garbage=4,
            deflate=True,
            clean=True,
        )
    finally:
        output_doc.close()


def parse_args(argv: Iterable[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="DART 공시를 수집해 하나의 PDF로 병합합니다.",
    )
    parser.add_argument("--company-name", default=None, help="대상 회사명")
    parser.add_argument("--start-date", default=None, help="조회 시작일 (YYYY-MM-DD)")
    parser.add_argument("--end-date", default=None, help="조회 종료일 (YYYY-MM-DD)")
    parser.add_argument("--output", default=None, help="저장할 PDF 전체 경로")
    parser.add_argument("--output-dir", default=None, help="자동 파일명을 저장할 폴더")
    parser.add_argument("--limit", type=int, default=None, help="앞에서부터 지정 개수만 처리")
    parser.add_argument("--dry-run", action="store_true", help="메타데이터만 조회하고 PDF는 생성하지 않음")
    parser.add_argument("--no-prompt", action="store_true", help="CLI 입력 없이 옵션 또는 기본값 사용")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args(list(argv))


def validate_args(args: argparse.Namespace) -> None:
    for value, label in ((args.start_date, "start-date"), (args.end_date, "end-date")):
        try:
            datetime.strptime(value, "%Y-%m-%d")
        except ValueError as error:
            raise SystemExit(f"--{label} 형식이 잘못되었습니다: {value}") from error
    if args.start_date > args.end_date:
        raise SystemExit("시작일은 종료일보다 늦을 수 없습니다.")
    if args.limit is not None and args.limit < 1:
        raise SystemExit("--limit는 1 이상의 정수여야 합니다.")


def to_user_friendly_error_message(error: RuntimeError) -> str:
    message = str(error)
    if "Remote end closed connection without response" in message:
        return (
            "DART 서버가 현재 이 실행 환경의 네트워크에서 응답을 끊고 있습니다.\n"
            "이 메시지가 웹사이트에서 보이면 사용자의 PC가 아니라 호스팅 서버 쪽 접속 환경 문제일 가능성이 큽니다.\n"
            "로컬 CLI/EXE에서 보이면 현재 PC 또는 네트워크(IP 대역, VPN, 보안SW) 쪽 영향일 수 있습니다.\n"
            "브라우저에서도 `ERR_EMPTY_RESPONSE`가 보인다면 코드 문제가 아니라 접속 환경 문제입니다.\n"
            "확인 방법:\n"
            "1. 다른 네트워크(예: 모바일 핫스팟)에서 다시 시도\n"
            "2. 호스팅 환경이라면 서버 위치나 클라우드 IP 대역을 바꿔 재시도\n"
            "3. 회사 VPN/프록시/보안SW가 켜져 있으면 잠시 해제 후 재시도\n"
            "4. 잠시 후 다시 시도\n"
            "5. 같은 조건으로 다시 실행하면 이미 캐시된 공시는 재사용되어 앞부분 부담이 줄어듭니다\n"
            f"원본 오류: {message}"
        )
    return message


def main(argv: Iterable[str]) -> int:
    try:
        app_config = load_app_config()
        args = resolve_args(parse_args(argv), app_config)
        validate_args(args)
        session = DartSession(verbose=args.verbose)
        batch = collect_disclosure_batch(
            session,
            company_name=args.company_name,
            start_date=args.start_date,
            end_date=args.end_date,
            limit=args.limit,
        )

        if args.auto_output:
            previous_output = args.output
            args.output = str(build_output_path_for_disclosures(args.company_name, batch.disclosures, args.output_dir))
            if args.output != previous_output:
                print(f"저장 파일 업데이트: {args.output}", file=sys.stderr)

        print(
            f"공시 {len(batch.disclosures)}건 조회 완료: {args.company_name}({batch.stock_code}) "
            f"{args.start_date} ~ {args.end_date}",
            file=sys.stderr,
        )

        if args.dry_run:
            for disclosure in batch.disclosures:
                print(
                    f"{disclosure.receipt_date} | {disclosure.report_title} | "
                    f"{disclosure.presenter} | {disclosure.rcp_no}"
                )
            return 0

        build_output_pdf(
            session,
            disclosures=batch.disclosures,
            output_path=Path(args.output),
        )
        print(f"완료: {args.output}", file=sys.stderr)
        return 0
    except RuntimeError as error:
        raise SystemExit(to_user_friendly_error_message(error)) from error


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
