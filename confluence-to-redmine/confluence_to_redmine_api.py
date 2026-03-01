#!/usr/bin/env python3
"""
Confluence On-Prem API -> Redmine Wiki Migration Pipeline
(Fixed version - CQL page fetch, Jira key resolution, body_html bug)
"""

import argparse
import gc
import json
import os
import re
import sys
import time
import threading
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import quote, unquote

import requests

try:
    import openpyxl
    HAS_OPENPYXL = True
except ImportError:
    HAS_OPENPYXL = False


class ConfluenceHealthMonitor:
    """Background thread that pings Confluence every N seconds and tracks server health.
    Exposes a throttle factor that the migration loop can use to self-regulate."""

    def __init__(self, session, api_base, interval=30, warn_threshold=3.0, critical_threshold=8.0):
        self.session = session
        self.api_base = api_base
        self.interval = interval
        self.warn_threshold = warn_threshold          # seconds — start throttling
        self.critical_threshold = critical_threshold   # seconds — pause migration
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._last_response_time = 0.0
        self._consecutive_failures = 0
        self._paused = False
        self._thread = None

    def start(self):
        self._thread = threading.Thread(target=self._run, daemon=True, name="health-monitor")
        self._thread.start()
        print(f"[HEALTH] Monitor started — ping every {self.interval}s, warn>{self.warn_threshold}s, critical>{self.critical_threshold}s")

    def stop(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)

    def _run(self):
        while not self._stop.is_set():
            self._ping()
            self._stop.wait(self.interval)

    def _ping(self):
        try:
            t0 = time.time()
            resp = self.session.get(
                f"{self.api_base}/space", params={"limit": 1}, timeout=20,
            )
            elapsed = time.time() - t0
            with self._lock:
                self._last_response_time = elapsed
                if resp.status_code == 200:
                    self._consecutive_failures = 0
                    if elapsed > self.critical_threshold:
                        if not self._paused:
                            print(f"\n[HEALTH] CRITICAL: {elapsed:.1f}s response — server overloaded, pausing migration")
                        self._paused = True
                    elif elapsed > self.warn_threshold:
                        self._paused = False
                        print(f"\n[HEALTH] WARN: {elapsed:.1f}s response — server under pressure, throttling")
                    else:
                        if self._paused:
                            print(f"\n[HEALTH] OK: {elapsed:.1f}s response — server recovered, resuming")
                        self._paused = False
                else:
                    self._consecutive_failures += 1
                    print(f"\n[HEALTH] Server returned {resp.status_code} ({elapsed:.1f}s)")
                    if self._consecutive_failures >= 3:
                        self._paused = True
        except Exception as e:
            with self._lock:
                self._consecutive_failures += 1
                self._last_response_time = 20.0
                if self._consecutive_failures >= 2:
                    if not self._paused:
                        print(f"\n[HEALTH] Server unreachable ({e}) — pausing migration")
                    self._paused = True

    @property
    def is_paused(self):
        with self._lock:
            return self._paused

    @property
    def response_time(self):
        with self._lock:
            return self._last_response_time

    def wait_if_paused(self, timeout=300):
        """Block until server recovers or timeout. Returns True if recovered."""
        if not self.is_paused:
            return True
        print(f"[HEALTH] Waiting for server to recover (up to {timeout}s)...", flush=True)
        waited = 0
        while self.is_paused and waited < timeout:
            time.sleep(5)
            waited += 5
        if self.is_paused:
            print(f"[HEALTH] Server still down after {timeout}s — continuing anyway")
            return False
        print(f"[HEALTH] Server recovered after {waited}s")
        return True

    @property
    def throttle_delay(self):
        """Extra delay (seconds) to add between requests based on server health."""
        rt = self.response_time
        if rt > self.critical_threshold:
            return 10.0
        elif rt > self.warn_threshold:
            return 5.0
        return 0.0


class ConfluenceClient:
    """REST API client for on-prem Confluence."""

    def __init__(self, base_url, username=None, password=None, pat=None,
                 cookies=None, cookie_file=None, verify_ssl=False, max_connections=4):
        self.base_url = base_url.rstrip("/")
        self.verify_ssl = verify_ssl
        self.max_connections = max_connections
        self.session = requests.Session()
        self.session.verify = self.verify_ssl

        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry
        retry = Retry(total=3, backoff_factor=0.5,
                       status_forcelist=[429, 502, 503, 504],
                       allowed_methods=["GET"])
        adapter = HTTPAdapter(max_retries=retry,
                              pool_connections=max_connections,
                              pool_maxsize=max_connections)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

        if not verify_ssl:
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        if pat:
            self.session.headers["Authorization"] = f"Bearer {pat}"
            print(f"[INFO] Using Personal Access Token (Bearer auth)")
        elif cookie_file:
            with open(cookie_file, "r") as f:
                cookie_data = json.load(f)
            if isinstance(cookie_data, dict):
                self.session.cookies.update(cookie_data)
            elif isinstance(cookie_data, list):
                for c in cookie_data:
                    self.session.cookies.set(c.get("name", ""), c.get("value", ""))
            print(f"[INFO] Using cookie auth ({len(self.session.cookies)} cookies)")
        elif cookies:
            for part in cookies.split(";"):
                part = part.strip()
                if "=" in part:
                    name, value = part.split("=", 1)
                    self.session.cookies.set(name.strip(), value.strip())
            print(f"[INFO] Using cookie auth ({len(self.session.cookies)} cookies)")
        elif username and password:
            import base64
            creds = base64.b64encode(f"{username}:{password}".encode()).decode()
            self.session.headers["Authorization"] = f"Basic {creds}"
            print(f"[INFO] Using preemptive basic auth as {username}")

        self.api_base = f"{self.base_url}/rest/api"
        self._verify_connection()

    def _verify_connection(self):
        try:
            resp = self.session.get(f"{self.api_base}/space", params={"limit": 1}, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                print(f"[INFO] Confluence API connected: {self.api_base} ({data.get('size', '?')} spaces in test)")
            else:
                auth_header = self.session.headers.get("Authorization", "none")
                auth_type = auth_header.split(" ")[0] if auth_header != "none" else "none"
                print(f"[WARN] Confluence API returned {resp.status_code} (auth: {auth_type})")
                print(f"[WARN] Response: {resp.text[:200]}")
        except Exception as e:
            print(f"[ERROR] Cannot reach Confluence: {e}")
            sys.exit(1)

    def get_all_spaces(self):
        """Fetch ALL spaces (no type filter - includes personal/archived)."""
        spaces = []
        start = 0
        limit = 100
        while True:
            resp = self.session.get(
                f"{self.api_base}/space",
                params={"start": start, "limit": limit, "expand": "description.plain"},
            )
            resp.raise_for_status()
            data = resp.json()
            results = data.get("results", [])
            spaces.extend(results)
            print(f"  Fetched {len(spaces)} spaces...", end="\r")
            if data.get("size", 0) < limit:
                break
            start += limit
            time.sleep(0.05)
        print(f"[INFO] Found {len(spaces)} Confluence spaces (all types)")
        return spaces

    def get_space_pages(self, space_key, expand="version,ancestors", with_history=False):
        """Fetch ALL pages via CQL (includes nested child pages).
        Default expand is lightweight (no body) to minimize server heap usage."""
        pages = []
        print(f"  [{space_key}] Fetching all pages via CQL...", end="\r")
        start = 0
        limit = 25
        cql = f'space="{space_key}" AND type=page'
        while True:
            resp = self.session.get(
                f"{self.api_base}/content/search",
                params={"cql": cql, "start": start, "limit": limit, "expand": expand},
            )
            if resp.status_code in (400, 403):
                print(f"  [{space_key}] CQL failed ({resp.status_code}), using fallback...")
                return self._get_space_pages_fallback(space_key, expand)
            if resp.status_code == 404:
                print(f"  [WARN] Space {space_key} not found or no access")
                return []
            resp.raise_for_status()
            data = resp.json()
            results = data.get("results", [])
            pages.extend(results)
            print(f"  [{space_key}] Fetched {len(pages)} pages...", end="\r")
            total_size = data.get("totalSize", 0)
            if len(pages) >= total_size or len(results) < limit:
                break
            start += limit
            time.sleep(0.05)
        if not pages:
            print(f"  [{space_key}] CQL returned 0, trying fallback...")
            return self._get_space_pages_fallback(space_key, expand)
        print(f"  [{space_key}] {len(pages)} pages total (CQL)         ")
        return pages

    def _get_space_pages_fallback(self, space_key, expand="version,ancestors"):
        """Fallback: paginated fetch with depth=all + recursive child discovery."""
        pages = []
        start = 0
        limit = 25
        while True:
            resp = self.session.get(
                f"{self.api_base}/space/{space_key}/content/page",
                params={"start": start, "limit": limit, "expand": expand, "depth": "all"},
            )
            if resp.status_code == 404:
                return []
            resp.raise_for_status()
            data = resp.json()
            results = data.get("results", [])
            if not results:
                results = data.get("page", {}).get("results", [])
            pages.extend(results)
            print(f"  [{space_key}] Fetched {len(pages)} pages...", end="\r")
            if data.get("size", len(results)) < limit:
                break
            start += limit
            time.sleep(0.05)
        if pages:
            n = len(pages)
            pages = self._ensure_all_children(pages, expand)
            if len(pages) > n:
                print(f"  [{space_key}] +{len(pages)-n} nested pages discovered")
        print(f"  [{space_key}] {len(pages)} pages total (fallback)     ")
        return pages

    def _ensure_all_children(self, pages, expand):
        """Recursively fetch child pages that depth=all might have missed."""
        seen = {p["id"] for p in pages}
        queue = list(pages)
        all_p = list(pages)
        while queue:
            pg = queue.pop(0)
            cs = 0
            while True:
                r = self.session.get(
                    f"{self.api_base}/content/{pg['id']}/child/page",
                    params={"start": cs, "limit": 25, "expand": expand},
                )
                if r.status_code != 200:
                    break
                d = r.json()
                kids = d.get("results", [])
                for kid in kids:
                    if kid["id"] not in seen:
                        seen.add(kid["id"])
                        all_p.append(kid)
                        queue.append(kid)
                if d.get("size", len(kids)) < 25:
                    break
                cs += 25
                time.sleep(0.02)
        return all_p

    def get_page_versions(self, page_id):
        """Fetch version list (metadata only — no content bodies).
        Uses /content/{id}/version to get real version numbers (handles gaps
        from deleted/purged versions). Falls back to /history + range."""
        versions = []
        start = 0
        limit = 50  # small pages to reduce server-side heap pressure
        try:
            while True:
                resp = self.session.get(
                    f"{self.api_base}/content/{page_id}/version",
                    params={"start": start, "limit": limit},
                    timeout=30,
                )
                if resp.status_code != 200:
                    break
                data = resp.json()
                results = data.get("results", [])
                for v in results:
                    versions.append({
                        "number": v.get("number", 0),
                        "by": v.get("by", {}),
                        "when": v.get("when", ""),
                        "message": v.get("message", ""),
                    })
                if data.get("size", len(results)) < limit:
                    break
                start += limit
                time.sleep(0.1)
        except Exception:
            versions = []

        if versions:
            versions.sort(key=lambda v: v["number"])
            return versions

        # Fallback: use /history endpoint (very lightweight — single GET, no content)
        resp = self.session.get(f"{self.api_base}/content/{page_id}/history", timeout=15)
        if resp.status_code != 200:
            return []
        hdata = resp.json()
        latest_num = hdata.get("lastUpdated", {}).get("number", 1)
        versions = []
        for v_num in range(1, latest_num + 1):
            ver_info = {"number": v_num}
            if v_num == latest_num:
                lu = hdata.get("lastUpdated", {})
                ver_info["by"] = lu.get("by", {})
                ver_info["when"] = lu.get("when", "")
            versions.append(ver_info)
        return versions

    def get_version_count(self, page_id):
        """Get just the version count using /history (single lightweight GET).
        Returns the latest version number without loading any version content."""
        try:
            resp = self.session.get(
                f"{self.api_base}/content/{page_id}/history",
                timeout=15,
            )
            if resp.status_code == 200:
                return resp.json().get("lastUpdated", {}).get("number", 1)
        except Exception:
            pass
        return 1

    def get_page_version_body(self, page_id, version_number, use_view=False):
        """Fetch a specific version's body. use_view=True requests body.view (expensive
        server-side render), use_view=False requests only body.storage (raw XML, fast)."""
        expand = "body.view,body.storage,version" if use_view else "body.storage,version"
        try:
            resp = self.session.get(
                f"{self.api_base}/content/{page_id}",
                params={"expand": expand, "status": "historical", "version": version_number},
                timeout=30,
            )
            if resp.status_code == 200:
                return resp.json()
        except Exception:
            pass
        return None

    def fetch_version_bodies_sequential(self, page_id, version_numbers, request_delay=0.3):
        """Fetch version bodies one at a time with a delay between each request.
        Sequential to avoid piling up heap pressure on constrained servers.
        Returns ({ver_num: data}, [errors])."""
        results = {}
        errors = []
        consecutive_fails = 0
        for vn in version_numbers:
            success = False
            for attempt in range(3):
                try:
                    resp = self.session.get(
                        f"{self.api_base}/content/{page_id}",
                        params={"expand": "body.storage,version", "status": "historical", "version": vn},
                        timeout=30,
                    )
                    if resp.status_code == 200:
                        results[vn] = resp.json()
                        consecutive_fails = 0
                        success = True
                        break
                    elif resp.status_code == 404:
                        errors.append(f"v{vn}:404_not_found")
                        success = True  # not retryable, but not a server issue
                        break
                    elif resp.status_code in (429, 503):
                        time.sleep(3.0 * (attempt + 1))
                        continue
                    else:
                        if attempt < 2:
                            time.sleep(1.0 * (attempt + 1))
                            continue
                        errors.append(f"v{vn}:http_{resp.status_code}")
                        break
                except Exception as e:
                    if attempt < 2:
                        time.sleep(1.0 * (attempt + 1))
                    else:
                        errors.append(f"v{vn}:error_{type(e).__name__}")
            if not success:
                consecutive_fails += 1
                if consecutive_fails >= 5:
                    errors.append("circuit_breaker:5_consecutive_fails")
                    break
            # Delay between each request to let server breathe
            time.sleep(request_delay)
        return results, errors

    def get_page_attachments(self, page_id):
        attachments = []
        start = 0
        limit = 50
        while True:
            resp = self.session.get(
                f"{self.api_base}/content/{page_id}/child/attachment",
                params={"start": start, "limit": limit},
            )
            if resp.status_code != 200:
                break
            data = resp.json()
            results = data.get("results", [])
            attachments.extend(results)
            if data.get("size", 0) < limit:
                break
            start += limit
            time.sleep(0.05)
        return attachments

    def download_attachment(self, download_path, output_dir):
        url = self.base_url + download_path
        resp = self.session.get(url, stream=True)
        if resp.status_code != 200:
            return None
        os.makedirs(output_dir, exist_ok=True)
        cd = resp.headers.get("Content-Disposition", "")
        fn_match = re.search(r'filename[*]?="?([^";]+)', cd)
        if fn_match:
            filename = fn_match.group(1).strip('"')
        else:
            filename = download_path.split("/")[-1].split("?")[0]
        filepath = os.path.join(output_dir, filename)
        with open(filepath, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
        return filepath


class RedmineClient:
    """REST API client for Redmine."""

    def __init__(self, base_url, api_key):
        self.base_url = base_url.rstrip("/")
        self.headers = {"X-Redmine-API-Key": api_key}

    def get_projects(self):
        projects = {}
        offset = 0
        limit = 100
        while True:
            resp = requests.get(f"{self.base_url}/projects.json", headers=self.headers, params={"offset": offset, "limit": limit})
            resp.raise_for_status()
            data = resp.json()
            for p in data["projects"]:
                projects[p["identifier"]] = p
            if offset + limit >= data["total_count"]:
                break
            offset += limit
        return projects

    def create_project(self, name, identifier, parent_id=None):
        payload = {"project": {"name": name, "identifier": identifier, "is_public": False,
                    "enabled_module_names": ["issue_tracking", "time_tracking", "wiki", "documents", "files", "news"]}}
        if parent_id:
            payload["project"]["parent_id"] = parent_id
        resp = requests.post(f"{self.base_url}/projects.json", headers=self.headers, json=payload)
        if resp.status_code == 201:
            return resp.json()["project"]
        elif resp.status_code == 422:
            print(f"    [422] {resp.json().get('errors', resp.text[:200])}")
            return None
        else:
            print(f"    [ERROR] Create project {identifier}: {resp.status_code}")
            return None

    def upload_file(self, filepath, filename=None):
        headers = dict(self.headers)
        headers["Content-Type"] = "application/octet-stream"
        if filename:
            headers["Content-Disposition"] = f'attachment; filename="{quote(filename)}"'
        with open(filepath, "rb") as f:
            resp = requests.post(f"{self.base_url}/uploads.json", headers=headers, data=f)
        if resp.status_code in (200, 201):
            return resp.json()["upload"]["token"]
        return None

    def put_wiki_page(self, project_id, wiki_title, text, parent_title=None,
                      uploads=None, comments=None, retries=3):
        """Create or update a wiki page with retry on transient errors.
        Returns the Redmine version number on success, False on failure."""
        url = f"{self.base_url}/projects/{project_id}/wiki/{wiki_title}.json"
        payload = {"wiki_page": {"text": text}}
        if parent_title:
            payload["wiki_page"]["parent_title"] = parent_title
        if uploads:
            payload["wiki_page"]["uploads"] = uploads
        if comments:
            payload["wiki_page"]["comments"] = comments
        for attempt in range(retries):
            try:
                resp = requests.put(url, headers=self.headers, json=payload, timeout=60)
                if resp.status_code in (200, 201, 204):
                    # Try to extract the version number from the response
                    try:
                        rdata = resp.json()
                        return rdata.get("wiki_page", {}).get("version", True)
                    except Exception:
                        return True
                if resp.status_code == 422:
                    err = resp.json().get("errors", resp.text[:200]) if resp.text else "unknown"
                    print(f"\n      [422] {wiki_title}: {err}")
                    return False
                if resp.status_code in (409, 500, 502, 503):
                    if attempt < retries - 1:
                        time.sleep(1.0 * (attempt + 1))
                        continue
                    print(f"\n      [{resp.status_code}] {wiki_title}: failed after {retries} attempts")
                    return False
                print(f"\n      [{resp.status_code}] {wiki_title}: {resp.text[:150]}")
                return False
            except requests.exceptions.RequestException as e:
                if attempt < retries - 1:
                    time.sleep(1.0 * (attempt + 1))
                    continue
                print(f"\n      [TIMEOUT] {wiki_title}: {e}")
                return False
        return False

    def get_wiki_page_info(self, project_id, wiki_title, retries=3):
        """Get current wiki page info including version number."""
        url = f"{self.base_url}/projects/{project_id}/wiki/{wiki_title}.json"
        for attempt in range(retries):
            try:
                resp = requests.get(url, headers=self.headers, timeout=30)
                if resp.status_code == 200:
                    return resp.json().get("wiki_page", {})
                if resp.status_code == 404:
                    return None
                if attempt < retries - 1:
                    time.sleep(1.0 * (attempt + 1))
                    continue
                return None
            except requests.exceptions.RequestException:
                if attempt < retries - 1:
                    time.sleep(2.0 * (attempt + 1))
                    continue
                return None
        return None


# =============================================================================
# NAMING / MAPPING
# =============================================================================

def sanitize_identifier(raw: str) -> str:
    normalized = unicodedata.normalize("NFKD", raw)
    stripped = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    cleaned = stripped.strip().lower()
    result = "".join(ch for ch in cleaned if ch.isalnum() or ch in ("-", "_"))
    return result[:100] if result else None

def sanitize_wiki_title(title: str) -> str:
    """Sanitize title for Redmine wiki URL.
    - Dots in numbers → underscores: 'Page 14.0' → 'Page_014_000'
    - ALL numbers zero-padded to 3 digits for correct alphabetical sort
    - Only Redmine-safe chars: alphanumeric, underscore, hyphen
    Examples:
        '9.00 Intro'   → 'Page_009_000_Intro'
        '14.0 Setup'   → 'Page_014_000_Setup'
        '1.2 Basics'   → 'Page_001_002_Basics'
        '1.10 Advanced' → 'Page_001_010_Advanced'
    """
    normalized = unicodedata.normalize("NFKD", title)
    stripped = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    # Replace dots between digits: "14.0" → "14_0", "1.1" → "1_1"
    result = re.sub(r'(\d)\.(\d)', r'\1_\2', stripped)
    # Strip non-word, non-space, non-hyphen
    result = re.sub(r'[^\w\s-]', '', result)
    # Collapse whitespace to underscore
    result = re.sub(r'[\s]+', '_', result.strip())
    # Zero-pad ALL numbers to 3 digits for correct alphabetical sort
    # "9_00" → "009_000", "14_0" → "014_000", so 009 < 014 sorts correctly
    result = re.sub(r'\d+', lambda m: m.group(0).zfill(3), result)
    if not result:
        return "Untitled"
    if result[0].isdigit():
        result = f"Page_{result}"
    return result[:255]

def sanitize_filename(filename: str) -> str:
    name, ext = os.path.splitext(filename)
    safe = re.sub(r'[^\w\-.]', '_', name)
    safe = re.sub(r'_+', '_', safe).strip('_')
    return f"{safe}{ext}" if safe else f"attachment{ext}"


def extract_body_view_images(html_body):
    """Extract all Confluence image download URLs from body.view HTML.
    Returns dict: {sanitized_filename: relative_download_url}"""
    images = {}
    for m in re.finditer(r'src="(/download/(?:attachments|thumbnails)/\d+/[^"]+)"', html_body):
        raw_url = m.group(1)
        # Extract filename from URL (before query params)
        fn_match = re.search(r'/download/(?:attachments|thumbnails)/\d+/([^?"]+)', raw_url)
        if fn_match:
            filename = unquote(fn_match.group(1))
            safe = sanitize_filename(filename)
            # Keep the URL without query params for clean download
            clean_url = raw_url.split('?')[0]
            if safe not in images:
                images[safe] = clean_url
    return images


def load_excel_mapping(filepath: str) -> dict:
    if not HAS_OPENPYXL:
        print("[WARN] openpyxl not installed, skipping Excel mapping")
        return {"entries": [], "conf_to_redmine": {}, "jira_to_conf": {}}

    wb = openpyxl.load_workbook(filepath, read_only=True, data_only=True)
    ws = wb.active
    rows = list(ws.iter_rows(min_row=1, values_only=True))
    if not rows:
        return {"entries": [], "conf_to_redmine": {}, "jira_to_conf": {}}

    header = [str(c).strip() if c else "" for c in rows[0]]
    print(f"[INFO] Excel headers: {header}")

    entries = []
    conf_to_redmine = {}
    jira_to_conf = {}

    for row in rows[1:]:
        parent_raw = str(row[0]).strip() if row[0] else ""
        subproject = str(row[1]).strip() if row[1] else ""
        jira_keys_raw = str(row[2]).strip() if row[2] else ""
        conf_keys_raw = str(row[5]).strip() if len(row) > 5 and row[5] else ""

        if parent_raw.startswith("\U0001f4c1") or not parent_raw or parent_raw == "None":
            continue
        if not subproject or subproject == "None":
            continue

        jira_keys = [k.strip() for k in jira_keys_raw.replace(",", " ").split() if k.strip() and k.strip() != "\u2014"]
        identifier = sanitize_identifier(jira_keys[0]) if jira_keys else sanitize_identifier(subproject.replace(" ", "-"))

        conf_keys = []
        if conf_keys_raw and conf_keys_raw != "\u2014" and conf_keys_raw != "None":
            for ck in conf_keys_raw.replace(",", " ").split():
                ck = ck.strip()
                if ck and ck != "\u2014":
                    conf_keys.append(ck)

        entry = {"parent": parent_raw, "subproject": subproject, "identifier": identifier, "confluence_keys": conf_keys}
        entries.append(entry)

        for ck in conf_keys:
            conf_to_redmine[ck.upper()] = identifier
            conf_to_redmine[ck] = identifier

        for jk in jira_keys:
            jira_to_conf[jk.upper()] = conf_keys
            jira_to_conf[jk] = conf_keys

    wb.close()

    with_conf = sum(1 for e in entries if e["confluence_keys"])
    without_conf = sum(1 for e in entries if not e["confluence_keys"])
    total_conf_keys = sum(len(e["confluence_keys"]) for e in entries)
    print(f"[INFO] Loaded {len(entries)} entries from Excel:")
    print(f"  {with_conf} have Confluence space(s) ({total_conf_keys} total keys)")
    print(f"  {without_conf} have no Confluence space")
    print(f"  {len(conf_to_redmine)} Confluence->Redmine mappings")
    print(f"  {len(jira_to_conf)} Jira->Confluence reverse mappings")

    return {"entries": entries, "conf_to_redmine": conf_to_redmine, "jira_to_conf": jira_to_conf}


def resolve_space_keys(requested_keys, excel_data, all_spaces):
    """Resolve --spaces values: Confluence space keys OR Jira keys (via Excel)."""
    available = {s["key"].upper(): s["key"] for s in all_spaces}
    jira_to_conf = excel_data.get("jira_to_conf", {})
    resolved = set()
    for key in requested_keys:
        ku = key.upper()
        if ku in available:
            resolved.add(available[ku])
            continue
        conf_keys = jira_to_conf.get(ku, [])
        if conf_keys:
            found = False
            for ck in conf_keys:
                if ck.upper() in available:
                    resolved.add(available[ck.upper()])
                    found = True
                    print(f"  [RESOLVED] Jira '{key}' -> Confluence '{available[ck.upper()]}'")
                else:
                    print(f"  [WARN] Jira '{key}' -> '{ck}' but space not found")
            if not found:
                print(f"  [WARN] Jira '{key}' maps to {conf_keys} but none exist")
        else:
            print(f"  [WARN] '{key}' not found as Confluence space or Jira key")
    return resolved


def ensure_redmine_projects(redmine, excel_data, dry_run=False):
    entries = excel_data.get("entries", [])
    if not entries:
        return redmine.get_projects()
    print(f"\n[INFO] Ensuring {len(entries)} Redmine projects exist...")
    existing = redmine.get_projects()
    existing_by_name = {p["name"]: p for p in existing.values()}
    parents = {}
    for e in entries:
        pname = e["parent"]
        if pname not in parents:
            parents[pname] = sanitize_identifier(pname.replace(" ", "-"))
    parent_id_map = {}
    for pname, pid in parents.items():
        if pid and pid in existing:
            parent_id_map[pname] = existing[pid]["id"]
            print(f"  [EXISTS] Parent '{pname}' ({pid})")
        elif pname in existing_by_name:
            parent_id_map[pname] = existing_by_name[pname]["id"]
            print(f"  [EXISTS] Parent '{pname}'")
        elif dry_run:
            print(f"  [DRY-RUN] Would create parent: '{pname}' ({pid})")
            parent_id_map[pname] = -1
        else:
            result = redmine.create_project(pname, pid)
            if result:
                parent_id_map[pname] = result["id"]
                existing[pid] = result
                print(f"  [CREATED] Parent '{pname}' ({pid})")
                time.sleep(0.2)
            else:
                print(f"  [ERROR] Failed to create parent '{pname}'")
                parent_id_map[pname] = None
    created = 0
    skipped = 0
    for e in entries:
        name = e["subproject"]
        identifier = sanitize_identifier(e["identifier"]) if e.get("identifier") else sanitize_identifier(name)
        parent_rm_id = parent_id_map.get(e["parent"])
        if not identifier:
            continue
        if identifier in existing:
            skipped += 1
            continue
        if parent_rm_id is None:
            print(f"  [SKIP] '{name}' -- parent '{e['parent']}' failed")
            continue
        if dry_run:
            print(f"  [DRY-RUN] Would create: '{name}' ({identifier}) under '{e['parent']}'")
            created += 1
            continue
        result = redmine.create_project(name, identifier, parent_id=parent_rm_id if parent_rm_id != -1 else None)
        if result:
            existing[identifier] = result
            created += 1
            print(f"  [CREATED] '{name}' ({identifier})")
            time.sleep(0.2)
        else:
            print(f"  [ERROR] '{name}' ({identifier})")
    print(f"  Projects: {created} created, {skipped} already existed")
    return existing


def build_space_to_project_map(confluence_spaces, redmine_projects, excel_mapping=None):
    mapping = {}
    for space in confluence_spaces:
        key = space["key"]
        name = space["name"]
        key_lower = key.lower()
        if excel_mapping:
            rm_id = excel_mapping.get(key) or excel_mapping.get(key.upper())
            if rm_id:
                mapping[key] = {"identifier": rm_id, "name": name, "exists": rm_id in redmine_projects, "source": "excel"}
                continue
        if key_lower in redmine_projects:
            mapping[key] = {"identifier": key_lower, "name": name, "exists": True, "source": "exact_match"}
            continue
        sanitized = sanitize_identifier(key)
        if sanitized and sanitized in redmine_projects:
            mapping[key] = {"identifier": sanitized, "name": name, "exists": True, "source": "sanitized_match"}
            continue
        identifier = sanitize_identifier(key)
        if not identifier:
            identifier = sanitize_identifier(name)
        mapping[key] = {"identifier": identifier, "name": name, "exists": False, "source": "new"}
    return mapping


# =============================================================================
# HTML -> MARKDOWN CONVERTER
# =============================================================================

def convert_html_to_markdown(html_body: str, fmt="markdown", page_id_map=None,
                             current_project=None) -> str:
    """
    Convert Confluence storage XHTML to Redmine-compatible wiki markup.

    Comprehensive converter supporting:
    - Images (Markdown ![](file) or Textile !file!)
    - Table of Contents ({{toc}})
    - Anchor links and in-page navigation
    - All Confluence macros (code, expand, panels, status, etc.)
    - Attachments, wiki links, cross-space links
    - User mentions, emoticons, task lists
    - Diagrams (draw.io, Gliffy, Lucidchart)
    - Layout/column macros
    - Color/styling preservation

    Args:
        html_body: Confluence storage format XHTML
        fmt: "markdown" or "textile" (match your Redmine setting)
        page_id_map: dict mapping page ID (str) to {"title": wiki_title, "project": project_id}
        current_project: current Redmine project identifier (for same-project link detection)
    """
    if not html_body:
        return ""
    if page_id_map is None:
        page_id_map = {}

    text = html_body

    def _safe_fn(filename):
        name, ext = os.path.splitext(filename)
        safe = re.sub(r'[^\w\-.]', '_', name)
        safe = re.sub(r'_+', '_', safe).strip('_')
        return f"{safe}{ext}" if safe else f"attachment{ext}"

    def _make_anchor(title):
        """Generate Redmine-compatible anchor from heading text."""
        anchor = re.sub(r'<[^>]+>', '', title)  # strip HTML
        anchor = re.sub(r'[^\w\s-]', '', anchor)
        anchor = anchor.strip().replace(' ', '-')
        return anchor

    def _img(filename):
        """Generate image markup as standard markdown inline image."""
        safe = _safe_fn(filename)
        return f"\n![{safe}]({safe})\n"

    # =========================================================================
    # PHASE 0: Pre-process — normalize whitespace in tags, fix self-closing
    # =========================================================================
    # Fix self-closing structured macros
    text = re.sub(
        r'<ac:structured-macro([^>]*)/\s*>',
        r'<ac:structured-macro\1></ac:structured-macro>',
        text,
    )

    # =========================================================================
    # PHASE 1: Confluence-specific macros
    # =========================================================================

    # --- TOC macro → {{toc}} ---
    def replace_toc(match):
        full = match.group(0)
        # Check for parameters like maxLevel, minLevel
        max_match = re.search(r'<ac:parameter ac:name="maxLevel">(\d+)</ac:parameter>', full)
        min_match = re.search(r'<ac:parameter ac:name="minLevel">(\d+)</ac:parameter>', full)
        # Redmine {{toc}} doesn't support min/max but we can note it
        return "\n\n{{toc}}\n\n"
    text = re.sub(
        r'<ac:structured-macro[^>]*ac:name="toc"[^>]*>.*?</ac:structured-macro>',
        replace_toc, text, flags=re.DOTALL,
    )
    text = re.sub(
        r'<ac:structured-macro[^>]*ac:name="toc"[^>]*/\s*>',
        '\n\n{{toc}}\n\n', text,
    )

    # --- Anchor macro → HTML anchor ---
    def replace_anchor(match):
        full = match.group(0)
        name_match = re.search(r'<ac:parameter ac:name="">([^<]*)</ac:parameter>', full)
        if not name_match:
            name_match = re.search(r'<ac:parameter[^>]*>([^<]*)</ac:parameter>', full)
        if name_match:
            anchor_name = name_match.group(1).strip()
            # Redmine passes through <a> tags in some configs
            return f'<a name="{anchor_name}" id="{anchor_name}"></a>'
        return ""
    text = re.sub(
        r'<ac:structured-macro[^>]*ac:name="anchor"[^>]*>.*?</ac:structured-macro>',
        replace_anchor, text, flags=re.DOTALL,
    )

    # --- Excerpt/include macros → {{include}} ---
    def replace_include(match):
        full = match.group(0)
        title_match = re.search(r'ri:content-title="([^"]*)"', full)
        space_match = re.search(r'ri:space-key="([^"]*)"', full)
        if title_match:
            t = title_match.group(1)
            wiki_t = sanitize_wiki_title(t)
            if space_match:
                # Cross-project include
                sk = space_match.group(1).lower()
                return f"\n{{{{include({sk}:{wiki_t})}}}}\n"
            return f"\n{{{{include({wiki_t})}}}}\n"
        return ""
    text = re.sub(
        r'<ac:structured-macro[^>]*ac:name="(?:excerpt-include|include)"[^>]*>.*?</ac:structured-macro>',
        replace_include, text, flags=re.DOTALL,
    )

    # --- Excerpt definition (keep content, strip macro wrapper) ---
    def replace_excerpt_def(match):
        full = match.group(0)
        body_match = re.search(r'<ac:rich-text-body>(.*?)</ac:rich-text-body>', full, re.DOTALL)
        return body_match.group(1) if body_match else ""
    text = re.sub(
        r'<ac:structured-macro[^>]*ac:name="excerpt"[^>]*>.*?</ac:structured-macro>',
        replace_excerpt_def, text, flags=re.DOTALL,
    )

    # --- Code/noformat macros → fenced code blocks ---
    def replace_code_macro(match):
        full = match.group(0)
        lang_match = re.search(r'<ac:parameter ac:name="language">([^<]*)</ac:parameter>', full)
        title_match = re.search(r'<ac:parameter ac:name="title">([^<]*)</ac:parameter>', full)
        lang = lang_match.group(1).lower() if lang_match else ""
        # Normalize language aliases
        lang_map = {"c#": "csharp", "c++": "cpp", "js": "javascript",
                     "py": "python", "rb": "ruby", "sh": "bash", "shell": "bash",
                     "yml": "yaml", "objective-c": "objc", "actionscript3": "actionscript"}
        lang = lang_map.get(lang, lang)
        body_match = re.search(r'<ac:plain-text-body>\s*<!\[CDATA\[(.*?)\]\]>\s*</ac:plain-text-body>', full, re.DOTALL)
        if not body_match:
            body_match = re.search(r'<ac:plain-text-body>(.*?)</ac:plain-text-body>', full, re.DOTALL)
        body = body_match.group(1) if body_match else ""
        title_line = ""
        if title_match:
            title_line = f"**{title_match.group(1)}**\n"
        if fmt == "textile":
            if lang:
                return f"\n{title_line}<pre><code class=\"{lang}\">\n{body}\n</code></pre>\n"
            return f"\n{title_line}<pre>\n{body}\n</pre>\n"
        return f"\n{title_line}```{lang}\n{body}\n```\n"
    text = re.sub(
        r'<ac:structured-macro[^>]*ac:name="(?:code|noformat)"[^>]*>.*?</ac:structured-macro>',
        replace_code_macro, text, flags=re.DOTALL,
    )

    # --- View-file macro ---
    def replace_view_file(match):
        fn_match = re.search(r'ri:filename="([^"]*)"', match.group(0))
        if fn_match:
            f = fn_match.group(1)
            sf = _safe_fn(f)
            if fmt == "textile":
                return f'\nattachment:"{sf}"\n'
            return f"\n[{f}]({sf})\n"
        return ""
    text = re.sub(
        r'<ac:structured-macro[^>]*ac:name="view-file"[^>]*>.*?</ac:structured-macro>',
        replace_view_file, text, flags=re.DOTALL,
    )

    # --- Multimedia macro (YouTube/Vimeo/attachment) ---
    def replace_multimedia(match):
        full = match.group(0)
        # Extract width/height if present
        w_match = re.search(r'<ac:parameter ac:name="width">(\d+)</ac:parameter>', full)
        h_match = re.search(r'<ac:parameter ac:name="height">(\d+)</ac:parameter>', full)
        width = int(w_match.group(1)) if w_match else 640
        height = int(h_match.group(1)) if h_match else 360
        url_match = re.search(r'<ac:parameter ac:name="(?:url|URL)">([^<]*)</ac:parameter>', full)
        if url_match:
            url = url_match.group(1)
            yt_match = re.search(r'(?:youtube\.com/watch\?v=|youtu\.be/)([\w-]+)', url)
            if yt_match:
                return f"\n{{{{youtube({yt_match.group(1)}, width={width}, height={height})}}}}\n"
            vim_match = re.search(r'vimeo\.com/(\d+)', url)
            if vim_match:
                return f"\n{{{{vimeo({vim_match.group(1)}, width={width}, height={height})}}}}\n"
            # Google Docs link
            if 'docs.google.com' in url:
                return f"\n{{{{google_docs({url}, width=100%, height={height})}}}}\n"
            return f"\n{{{{iframe({url}, width={width}, height={height})}}}}\n"
        fn_match = re.search(r'ri:filename="([^"]*)"', full)
        if fn_match:
            f = fn_match.group(1)
            sf = _safe_fn(f)
            if fmt == "textile":
                return f'\nattachment:"{sf}"\n'
            return f"\n![{f}]({sf})\n"
        return ""
    text = re.sub(
        r'<ac:structured-macro[^>]*ac:name="multimedia"[^>]*>.*?</ac:structured-macro>',
        replace_multimedia, text, flags=re.DOTALL,
    )

    # --- Widget connector (embeds: YouTube, Google Maps, iframe) ---
    def replace_widget(match):
        full = match.group(0)
        w_match = re.search(r'<ac:parameter ac:name="width">(\d+)</ac:parameter>', full)
        h_match = re.search(r'<ac:parameter ac:name="height">(\d+)</ac:parameter>', full)
        width = int(w_match.group(1)) if w_match else 640
        height = int(h_match.group(1)) if h_match else 360
        url_match = re.search(r'<ac:parameter ac:name="url">([^<]*)</ac:parameter>', full)
        if url_match:
            url = url_match.group(1)
            yt_match = re.search(r'(?:youtube\.com/watch\?v=|youtu\.be/)([\w-]+)', url)
            if yt_match:
                return f"\n{{{{youtube({yt_match.group(1)}, width={width}, height={height})}}}}\n"
            vim_match = re.search(r'vimeo\.com/(\d+)', url)
            if vim_match:
                return f"\n{{{{vimeo({vim_match.group(1)}, width={width}, height={height})}}}}\n"
            if 'docs.google.com' in url:
                return f"\n{{{{google_docs({url}, width=100%, height={height})}}}}\n"
            # Generic embed → iframe macro
            return f"\n{{{{iframe({url}, width={width}, height={height})}}}}\n"
        return ""
    text = re.sub(
        r'<ac:structured-macro[^>]*ac:name="widget"[^>]*>.*?</ac:structured-macro>',
        replace_widget, text, flags=re.DOTALL,
    )

    # --- Swagger/OpenAPI macro → iframe embed ---
    def replace_swagger(match):
        full = match.group(0)
        url_match = re.search(r'<ac:parameter ac:name="(?:url|specUrl)">([^<]*)</ac:parameter>', full)
        if url_match:
            url = url_match.group(1)
            return f"\n**API Documentation (Swagger/OpenAPI)**\n{{{{iframe({url}, width=100%, height=600)}}}}\n"
        return "\n> **API Documentation (Swagger/OpenAPI)** -- *embedded content not migrated*\n"
    text = re.sub(
        r'<ac:structured-macro[^>]*ac:name="(?:swagger|open-api|openapi)"[^>]*>.*?</ac:structured-macro>',
        replace_swagger, text, flags=re.DOTALL,
    )

    # --- Expand macro (collapsible) → {{collapse}} ---
    def replace_expand(match):
        full = match.group(0)
        title_match = re.search(r'<ac:parameter ac:name="title">([^<]*)</ac:parameter>', full)
        title = title_match.group(1) if title_match else "Details"
        body_match = re.search(r'<ac:rich-text-body>(.*?)</ac:rich-text-body>', full, re.DOTALL)
        body = body_match.group(1) if body_match else ""
        return f"\n{{{{collapse({title})\n{body}\n}}}}\n"
    text = re.sub(
        r'<ac:structured-macro[^>]*ac:name="expand"[^>]*>.*?</ac:structured-macro>',
        replace_expand, text, flags=re.DOTALL,
    )

    # --- Section/column layout macros → div or just content ---
    def replace_section(match):
        full = match.group(0)
        # Extract all column bodies
        columns = re.findall(r'<ac:rich-text-body>(.*?)</ac:rich-text-body>', full, re.DOTALL)
        if columns:
            parts = []
            for col in columns:
                parts.append(col.strip())
            return "\n\n" + "\n\n---\n\n".join(parts) + "\n\n"
        return ""
    text = re.sub(
        r'<ac:structured-macro[^>]*ac:name="section"[^>]*>.*?</ac:structured-macro>',
        replace_section, text, flags=re.DOTALL,
    )
    # Standalone column macros (outside section)
    def replace_column(match):
        full = match.group(0)
        body_match = re.search(r'<ac:rich-text-body>(.*?)</ac:rich-text-body>', full, re.DOTALL)
        return body_match.group(1) if body_match else ""
    text = re.sub(
        r'<ac:structured-macro[^>]*ac:name="column"[^>]*>.*?</ac:structured-macro>',
        replace_column, text, flags=re.DOTALL,
    )

    # --- Status macro → colored label ---
    def replace_status(match):
        full = match.group(0)
        color_match = re.search(r'<ac:parameter ac:name="colour">([^<]*)</ac:parameter>', full)
        title_match = re.search(r'<ac:parameter ac:name="title">([^<]*)</ac:parameter>', full)
        title = title_match.group(1) if title_match else "Status"
        color = color_match.group(1) if color_match else ""
        color_map = {"Green": "#00875A", "Yellow": "#FF991F", "Red": "#DE350B",
                     "Blue": "#0052CC", "Grey": "#97A0AF", "Purple": "#6554C0"}
        hex_color = color_map.get(color, "")
        if hex_color and fmt == "textile":
            return f' %{{background:{hex_color};color:#fff;padding:1px 6px;border-radius:3px}}{title}% '
        elif hex_color:
            return f' <span style="background:{hex_color};color:#fff;padding:1px 6px;border-radius:3px;font-size:0.85em">{title}</span> '
        return f" **{title}** "
    text = re.sub(
        r'<ac:structured-macro[^>]*ac:name="status"[^>]*>.*?</ac:structured-macro>',
        replace_status, text, flags=re.DOTALL,
    )

    # --- Info/note/warning/tip/panel → blockquotes ---
    def replace_panel(match):
        macro_name = match.group(1)
        body = match.group(0)
        # Extract title if present
        title_match = re.search(r'<ac:parameter ac:name="title">([^<]*)</ac:parameter>', body)
        title = title_match.group(1) if title_match else ""
        body_match = re.search(r'<ac:rich-text-body>(.*?)</ac:rich-text-body>', body, re.DOTALL)
        content = body_match.group(1) if body_match else ""
        content = re.sub(r'<[^>]+>', ' ', content).strip()
        content = re.sub(r'\s+', ' ', content)
        icons = {"info": "INFO", "note": "NOTE", "warning": "WARNING", "tip": "TIP", "panel": "PANEL"}
        label = icons.get(macro_name, macro_name.upper())
        header = f"**{label}**"
        if title:
            header = f"**{label}: {title}**"
        lines = content.split('\n')
        quoted = '\n'.join(f"> {line.strip()}" for line in lines if line.strip())
        return f"\n> {header}\n{quoted}\n"
    text = re.sub(
        r'<ac:structured-macro[^>]*ac:name="(info|note|warning|tip|panel)"[^>]*>.*?</ac:structured-macro>',
        replace_panel, text, flags=re.DOTALL,
    )

    # --- Children display macro → {{child_pages}} ---
    text = re.sub(
        r'<ac:structured-macro[^>]*ac:name="children"[^>]*>.*?</ac:structured-macro>',
        '\n\n{{child_pages}}\n\n', text, flags=re.DOTALL,
    )

    # --- Recently updated / contributors / profile macros (remove) ---
    text = re.sub(
        r'<ac:structured-macro[^>]*ac:name="(?:recently-updated|livesearch|content-report-table|popular-labels)"[^>]*>.*?</ac:structured-macro>',
        '', text, flags=re.DOTALL,
    )
    text = re.sub(
        r'<ac:structured-macro[^>]*ac:name="(?:contributors|profile|contributors-summary|user-profile|user-list)"[^>]*>.*?</ac:structured-macro>',
        '', text, flags=re.DOTALL,
    )

    # --- Jira issue macro ---
    def replace_jira(match):
        full = match.group(0)
        key_match = re.search(r'<ac:parameter ac:name="key">([^<]*)</ac:parameter>', full)
        if key_match:
            key = key_match.group(1)
            return f"`{key}`"
        jql_match = re.search(r'<ac:parameter ac:name="jqlQuery">([^<]*)</ac:parameter>', full)
        if jql_match:
            return f"\n*Jira query: `{jql_match.group(1)}`*\n"
        server_match = re.search(r'<ac:parameter ac:name="server">([^<]*)</ac:parameter>', full)
        columns_match = re.search(r'<ac:parameter ac:name="columns">([^<]*)</ac:parameter>', full)
        if columns_match or server_match:
            return "\n*Jira issue list -- not migrated*\n"
        return ""
    text = re.sub(
        r'<ac:structured-macro[^>]*ac:name="jira"[^>]*>.*?</ac:structured-macro>',
        replace_jira, text, flags=re.DOTALL,
    )

    # --- Draw.io / Gliffy / Lucidchart diagrams ---
    def replace_diagram(match):
        full = match.group(0)
        macro_name = re.search(r'ac:name="([^"]*)"', full)
        name = macro_name.group(1) if macro_name else "diagram"
        diag_name_match = re.search(r'<ac:parameter ac:name="(?:diagramName|name|filename)">([^<]*)</ac:parameter>', full)
        if diag_name_match:
            dname = diag_name_match.group(1)
            return f"\n> **Diagram ({name}):** {dname}\n> *Embedded diagram not migrated — see original Confluence page*\n"
        return f"\n> **Diagram ({name})** — *embedded diagram not migrated*\n"
    text = re.sub(
        r'<ac:structured-macro[^>]*ac:name="(?:drawio|gliffy|lucidchart|cloudfortress-draw\.io|balsamiq)"[^>]*>.*?</ac:structured-macro>',
        replace_diagram, text, flags=re.DOTALL,
    )

    # --- Roadmap / calendar macros ---
    text = re.sub(
        r'<ac:structured-macro[^>]*ac:name="(?:roadmap|calendar|team-calendar)"[^>]*>.*?</ac:structured-macro>',
        '\n> *Calendar/Roadmap macro — not migrated*\n', text, flags=re.DOTALL,
    )

    # --- Details macro (Confluence Cloud) ---
    def replace_details(match):
        body_match = re.search(r'<ac:rich-text-body>(.*?)</ac:rich-text-body>', match.group(0), re.DOTALL)
        return body_match.group(1) if body_match else ""
    text = re.sub(
        r'<ac:structured-macro[^>]*ac:name="(?:details|page-info|details-summary)"[^>]*>.*?</ac:structured-macro>',
        replace_details, text, flags=re.DOTALL,
    )

    # --- Remaining structured macros → HTML comment ---
    text = re.sub(
        r'<ac:structured-macro[^>]*ac:name="([^"]*)"[^>]*>.*?</ac:structured-macro>',
        r'<!-- Confluence macro: \1 -->', text, flags=re.DOTALL,
    )

    # =========================================================================
    # PHASE 2: Images and attachment links
    # =========================================================================

    # --- Confluence images ---
    def replace_image(match):
        full = match.group(0)
        fn_match = re.search(r'ri:filename="([^"]*)"', full)
        url_match = re.search(r'ri:url ri:value="([^"]*)"', full)
        if fn_match:
            filename = fn_match.group(1)
            alt_match = re.search(r'ac:alt="([^"]*)"', full)
            alt = alt_match.group(1) if alt_match else ""
            w_match = re.search(r'ac:width="(\d+)"', full)
            width = int(w_match.group(1)) if w_match else None
            return _img(filename)
        elif url_match:
            url = url_match.group(1)
            if fmt == "textile":
                return f"\n!{url}!\n"
            return f"\n![]({url})\n"
        return ""
    text = re.sub(r'<ac:image[^>]*>.*?</ac:image>', replace_image, text, flags=re.DOTALL)

    # --- body.view Confluence attachment images ---
    # body.view renders images as: <p><span class="confluence-embedded-file-wrapper ..."><img ...></span></p>
    # Must handle the WHOLE wrapper in Phase 2 so the resulting markdown isn't
    # trapped inside an HTML block (CommonMark treats markdown inside <p>/<span> as literal text).
    def replace_view_img_block(match):
        full = match.group(0)
        alias_m = re.search(r'data-linked-resource-default-alias="([^"]*)"', full)
        url_m = re.search(r'src="[^"]*?/download/(?:attachments|thumbnails)/\d+/([^?"]+)', full)
        width_m = re.search(r'width="(\d+)"', full)
        height_m = re.search(r'height="(\d+)"', full)
        alt_m = re.search(r'alt="([^"]*)"', full)
        w = width_m.group(1) if width_m else (height_m.group(1) if height_m else None)
        alt = alt_m.group(1) if alt_m else ""
        if alias_m:
            return "\n\n" + _img(alias_m.group(1)).strip() + "\n\n"
        elif url_m:
            return "\n\n" + _img(unquote(url_m.group(1))).strip() + "\n\n"
        return ""

    # Match full <p><span class="confluence-embedded-file-wrapper..."><img ...></span></p> blocks
    text = re.sub(
        r'<p>\s*<span[^>]*class="[^"]*confluence-embedded-file-wrapper[^"]*"[^>]*>\s*<img[^>]*/?\s*>\s*</span>\s*</p>',
        replace_view_img_block, text, flags=re.DOTALL,
    )
    # Also handle bare <span class="confluence-embedded-file-wrapper"><img></span> without <p> wrapper
    text = re.sub(
        r'<span[^>]*class="[^"]*confluence-embedded-file-wrapper[^"]*"[^>]*>\s*<img[^>]*/?\s*>\s*</span>',
        replace_view_img_block, text, flags=re.DOTALL,
    )

    # --- Attachment links ---
    def replace_att_link(match):
        full = match.group(0)
        fn_match = re.search(r'ri:filename="([^"]*)"', full)
        if not fn_match:
            return ""
        filename = fn_match.group(1)
        safe = _safe_fn(filename)
        body_match = re.search(r'<ac:plain-text-link-body>\s*<!\[CDATA\[(.*?)\]\]>\s*</ac:plain-text-link-body>', full, re.DOTALL)
        if not body_match:
            body_match = re.search(r'<ac:link-body>(.*?)</ac:link-body>', full, re.DOTALL)
        display = ""
        if body_match:
            display = re.sub(r'<[^>]+>', '', body_match.group(1)).strip()
        if not display:
            display = filename
        if fmt == "textile":
            return f' attachment:"{safe}" '
        return f" [{display}]({safe}) "
    text = re.sub(r'<ac:link[^>]*>.*?<ri:attachment[^/]*/?>.*?</ac:link>', replace_att_link, text, flags=re.DOTALL)

    # --- Wiki page links (with anchor support) ---
    def replace_wiki_link(match):
        full = match.group(0)
        title_match = re.search(r'ri:content-title="([^"]*)"', full)
        if not title_match:
            return ""
        title = title_match.group(1)
        wiki_title = sanitize_wiki_title(title)
        # Check for anchor
        anchor_match = re.search(r'ac:anchor="([^"]*)"', full)
        anchor = ""
        if anchor_match:
            anchor = f"#{anchor_match.group(1)}"
        # Check for cross-space link
        space_match = re.search(r'ri:space-key="([^"]*)"', full)
        space_prefix = ""
        if space_match:
            space_prefix = f"{space_match.group(1).lower()}:"
        body_match = re.search(r'<ac:plain-text-link-body>\s*<!\[CDATA\[(.*?)\]\]>\s*</ac:plain-text-link-body>', full, re.DOTALL)
        if not body_match:
            body_match = re.search(r'<ac:link-body>(.*?)</ac:link-body>', full, re.DOTALL)
        display = ""
        if body_match:
            display = re.sub(r'<[^>]+>', '', body_match.group(1)).strip()
        if not display:
            display = title
        # Cross-project links use {{wiki()}} macro from Wiki Extensions
        if space_match:
            project_id = space_match.group(1).lower()
            page_ref = f"{wiki_title}{anchor}"
            return f"{{{{wiki({project_id}, {page_ref}, {display})}}}}"
        return f"[[{wiki_title}{anchor}|{display}]]"
    text = re.sub(r'<ac:link[^>]*>.*?<ri:page[^/]*/?>.*?</ac:link>', replace_wiki_link, text, flags=re.DOTALL)

    # --- Anchor-only links (jump to section) ---
    def replace_anchor_link(match):
        full = match.group(0)
        anchor_match = re.search(r'ac:anchor="([^"]*)"', full)
        if anchor_match:
            anchor = anchor_match.group(1)
            body_match = re.search(r'<ac:plain-text-link-body>\s*<!\[CDATA\[(.*?)\]\]>', full, re.DOTALL)
            if not body_match:
                body_match = re.search(r'<ac:link-body>(.*?)</ac:link-body>', full, re.DOTALL)
            display = ""
            if body_match:
                display = re.sub(r'<[^>]+>', '', body_match.group(1)).strip()
            if not display:
                display = anchor
            # Redmine link to anchor on same page
            return f"[{display}](#{anchor})"
        return ""
    # Links with anchor but no page reference (in-page jumps)
    text = re.sub(r'<ac:link\s+ac:anchor="[^"]*"[^>]*>(?:(?!<ri:page).)*?</ac:link>', replace_anchor_link, text, flags=re.DOTALL)

    # --- Remaining ac:link (URL links, space links, etc) ---
    def replace_remaining_link(match):
        full = match.group(0)
        url_match = re.search(r'<ri:url ri:value="([^"]*)"', full)
        if url_match:
            url = url_match.group(1)
            body_match = re.search(r'<ac:plain-text-link-body>\s*<!\[CDATA\[(.*?)\]\]>', full, re.DOTALL)
            if not body_match:
                body_match = re.search(r'<ac:link-body>(.*?)</ac:link-body>', full, re.DOTALL)
            display = ""
            if body_match:
                display = re.sub(r'<[^>]+>', '', body_match.group(1)).strip()
            if not display:
                display = url
            if fmt == "textile":
                return f'"{display}":{url}'
            return f"[{display}]({url})"
        # Space home link
        space_match = re.search(r'ri:space-key="([^"]*)"', full)
        if space_match:
            sk = space_match.group(1)
            return f"[[{sk.lower()}:]]"
        body_match = re.search(r'<ac:link-body>(.*?)</ac:link-body>', full, re.DOTALL)
        if body_match:
            return re.sub(r'<[^>]+>', '', body_match.group(1)).strip()
        return ""
    text = re.sub(r'<ac:link[^>]*>.*?</ac:link>', replace_remaining_link, text, flags=re.DOTALL)

    # --- User mentions ---
    def replace_user_mention(match):
        full = match.group(0)
        key_match = re.search(r'ri:userkey="([^"]*)"', full)
        username_match = re.search(r'ri:username="([^"]*)"', full)
        # Try to get display name from link body
        body_match = re.search(r'<ac:link-body[^>]*>(.*?)</ac:link-body>', full, re.DOTALL)
        if body_match:
            name = re.sub(r'<[^>]+>', '', body_match.group(1)).strip()
            return f"@{name}"
        if username_match:
            return f"@{username_match.group(1)}"
        return "@user"
    text = re.sub(r'<ac:link[^>]*>.*?<ri:user[^/]*/?>.*?</ac:link>', replace_user_mention, text, flags=re.DOTALL)

    # --- Emoticons → FontAwesome icons via {{fa()}} macro (Additionals plugin) ---
    # Maps Confluence emoticon names to FontAwesome 5 icon names + colors
    emoticon_fa_map = {
        "tick": ("check-circle", "green"),
        "cross": ("times-circle", "red"),
        "warning": ("exclamation-triangle", "#FF991F"),
        "information": ("info-circle", "#0052CC"),
        "plus": ("plus-circle", "green"),
        "minus": ("minus-circle", "red"),
        "question": ("question-circle", "#6554C0"),
        "light-on": ("lightbulb", "#FF991F"),
        "light-off": ("lightbulb", "#97A0AF"),
        "yellow-star": ("star", "#FFAB00"),
        "red-star": ("star", "red"),
        "green-star": ("star", "green"),
        "blue-star": ("star", "#0052CC"),
        "thumbs-up": ("thumbs-up", "green"),
        "thumbs-down": ("thumbs-down", "red"),
        "smile": ("smile", "#FFAB00"),
        "sad": ("frown", "#97A0AF"),
        "laugh": ("laugh", "#FFAB00"),
        "wink": ("grin-wink", "#FFAB00"),
        "cheeky": ("grin-tongue-wink", "#FFAB00"),
        "heart": ("heart", "red"),
        "broken_heart": ("heart-broken", "red"),
        "flag": ("flag", "red"),
    }
    def replace_emoticon(match):
        name = match.group(1)
        if name in emoticon_fa_map:
            icon, color = emoticon_fa_map[name]
            return f"{{{{fa({icon}, color={color})}}}}"
        return f"[{name}]"
    text = re.sub(r'<ac:emoticon ac:name="([^"]*)"\s*/?>', replace_emoticon, text)

    # --- Strip remaining ac:*/ri:* tags ---
    text = re.sub(r'</?ac:[^>]*/?>', '', text)
    text = re.sub(r'</?ri:[^>]*/?>', '', text)

    # =========================================================================
    # PHASE 3: Standard HTML → Wiki markup
    # =========================================================================

    # --- Convert legacy <font color> to <span style="color:"> EARLY (before list processing) ---
    def _convert_font_color(m):
        color = m.group(1)
        content = m.group(2)
        return f'<span style="color:{color}">{content}</span>'
    text = re.sub(r'<font[^>]*color="([^"]*)"[^>]*>(.*?)</font>', _convert_font_color, text, flags=re.DOTALL)
    text = re.sub(r'</?font[^>]*>', '', text)  # Strip remaining font tags

    # --- Tables — keep ALL as HTML for maximum fidelity ---
    # CommonMark (Redmine 5+) passes through HTML tables with styles.
    # This preserves colors, colspan, rowspan, backgrounds, etc.
    def convert_table(match):
        table_html = match.group(0)
        # Clean Confluence-specific classes/data attributes but keep everything else
        table_html = re.sub(r'\s+class="[^"]*confluenc[^"]*"', '', table_html)
        table_html = re.sub(r'\s+data-[a-z-]+="[^"]*"', '', table_html)
        table_html = re.sub(r'\s+class="wrapped"', '', table_html)
        # Convert inline Confluence formatting inside cells
        table_html = re.sub(r'<strong[^>]*>(.*?)</strong>', r'<b>\1</b>', table_html, flags=re.DOTALL)
        table_html = re.sub(r'<em[^>]*>(.*?)</em>', r'<i>\1</i>', table_html, flags=re.DOTALL)
        return f"\n{table_html}\n"
    text = re.sub(r'<table[^>]*>.*?</table>', convert_table, text, flags=re.DOTALL)

    # --- Headers (with auto-anchor generation) ---
    for i in range(1, 7):
        if fmt == "textile":
            text = re.sub(
                rf'<h{i}[^>]*>(.*?)</h{i}>',
                rf'\nh{i}. \1\n',
                text, flags=re.DOTALL,
            )
        else:
            text = re.sub(
                rf'<h{i}[^>]*>(.*?)</h{i}>',
                rf'\n{"#" * i} \1\n',
                text, flags=re.DOTALL,
            )

    # --- Bold, italic, strikethrough, underline ---
    def _strip_inline_html(s):
        """Strip leftover inline HTML tags (span, font, etc.) from inside bold/italic content."""
        return re.sub(r'</?(?:span|font|sup|sub)[^>]*>', '', s)

    def fix_bold(m):
        content = _strip_inline_html(m.group(1))
        leading, trailing, stripped = '', '', content
        if stripped.startswith((' ', '\n')): leading, stripped = ' ', stripped.lstrip()
        if stripped.endswith((' ', '\n')): trailing, stripped = ' ', stripped.rstrip()
        if not stripped:
            return content
        # Multiline bold doesn't work in markdown — join into single line
        if '\n' in stripped:
            stripped = ' '.join(line.strip() for line in stripped.split('\n') if line.strip())
        if fmt == "textile":
            return f"{leading}*{stripped}*{trailing}"
        return f"{leading}**{stripped}**{trailing}"
    def fix_italic(m):
        content = _strip_inline_html(m.group(1))
        leading, trailing, stripped = '', '', content
        if stripped.startswith((' ', '\n')): leading, stripped = ' ', stripped.lstrip()
        if stripped.endswith((' ', '\n')): trailing, stripped = ' ', stripped.rstrip()
        if not stripped:
            return content
        if '\n' in stripped:
            stripped = ' '.join(line.strip() for line in stripped.split('\n') if line.strip())
        if fmt == "textile":
            return f"{leading}_{stripped}_{trailing}"
        return f"{leading}*{stripped}*{trailing}"

    text = re.sub(r'<strong[^>]*>(.*?)</strong>', fix_bold, text, flags=re.DOTALL)
    text = re.sub(r'<b[^>]*>(.*?)</b>', fix_bold, text, flags=re.DOTALL)
    text = re.sub(r'<em[^>]*>(.*?)</em>', fix_italic, text, flags=re.DOTALL)
    text = re.sub(r'<i[^>]*>(.*?)</i>', fix_italic, text, flags=re.DOTALL)
    if fmt == "textile":
        text = re.sub(r'<del[^>]*>(.*?)</del>', r'-\1-', text, flags=re.DOTALL)
        text = re.sub(r'<s[^>]*>(.*?)</s>', r'-\1-', text, flags=re.DOTALL)
        text = re.sub(r'<u[^>]*>(.*?)</u>', r'+\1+', text, flags=re.DOTALL)
    else:
        text = re.sub(r'<del[^>]*>(.*?)</del>', r'~~\1~~', text, flags=re.DOTALL)
        text = re.sub(r'<s[^>]*>(.*?)</s>', r'~~\1~~', text, flags=re.DOTALL)
        # <u> kept as HTML for Markdown (no native underline)

    # --- Code blocks ---
    if fmt == "textile":
        text = re.sub(r'<pre[^>]*><code[^>]*class="([^"]*)"[^>]*>(.*?)</code></pre>',
                       r'\n<pre><code class="\1">\n\2\n</code></pre>\n', text, flags=re.DOTALL)
        text = re.sub(r'<pre[^>]*><code[^>]*>(.*?)</code></pre>', r'\n<pre>\n\1\n</pre>\n', text, flags=re.DOTALL)
        text = re.sub(r'<pre[^>]*>(.*?)</pre>', r'\n<pre>\n\1\n</pre>\n', text, flags=re.DOTALL)
    else:
        text = re.sub(r'<pre[^>]*><code[^>]*>(.*?)</code></pre>', r'\n```\n\1\n```\n', text, flags=re.DOTALL)
        text = re.sub(r'<pre[^>]*>(.*?)</pre>', r'\n```\n\1\n```\n', text, flags=re.DOTALL)
    text = re.sub(r'<code[^>]*>(.*?)</code>', r'`\1`', text, flags=re.DOTALL)

    # --- User mentions (body.view format) ---
    # body.view renders mentions as: <a class="confluence-userlink user-mention" ...>Display Name</a>
    # Must run BEFORE convert_link, which would treat these as regular links.
    def replace_view_mention(match):
        full = match.group(0)
        # Extract display name from inner text
        name = re.sub(r'<[^>]+>', '', full).strip()
        if name:
            return f"@{name}"
        # Fallback: try data-username attribute
        uname = re.search(r'data-username="([^"]*)"', full)
        if uname:
            return f"@{uname.group(1)}"
        return "@user"
    text = re.sub(r'<a[^>]*class="[^"]*confluence-userlink[^"]*"[^>]*>.*?</a>', replace_view_mention, text, flags=re.DOTALL)

    # --- Links ---
    def _redmine_anchor(heading_text):
        """Generate anchor name matching Redmine's sanitize_anchor_name."""
        import html as _html_mod
        clean = re.sub(r'<[^>]+>', '', heading_text)
        clean = _html_mod.unescape(clean)
        clean = re.sub(r'[^\w\s\-]', '', clean)
        clean = re.sub(r'\s+[-\s]*', '-', clean)
        return clean.strip('-')

    def convert_link(match):
        href = match.group(1)
        label = match.group(2)
        import html as _html_mod
        label_clean = re.sub(r'<[^>]+>', '', label).strip()
        label_clean = _html_mod.unescape(label_clean)
        if not label_clean:
            label_clean = href

        # --- Confluence anchor links (#id-XXXX-HeadingText) ---
        if href.startswith('#'):
            anchor = _redmine_anchor(label_clean)
            if fmt == "textile":
                return f'"{label_clean}":#{ anchor }'
            return f"[{label_clean}](#{anchor})"

        # --- Confluence page links (/pages/viewpage.action?pageId=XXX) ---
        page_id_match = re.search(r'pageId=(\d+)', href)
        if page_id_match:
            pid = page_id_match.group(1)
            page_info = page_id_map.get(pid)
            if isinstance(page_info, dict):
                wiki_title = page_info["title"]
                target_project = page_info.get("project", "")
            elif isinstance(page_info, str):
                wiki_title = page_info
                target_project = current_project or ""
            else:
                wiki_title = sanitize_wiki_title(label_clean)
                target_project = ""
            # Cross-project link if target is in a different Redmine project
            if target_project and current_project and target_project != current_project:
                return f"{{{{wiki({target_project}, {wiki_title}, {label_clean})}}}}"
            return f"[[{wiki_title}|{label_clean}]]"

        # --- Confluence /display/SPACE/Title links ---
        display_match = re.match(r'/display/([^/]+)/(.+?)(?:\?.*)?$', href)
        if display_match:
            link_space = display_match.group(1).lower()
            link_title = unquote(display_match.group(2).replace('+', ' '))
            wiki_title = sanitize_wiki_title(link_title)
            return f"{{{{wiki({link_space}, {wiki_title}, {label_clean})}}}}"

        # --- Other Confluence-relative URLs (skip broken relative links) ---
        if href.startswith('/') and not href.startswith('//'):
            # Relative Confluence URL that we can't resolve — use label as-is
            if fmt == "textile":
                return label_clean
            return label_clean

        # --- External / normal links ---
        if fmt == "textile":
            return f'"{label_clean}":{href}'
        return f"[{label_clean}]({href})"
    text = re.sub(r'<a[^>]*href="([^"]*)"[^>]*>(.*?)</a>', convert_link, text, flags=re.DOTALL)

    # --- Standalone images in HTML ---
    def convert_html_img(match):
        full = match.group(0)
        src_match = re.search(r'src="([^"]*)"', full)
        alt_match = re.search(r'alt="([^"]*)"', full)
        src = src_match.group(1) if src_match else ""
        alt = alt_match.group(1) if alt_match else ""
        if not src:
            return ""

        # body.view renders Confluence attachments as:
        #   <img src="/download/attachments/{pageId}/{filename}?..."
        #        data-linked-resource-default-alias="filename.png" ...>
        # Extract the filename and reference the local attachment instead.
        alias_match = re.search(r'data-linked-resource-default-alias="([^"]*)"', full)
        att_url_match = re.search(r'/download/(?:attachments|thumbnails)/\d+/([^?"]+)', src)
        width_match = re.search(r'width="(\d+)"', full)
        height_match = re.search(r'height="(\d+)"', full)
        w = width_match.group(1) if width_match else (height_match.group(1) if height_match else None)

        if alias_match:
            filename = alias_match.group(1)
            return "\n\n" + _img(filename).strip() + "\n\n"
        elif att_url_match:
            filename = unquote(att_url_match.group(1))
            return "\n\n" + _img(filename).strip() + "\n\n"

        # Non-Confluence image — keep the original URL
        if fmt == "textile":
            return f"!{src}!"
        return f"![{alt}]({src})"
    text = re.sub(r'<img[^>]*/?\s*>', convert_html_img, text, flags=re.DOTALL)

    # --- Lists ---
    def _clean_li_content(item_html):
        """Clean list item HTML while PRESERVING color/style spans and inline formatting.
        Strips structural tags (p, div) but keeps: span with style, strong, em, a, code, u, del, s."""
        text_out = item_html
        # Convert inline formatting to markup BEFORE stripping
        # Bold
        if fmt == "textile":
            text_out = re.sub(r'<strong[^>]*>(.*?)</strong>', r'*\1*', text_out, flags=re.DOTALL)
            text_out = re.sub(r'<b[^>]*>(.*?)</b>', r'*\1*', text_out, flags=re.DOTALL)
            text_out = re.sub(r'<em[^>]*>(.*?)</em>', r'_\1_', text_out, flags=re.DOTALL)
            text_out = re.sub(r'<i[^>]*>(.*?)</i>', r'_\1_', text_out, flags=re.DOTALL)
            text_out = re.sub(r'<del[^>]*>(.*?)</del>', r'-\1-', text_out, flags=re.DOTALL)
            text_out = re.sub(r'<s[^>]*>(.*?)</s>', r'-\1-', text_out, flags=re.DOTALL)
            text_out = re.sub(r'<u[^>]*>(.*?)</u>', r'+\1+', text_out, flags=re.DOTALL)
        else:
            text_out = re.sub(r'<strong[^>]*>(.*?)</strong>', r'**\1**', text_out, flags=re.DOTALL)
            text_out = re.sub(r'<b[^>]*>(.*?)</b>', r'**\1**', text_out, flags=re.DOTALL)
            text_out = re.sub(r'<em[^>]*>(.*?)</em>', r'*\1*', text_out, flags=re.DOTALL)
            text_out = re.sub(r'<i[^>]*>(.*?)</i>', r'*\1*', text_out, flags=re.DOTALL)
            text_out = re.sub(r'<del[^>]*>(.*?)</del>', r'~~\1~~', text_out, flags=re.DOTALL)
            text_out = re.sub(r'<s[^>]*>(.*?)</s>', r'~~\1~~', text_out, flags=re.DOTALL)
        # Inline code
        text_out = re.sub(r'<code[^>]*>(.*?)</code>', r'`\1`', text_out, flags=re.DOTALL)
        # Links
        def _li_link(m):
            href = m.group(1)
            label = re.sub(r'<[^>]+>', '', m.group(2)).strip()
            if not label: label = href
            if fmt == "textile":
                return f'"{label}":{href}'
            return f"[{label}]({href})"
        text_out = re.sub(r'<a[^>]*href="([^"]*)"[^>]*>(.*?)</a>', _li_link, text_out, flags=re.DOTALL)
        # Color spans → keep as HTML (Redmine passes through styled spans)
        # Convert Confluence color style to inline HTML span
        # Already <span style="color:..."> — keep these!
        # Strip structural/block tags: p, div, br → space/newline
        text_out = re.sub(r'<br\s*/?>', ' ', text_out)
        text_out = re.sub(r'</?p[^>]*>', ' ', text_out)
        text_out = re.sub(r'</?div[^>]*>', ' ', text_out)
        # Strip remaining non-style tags but KEEP: span (with style), u
        def _strip_non_style_tag(m):
            tag = m.group(0)
            # Keep span tags that have style attribute (colors, backgrounds)
            if re.match(r'</?span', tag, re.I):
                if 'style=' in tag or tag.startswith('</span'):
                    return tag
                return ''
            # Keep <u> for underline
            if re.match(r'</?u\b', tag, re.I):
                return tag
            return ''
        text_out = re.sub(r'<[^>]+>', _strip_non_style_tag, text_out)
        # Clean up whitespace
        text_out = re.sub(r'\s+', ' ', text_out).strip()
        return text_out

    def _split_list_items(inner_html):
        """Split <li>...</li> items properly, handling nested lists.
        Returns list of item HTML contents (between <li> and </li>)."""
        items = []
        depth = 0
        current = None
        pos = 0
        while pos < len(inner_html):
            next_tag = re.search(r'<(/?)(?:li|ul|ol)\b[^>]*>', inner_html[pos:])
            if not next_tag:
                break
            tag_start = pos + next_tag.start()
            tag_end = pos + next_tag.end()
            is_closing = next_tag.group(1) == '/'
            tag_name = re.search(r'</?(\w+)', next_tag.group(0)).group(1).lower()
            if tag_name == 'li' and not is_closing:
                if depth == 0:
                    current = tag_end
                depth += 1
            elif tag_name == 'li' and is_closing:
                depth -= 1
                if depth == 0 and current is not None:
                    items.append(inner_html[current:tag_start])
                    current = None
            pos = tag_end
        return items

    def _extract_nested_lists(item_html):
        """Extract nested <ul>/<ol> blocks from an <li> content.
        Finds the FIRST (outermost) nested list with all its children intact.
        Returns (text_before, [nested_list_html, ...])."""
        nested_lists = []
        remaining = item_html
        while True:
            # Find the first <ul> or <ol> opening tag
            first_open = re.search(r'<(ul|ol)\b[^>]*>', remaining)
            if not first_open:
                break
            # Find its balanced closing tag (counts ALL ul/ol, not just same type)
            depth = 1
            pos = first_open.end()
            while depth > 0 and pos < len(remaining):
                next_tag = re.search(r'<(/?)(?:ul|ol)\b[^>]*>', remaining[pos:])
                if not next_tag:
                    break
                if next_tag.group(1) == '/':
                    depth -= 1
                else:
                    depth += 1
                pos = pos + next_tag.end()
                if depth == 0:
                    break
            if depth == 0:
                nested_html = remaining[first_open.start():pos]
                nested_lists.append(nested_html)
                before = remaining[:first_open.start()]
                after = remaining[pos:]
                remaining = before + after
            else:
                break
        return remaining.strip(), nested_lists

    def convert_list_recursive(html, depth=0):
        result = ""
        # Use 3 spaces per depth: enough for CommonMark nesting, avoids code block
        indent = "   " * depth
        list_match = re.match(r'^<(ul|ol)[^>]*>(.*)</\1>$', html.strip(), re.DOTALL)
        if not list_match:
            return html
        tag_full = re.match(r'^<(ul|ol)([^>]*)>', html.strip())
        list_type = list_match.group(1)
        inner = list_match.group(2)
        # Respect start= attribute on <ol>
        start_num = 1
        if tag_full:
            start_m = re.search(r'start="(\d+)"', tag_full.group(2))
            if start_m:
                start_num = int(start_m.group(1))
        items = _split_list_items(inner)
        for idx, item_html in enumerate(items):
            # Extract nested sub-lists from end of this item
            before_html, nested_lists = _extract_nested_lists(item_html)
            item_text = _clean_li_content(before_html)

            # Strip duplicate numbering: Confluence body.view sometimes
            # bakes "1." "2." etc. into the text of <ol> items
            if list_type == "ol":
                item_text = re.sub(r'^\d+[\.\)]\s*', '', item_text)

            # Task list checkbox detection
            is_checked = bool(re.search(r'class="[^"]*checked[^"]*"', item_html))
            is_unchecked = bool(re.search(r'class="[^"]*unchecked[^"]*"', item_html))

            if fmt == "textile":
                marker = "#" * (depth + 1) if list_type == "ol" else "*" * (depth + 1)
                result += f"{marker} {item_text}\n"
            elif is_checked:
                result += f"{indent}- [x] {item_text}\n"
            elif is_unchecked:
                result += f"{indent}- [ ] {item_text}\n"
            elif list_type == "ol":
                num = start_num + idx
                result += f"{indent}{num}. {item_text}\n"
            else:
                result += f"{indent}- {item_text}\n"

            # Process nested sub-lists
            for nested_list in nested_lists:
                result += convert_list_recursive(nested_list, depth + 1)
        return result

    def _replace_outermost_lists(text_in):
        """Find and replace outermost <ul>/<ol> blocks using balanced tag matching."""
        result = ""
        i = 0
        while i < len(text_in):
            m = re.search(r'<(ul|ol)\b[^>]*>', text_in[i:])
            if not m:
                result += text_in[i:]
                break
            # Add text before the list
            result += text_in[i:i + m.start()]
            tag_name = m.group(1)
            # Find matching closing tag (balanced)
            depth = 1
            pos = i + m.end()
            while depth > 0 and pos < len(text_in):
                next_tag = re.search(rf'<(/?)(ul|ol)\b[^>]*>', text_in[pos:])
                if not next_tag:
                    break
                if next_tag.group(1) == '/' and next_tag.group(2) == tag_name:
                    depth -= 1
                elif next_tag.group(1) == '' and next_tag.group(2) == tag_name:
                    depth += 1
                pos = pos + next_tag.end()
                if depth == 0:
                    break
            if depth == 0:
                list_html = text_in[i + m.start():pos]
                result += "\n" + convert_list_recursive(list_html) + "\n"
                i = pos
            else:
                # Unbalanced — just include as-is
                result += text_in[i + m.start():i + m.end()]
                i = i + m.end()
        return result

    text = _replace_outermost_lists(text)

    # --- Paragraphs, line breaks ---
    text = re.sub(r'<p[^>]*>', '\n', text)
    text = re.sub(r'</p>', '\n', text)
    text = re.sub(r'<br\s*/?>', '\n', text)
    text = re.sub(r'<hr\s*/?>', '\n---\n', text)

    # --- Definition lists ---
    def convert_dl(match):
        dl_html = match.group(0)
        items = re.findall(r'<dt[^>]*>(.*?)</dt>\s*<dd[^>]*>(.*?)</dd>', dl_html, re.DOTALL)
        result = "\n"
        for term, definition in items:
            t = re.sub(r'<[^>]+>', '', term).strip()
            d = re.sub(r'<[^>]+>', '', definition).strip()
            if fmt == "textile":
                result += f"- *{t}*: {d}\n"
            else:
                result += f"**{t}**: {d}\n\n"
        return result
    text = re.sub(r'<dl[^>]*>.*?</dl>', convert_dl, text, flags=re.DOTALL)

    # --- Blockquotes ---
    def convert_blockquote(match):
        content = match.group(1)
        content = re.sub(r'<[^>]+>', ' ', content).strip()
        lines = content.split('\n')
        return "\n" + "\n".join(f"> {line.strip()}" for line in lines if line.strip()) + "\n"
    text = re.sub(r'<blockquote[^>]*>(.*?)</blockquote>', convert_blockquote, text, flags=re.DOTALL)

    # --- Strip remaining HTML (keep: styled spans, u, tables, anchors) ---
    text = re.sub(r'<(?!/?(?:span|u|table|thead|tbody|tr|th|td|a\s|a>)\b)[^>]+>', '', text)

    # =========================================================================
    # PHASE 4: Cleanup
    # =========================================================================
    import html as html_module
    text = html_module.unescape(text)

    # Fix Redmine issue reference collision (# followed by digit)
    lines = text.split('\n')
    fixed_lines = []
    for line in lines:
        stripped = line.lstrip()
        # Don't escape actual headings
        if fmt == "markdown" and re.match(r'^#{1,6}\s', stripped):
            fixed_lines.append(line)
        elif re.match(r'^#\d', stripped):
            line = line.replace('#', '\\#', 1)
            fixed_lines.append(line)
        else:
            fixed_lines.append(line)
    text = '\n'.join(fixed_lines)

    # Fix broken markdown formatting
    if fmt != "textile":
        # Remove empty bold/italic markers: ** ** or **** or * *
        text = re.sub(r'\*\*\s*\*\*', '', text)
        text = re.sub(r'\*\s*\*(?!\*)', '', text)
        # Fix bold markers stuck to punctuation: **text**. → **text** .
        # (not needed, markdown handles this)
        # Fix adjacent bold markers: **text****text2** → **text** **text2**
        text = re.sub(r'\*\*\*\*', '** **', text)
        # Strip leftover HTML span/font tags that might wrap bold content
        text = re.sub(r'</?(?:span|font)[^>]*>', '', text)

    # Clean up whitespace
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = re.sub(r'[ \t]+\n', '\n', text)
    text = text.strip()

    return text


# =============================================================================
# MAIN PIPELINE
# =============================================================================

def migrate_space(confluence, redmine, space_key, project_id,
                  with_history=False, dry_run=False, delay=0.1,
                  tmp_dir="/tmp/confluence_migration", fmt="markdown",
                  max_versions=0, global_page_id_map=None,
                  concurrency=2, batch_size=25, health=None,
                  page_cooldown=0, version_delay=1.0):
    print(f"\n{'='*60}")
    print(f"Migrating space [{space_key}] -> project [{project_id}]")
    print(f"{'='*60}")

    pages = confluence.get_space_pages(space_key)
    if not pages:
        print(f"  No pages found in space {space_key}")
        return

    pages_by_id = {p["id"]: p for p in pages}
    # Build local page_id_map and merge into global map
    local_map = {str(p["id"]): sanitize_wiki_title(p["title"]) for p in pages}
    if global_page_id_map is not None:
        # Update global map with this space's pages (adds project context)
        for pid, wiki_title in local_map.items():
            if pid not in global_page_id_map:
                global_page_id_map[pid] = {"title": wiki_title, "project": project_id}
        page_id_map = global_page_id_map
    else:
        # Standalone mode — simple dict without project context
        page_id_map = {pid: {"title": wt, "project": project_id} for pid, wt in local_map.items()}
    roots = []
    children_map = {}
    for p in pages:
        ancestors = p.get("ancestors", [])
        if ancestors:
            parent_id = ancestors[-1]["id"]
            children_map.setdefault(parent_id, []).append(p)
        else:
            roots.append(p)

    ordered = []
    queue = list(roots)
    while queue:
        page = queue.pop(0)
        ordered.append(page)
        children = children_map.get(page["id"], [])
        queue.extend(children)

    print(f"  Pages to import: {len(ordered)} ({len(roots)} root, {len(ordered)-len(roots)} nested)")

    if dry_run:
        for p in ordered[:30]:
            ancestors = p.get("ancestors", [])
            depth = len(ancestors)
            parent = ancestors[-1]["title"] if ancestors else "(root)"
            body_len = len(p.get("body", {}).get("view", {}).get("value", "") or p.get("body", {}).get("storage", {}).get("value", ""))
            indent = "  " * depth
            print(f"    {indent}{sanitize_wiki_title(p['title'])} [parent: {parent}, body: {body_len}b]")
        if len(ordered) > 30:
            print(f"    ... and {len(ordered)-30} more")
        return

    _space_start = time.time()
    created = 0
    version_count = 0
    errors = 0
    created_wiki_titles = set()

    for idx, page in enumerate(ordered):
        # Health check: wait if server is overloaded before starting next page
        if health:
            health.wait_if_paused(timeout=300)
            extra = health.throttle_delay
            if extra > 0:
                time.sleep(extra)

        title = page["title"]
        wiki_title = sanitize_wiki_title(title)
        page_id = page["id"]

        parent_wiki = None
        ancestors = page.get("ancestors", [])
        if ancestors:
            parent_title = ancestors[-1]["title"]
            parent_sanitized = sanitize_wiki_title(parent_title)
            if parent_sanitized in created_wiki_titles:
                parent_wiki = parent_sanitized

        children = children_map.get(page_id, [])
        child_md = ""
        if children:
            child_md = "\n\n---\n\n{{child_pages}}\n"

        versions = []
        if with_history:
            versions = confluence.get_page_versions(page_id)
            if len(versions) > 1:
                total_in_confluence = len(versions)  # includes current
                old_ver_nums = [v["number"] for v in versions[:-1]]
                # Apply version limit (default 200 — prevents server overload)
                if max_versions > 0 and len(old_ver_nums) > max_versions:
                    limit_skipped = len(old_ver_nums) - max_versions
                    old_ver_nums = old_ver_nums[-max_versions:]
                    print(f"    [{wiki_title}] {total_in_confluence} vers in Confluence, importing last {max_versions} + current, skipping {limit_skipped} oldest", flush=True)
                elif len(old_ver_nums) > 500:
                    # Safety: warn about very large version counts even with unlimited setting
                    print(f"    [{wiki_title}] WARNING: {total_in_confluence} versions — this is very large, consider --max-versions", flush=True)
                else:
                    print(f"    [{wiki_title}] {total_in_confluence} vers in Confluence, fetching {len(old_ver_nums)} old...", end="", flush=True)

                # Stream: fetch batch → push to Redmine → discard → next batch
                # This keeps memory usage proportional to batch_size, not total versions.
                BATCH_SIZE = batch_size
                imported_v = 0
                failed_v = []
                total_fetched = 0

                # Get initial Redmine version number for this page (if it exists already)
                _page_info = redmine.get_wiki_page_info(project_id, wiki_title)
                redmine_ver_before = _page_info.get("version", 0) if _page_info else None

                for batch_start in range(0, len(old_ver_nums), BATCH_SIZE):
                    batch = old_ver_nums[batch_start:batch_start + BATCH_SIZE]
                    batch_bodies, _batch_errs = confluence.fetch_version_bodies_sequential(
                        page_id, batch, request_delay=version_delay
                    )
                    total_fetched += len(batch_bodies)

                    # Push this batch to Redmine immediately (ascending order)
                    for ver_num in sorted(batch):
                        ver_data = batch_bodies.get(ver_num)
                        if not ver_data:
                            failed_v.append(f"v{ver_num}:no_data")
                            continue

                        ver_body_html = ver_data.get("body", {}).get("view", {}).get("value", "")
                        if not ver_body_html:
                            ver_body_html = ver_data.get("body", {}).get("storage", {}).get("value", "")
                        md = convert_html_to_markdown(ver_body_html, fmt=fmt, page_id_map=page_id_map, current_project=project_id) or "*Empty version*"

                        v_info = ver_data.get("version", {})
                        author = v_info.get("by", {}).get("displayName", "unknown")
                        when = v_info.get("when", "")[:19]
                        comment = f"v{ver_num}/{total_in_confluence} by {author} ({when})"

                        full = f"# {title}\n\n{md}\n\n<!-- Confluence version {ver_num}/{total_in_confluence} -->"

                        result = redmine.put_wiki_page(project_id, wiki_title, full, comments=comment)
                        if result is not False:
                            version_count += 1
                            imported_v += 1
                        else:
                            failed_v.append(f"v{ver_num}:push_fail")
                        time.sleep(delay)

                    # Discard batch data to free memory before next batch
                    del batch_bodies

                    # Breathing room between batches + active health probe
                    if batch_start + BATCH_SIZE < len(old_ver_nums):
                        print(f"      [{wiki_title}] batch {batch_start + BATCH_SIZE}/{len(old_ver_nums)} done ({imported_v} pushed)...", flush=True)
                        # Active probe: measure actual response time right now
                        probe_delay = 5.0
                        try:
                            _t0 = time.time()
                            confluence.session.get(
                                f"{confluence.api_base}/space",
                                params={"limit": 1}, timeout=20)
                            _probe_rt = time.time() - _t0
                            if _probe_rt > 8.0:
                                print(f"      [HEALTH] Probe: {_probe_rt:.1f}s — server under GC pressure, waiting 30s...")
                                probe_delay = 30.0
                            elif _probe_rt > 3.0:
                                print(f"      [HEALTH] Probe: {_probe_rt:.1f}s — server warm, waiting 10s...")
                                probe_delay = 10.0
                        except Exception:
                            print(f"      [HEALTH] Probe failed — server unresponsive, waiting 60s...")
                            probe_delay = 60.0
                        if health:
                            health.wait_if_paused(timeout=300)
                            probe_delay = max(probe_delay, health.throttle_delay)
                        time.sleep(probe_delay)

                # Verify: check Redmine version count after pushing
                _page_after = redmine.get_wiki_page_info(project_id, wiki_title)
                redmine_ver_after = _page_after.get("version", 0) if _page_after else 0
                new_revisions = redmine_ver_after - (redmine_ver_before or 0)

                parts = [f"{imported_v}/{len(old_ver_nums)} pushed (fetched {total_fetched})"]
                if failed_v:
                    parts.append(f"{len(failed_v)} FAILED: {failed_v}")
                parts.append(f"Redmine revisions: {new_revisions}")
                if new_revisions < imported_v:
                    parts.append(f"MISMATCH: pushed {imported_v} but only {new_revisions} new revisions!")
                print(f"      [{wiki_title}] {', '.join(parts)}")
            elif len(versions) == 1:
                print(f"    [{wiki_title}] 1 version only (no history to import)")
            else:
                print(f"    [{wiki_title}] could not retrieve version history")

        # Fetch the current version body using the SAME method as old versions.
        # This ensures identical HTML format — the "fresh" endpoint (without
        # status=historical) can return body.storage in a different format on
        # some Confluence versions, causing content to differ from old versions.
        body_html = ""
        if with_history and versions:
            latest_ver_num = versions[-1]["number"]
            _cur_data = confluence.get_page_version_body(page_id, latest_ver_num, use_view=True)
            if _cur_data:
                body_html = _cur_data.get("body", {}).get("view", {}).get("value", "")
                if not body_html:
                    # body.view empty for historical — try body.storage from same response
                    body_html = _cur_data.get("body", {}).get("storage", {}).get("value", "")
                _cur_ver = _cur_data.get("version", {})
                if _cur_ver:
                    page["version"] = _cur_ver
                if body_html:
                    print(f"    [{wiki_title}] current v{latest_ver_num} fetched ({len(body_html)}b)")

        # Fallback 1: direct GET with body.view + body.storage (request both)
        if not body_html:
            _fresh = confluence.session.get(
                f"{confluence.api_base}/content/{page_id}",
                params={"expand": "body.view,body.storage,version"},
                timeout=30,
            )
            if _fresh.status_code == 200:
                _fresh_data = _fresh.json()
                body_html = _fresh_data.get("body", {}).get("view", {}).get("value", "")
                if not body_html:
                    body_html = _fresh_data.get("body", {}).get("storage", {}).get("value", "")
                _fresh_ver = _fresh_data.get("version", {})
                if _fresh_ver:
                    page["version"] = _fresh_ver
                if body_html:
                    print(f"    [{wiki_title}] current fetched via direct GET ({len(body_html)}b)")
            else:
                print(f"    [{wiki_title}] direct GET returned {_fresh.status_code}")

        # Fallback 2: body from page listing data
        if not body_html:
            body_html = page.get("body", {}).get("view", {}).get("value", "")
            if not body_html:
                body_html = page.get("body", {}).get("storage", {}).get("value", "")

        if not body_html:
            print(f"    [WARN] Could not fetch body for {wiki_title} (page_id={page_id}) — page may be empty")

        md = convert_html_to_markdown(body_html, fmt=fmt, page_id_map=page_id_map, current_project=project_id) or "*Empty page*"
        md += child_md
        md = md.replace("\x00", "")
        full_text = f"# {title}\n\n{md}"

        upload_tokens = []
        att_dir = os.path.join(tmp_dir, space_key, str(page_id))
        uploaded_filenames = set()

        # 1) Upload attachments from Confluence attachment API
        atts = confluence.get_page_attachments(page_id)
        if atts:
            def _download_and_upload(att):
                dl_path = att.get("_links", {}).get("download", "")
                if not dl_path:
                    return None
                original_fn = att.get("title", "attachment")
                safe_fn = sanitize_filename(original_fn)
                try:
                    local_path = confluence.download_attachment(dl_path, att_dir)
                    if local_path:
                        token = redmine.upload_file(local_path, safe_fn)
                        if token:
                            return {"token": token, "filename": safe_fn, "content_type": att.get("mediaType", "application/octet-stream")}
                except Exception as ex:
                    print(f"    [WARN] Attachment '{original_fn}': {ex}")
                return None
            att_workers = min(concurrency, len(atts))
            with ThreadPoolExecutor(max_workers=att_workers) as pool:
                futures = [pool.submit(_download_and_upload, a) for a in atts]
                for f in as_completed(futures):
                    result = f.result()
                    if result:
                        upload_tokens.append(result)
                        uploaded_filenames.add(result["filename"])

        # 2) Download images referenced in body.view HTML but missing from attachments.
        #    body.view can reference images from other pages or inline embeds that
        #    aren't in this page's attachment list.
        if body_html:
            view_images = extract_body_view_images(body_html)
            missing_images = {fn: url for fn, url in view_images.items() if fn not in uploaded_filenames}
            if missing_images:
                os.makedirs(att_dir, exist_ok=True)
                for safe_fn, rel_url in missing_images.items():
                    try:
                        img_url = confluence.base_url + rel_url
                        img_resp = confluence.session.get(img_url, stream=True, timeout=30)
                        if img_resp.status_code == 200:
                            local_path = os.path.join(att_dir, safe_fn)
                            with open(local_path, "wb") as fimg:
                                for chunk in img_resp.iter_content(8192):
                                    fimg.write(chunk)
                            token = redmine.upload_file(local_path, safe_fn)
                            if token:
                                ct = img_resp.headers.get("content-type", "application/octet-stream")
                                upload_tokens.append({"token": token, "filename": safe_fn, "content_type": ct})
                                uploaded_filenames.add(safe_fn)
                    except Exception as ex:
                        print(f"    [WARN] body.view image '{safe_fn}': {ex}")
                if missing_images:
                    print(f"    [{wiki_title}] downloaded {len(uploaded_filenames & set(missing_images))} extra images from body.view")

        ver = page.get("version", {})
        author = ver.get("by", {}).get("displayName", "")
        when = ver.get("when", "")[:19]
        comment = f"Current version by {author} ({when})" if author else None

        ok = redmine.put_wiki_page(
            project_id, wiki_title, full_text,
            parent_title=parent_wiki,
            uploads=upload_tokens if upload_tokens else None,
            comments=comment,
        )
        if ok:
            created += 1
            created_wiki_titles.add(wiki_title)
            a_str = f", {len(upload_tokens)} att" if upload_tokens else ""
            depth = len(ancestors)
            print(f"  [OK] {'  '*depth}{wiki_title}{a_str}")
        else:
            errors += 1
            print(f"  [ERR] {wiki_title}")

        # Free memory after each page (especially important for pages with history)
        if with_history and versions and len(versions) > 5:
            gc.collect()

        # Extra cooldown after heavy pages (many versions = lots of server heap used)
        if with_history and versions and len(versions) > 50 and page_cooldown > 0:
            cool = min(page_cooldown, 30)  # cap at 30s for per-page
            print(f"      [COOLDOWN] {len(versions)} versions processed — waiting {cool}s for server GC...")
            time.sleep(cool)

        # Periodic cooldown every 50 pages within a space
        if page_cooldown > 0 and (idx + 1) % 50 == 0 and (idx + 1) < len(ordered):
            print(f"  [COOLDOWN] {idx+1}/{len(ordered)} pages done — waiting {page_cooldown}s for server GC...")
            time.sleep(page_cooldown)

        if (idx + 1) % 25 == 0:
            elapsed = time.time() - _space_start
            rate = (idx + 1) / elapsed if elapsed > 0 else 0
            print(f"  --- Progress: {idx+1}/{len(ordered)} ({rate:.1f} pages/s) ---")

    elapsed = time.time() - _space_start
    print(f"\n  Space {space_key}: {created} pages, {version_count} old versions, {errors} errors ({elapsed:.0f}s)")

    import shutil
    att_space_dir = os.path.join(tmp_dir, space_key)
    if os.path.exists(att_space_dir):
        shutil.rmtree(att_space_dir, ignore_errors=True)
    gc.collect()


def main():
    parser = argparse.ArgumentParser(description="Migrate Confluence spaces to Redmine wikis via REST API",
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--confluence-url", required=True)
    parser.add_argument("--confluence-user", help="Confluence username (basic auth)")
    parser.add_argument("--confluence-pass", help="Confluence password")
    parser.add_argument("--confluence-pat", help="Confluence Personal Access Token")
    parser.add_argument("--no-verify-ssl", action="store_true")
    parser.add_argument("--redmine-url", required=True)
    parser.add_argument("--redmine-key", required=True)
    parser.add_argument("--spaces", help="Comma-separated space keys or Jira keys (resolved via --excel-map)")
    parser.add_argument("--exclude-spaces", help="Comma-separated space keys to skip")
    parser.add_argument("--excel-map", help="Excel mapping file")
    parser.add_argument("--with-history", action="store_true")
    parser.add_argument("--max-versions", type=int, default=200,
                        help="Max old versions to import per page (default: 200, 0=unlimited)")
    parser.add_argument("--concurrency", type=int, default=2,
                        help="Max parallel Confluence requests (default: 2, lower=gentler on server)")
    parser.add_argument("--batch-size", type=int, default=25,
                        help="Versions to fetch per batch (default: 25, lower=less server memory)")
    parser.add_argument("--space-cooldown", type=int, default=0,
                        help="Seconds to wait between spaces for server GC (default: 0, recommended: 300 for constrained servers)")
    parser.add_argument("--page-cooldown", type=int, default=0,
                        help="Seconds to pause every 50 pages within a space (default: 0, recommended: 60 for large spaces)")
    parser.add_argument("--create-projects", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--delay", type=float, default=0.1,
                        help="Delay between Redmine pushes (default: 0.1s)")
    parser.add_argument("--version-delay", type=float, default=1.0,
                        help="Delay between Confluence version fetches (default: 1.0s, critical for GC-constrained servers)")
    parser.add_argument("--tmp-dir", default="/tmp/confluence_migration")
    parser.add_argument("--list-spaces", action="store_true")
    parser.add_argument("--format", choices=["markdown", "textile"], default="markdown",
                        help="Redmine text format (check Admin > Settings > General > Text formatting)")
    args = parser.parse_args()

    confluence_kwargs = {"verify_ssl": not args.no_verify_ssl,
                         "max_connections": args.concurrency + 2}  # pool slightly larger than workers
    if args.confluence_pat:
        confluence_kwargs["pat"] = args.confluence_pat
    elif args.confluence_user:
        confluence_pass = args.confluence_pass
        if not confluence_pass:
            import getpass
            confluence_pass = getpass.getpass(f"Confluence password for {args.confluence_user}: ")
        confluence_kwargs["username"] = args.confluence_user
        confluence_kwargs["password"] = confluence_pass
    else:
        print("[ERROR] Provide --confluence-pat or --confluence-user")
        sys.exit(1)

    print(f"[INFO] Concurrency: {args.concurrency} workers, batch size: {args.batch_size}")
    confluence = ConfluenceClient(args.confluence_url, **confluence_kwargs)
    redmine = RedmineClient(args.redmine_url, args.redmine_key)

    # Start background health monitor
    health = ConfluenceHealthMonitor(confluence.session, confluence.api_base)
    health.start()

    print("\n[STEP 1] Fetching Confluence spaces...")
    all_spaces = confluence.get_all_spaces()

    if args.list_spaces:
        print(f"\n{'Key':<15} {'Name':<50} {'Type'}")
        print("-" * 80)
        for s in sorted(all_spaces, key=lambda x: x["key"]):
            print(f"{s['key']:<15} {s['name']:<50} {s.get('type', '?')}")
        sys.exit(0)

    # Load Excel mapping BEFORE filtering spaces (needed for Jira key resolution)
    excel_data = {"entries": [], "conf_to_redmine": {}, "jira_to_conf": {}}
    if args.excel_map:
        print("\n[STEP 2] Loading Excel mapping...")
        excel_data = load_excel_mapping(args.excel_map)
    else:
        print("\n[STEP 2] No Excel mapping -- will use Confluence key as Redmine identifier")

    # Filter spaces -- resolves Jira keys to Confluence keys via Excel
    if args.spaces:
        requested = set(k.strip() for k in args.spaces.split(","))
        print(f"\n[INFO] Resolving requested keys: {requested}")
        resolved = resolve_space_keys(requested, excel_data, all_spaces)
        if not resolved:
            print("[ERROR] None of the requested keys resolved to Confluence spaces")
            print("  Tip: --list-spaces shows available Confluence space keys")
            if excel_data.get("jira_to_conf"):
                print("  Tip: With --excel-map, Jira keys auto-resolve to Confluence spaces")
            sys.exit(1)
        spaces = [s for s in all_spaces if s["key"] in resolved]
        print(f"[INFO] Resolved to {len(spaces)} space(s): {[s['key'] for s in spaces]}")
    else:
        spaces = all_spaces

    if args.exclude_spaces:
        exclude = set(k.strip().upper() for k in args.exclude_spaces.split(","))
        spaces = [s for s in spaces if s["key"].upper() not in exclude]

    print(f"\n[INFO] {len(spaces)} spaces to migrate")

    print("\n[STEP 3] Checking Redmine projects...")
    if args.create_projects and excel_data["entries"]:
        redmine_projects = ensure_redmine_projects(redmine, excel_data, dry_run=args.dry_run)
    else:
        redmine_projects = redmine.get_projects()
    print(f"[INFO] {len(redmine_projects)} Redmine projects available")

    print("\n[STEP 4] Building space -> project mapping...")
    conf_to_redmine = excel_data.get("conf_to_redmine", {})
    space_map = build_space_to_project_map(spaces, redmine_projects, conf_to_redmine if conf_to_redmine else None)

    new_count = sum(1 for v in space_map.values() if not v["exists"])
    existing_count = sum(1 for v in space_map.values() if v["exists"])
    print(f"\n  Existing projects: {existing_count}")
    print(f"  Need to create: {new_count}")

    if new_count > 0:
        print(f"\n  New projects to create:")
        for key, info in space_map.items():
            if not info["exists"]:
                print(f"    {key} -> {info['identifier']} ({info['name']})")

    if new_count > 0 and args.create_projects and not args.dry_run:
        print(f"\n[STEP 5] Creating {new_count} missing Redmine projects...")
        unmapped_parent_id = None
        unmapped_parent_identifier = "unmapped-confluence"
        if unmapped_parent_identifier not in redmine_projects:
            result = redmine.create_project("Unmapped Confluence Spaces", unmapped_parent_identifier)
            if result:
                unmapped_parent_id = result["id"]
                redmine_projects[unmapped_parent_identifier] = result
                time.sleep(0.2)
        else:
            unmapped_parent_id = redmine_projects[unmapped_parent_identifier]["id"]
        for key, info in space_map.items():
            if not info["exists"]:
                result = redmine.create_project(info["name"], info["identifier"], parent_id=unmapped_parent_id)
                if result:
                    info["exists"] = True
                    redmine_projects[info["identifier"]] = result
                    print(f"  + Created: {info['identifier']} ({info['name']})")
                else:
                    print(f"  x Failed: {info['identifier']}")
                time.sleep(0.2)
    elif new_count > 0 and args.create_projects and args.dry_run:
        print(f"\n[STEP 5] Would create {new_count} unmapped spaces:")
        for key, info in space_map.items():
            if not info["exists"]:
                print(f"  [DRY-RUN] {info['identifier']} ({info['name']})")
    elif new_count > 0 and not args.create_projects:
        print(f"\n[WARN] {new_count} spaces have no Redmine project. Use --create-projects.")

    # Build global page_id_map across ALL spaces for cross-project link resolution.
    # Maps Confluence page ID (str) → {"title": redmine_wiki_title, "project": redmine_project_id}
    print(f"\n[STEP 6] Building global page ID map for link resolution...")
    global_page_id_map = {}
    for space in spaces:
        key = space["key"]
        info = space_map.get(key)
        if not info or not info["exists"]:
            continue
        space_pages = confluence.get_space_pages(key, expand="version,ancestors")
        for p in space_pages:
            pid = str(p["id"])
            if pid not in global_page_id_map:
                global_page_id_map[pid] = {
                    "title": sanitize_wiki_title(p["title"]),
                    "project": info["identifier"],
                }
    print(f"  Indexed {len(global_page_id_map)} pages across {len(spaces)} spaces")

    print(f"\n[STEP 7] Migrating spaces...")
    spaces_done = 0
    for space in spaces:
        key = space["key"]
        info = space_map.get(key)
        if not info or not info["exists"]:
            print(f"\n  Skipping {key} -- no Redmine project")
            continue

        # Cooldown between spaces — lets Confluence GC reclaim heap from previous space
        if spaces_done > 0 and args.space_cooldown > 0:
            print(f"\n[COOLDOWN] Waiting {args.space_cooldown}s for Confluence GC before next space...")
            time.sleep(args.space_cooldown)
            print(f"[COOLDOWN] Done, continuing with {key}")

        migrate_space(confluence=confluence, redmine=redmine, space_key=key, project_id=info["identifier"],
                      with_history=args.with_history, dry_run=args.dry_run, delay=args.delay, tmp_dir=args.tmp_dir,
                      fmt=args.format, max_versions=args.max_versions,
                      global_page_id_map=global_page_id_map,
                      concurrency=args.concurrency, batch_size=args.batch_size,
                      health=health, page_cooldown=args.page_cooldown,
                      version_delay=args.version_delay)
        spaces_done += 1

    health.stop()
    print(f"\n{'='*60}")
    print("Migration complete!")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()