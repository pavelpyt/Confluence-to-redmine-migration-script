#!/usr/bin/env python3
"""
Confluence Version Debug Tool
Fetches a single page using ALL possible API methods and compares the results.
Shows exactly which method returns what content, and where differences occur.

Usage:
    python confluence_version_debug.py \
        --url https://confluence.zentity.com \
        --pat 'YOUR_PAT' \
        --page-id 12345

    # Or by space key + title:
    python confluence_version_debug.py \
        --url https://confluence.zentity.com \
        --pat 'YOUR_PAT' \
        --space IDMUNIPET \
        --title "Page Title Here"
"""

import argparse
import json
import hashlib
import sys
import time
import difflib
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def make_session(base_url, pat):
    s = requests.Session()
    s.verify = False
    s.headers["Authorization"] = f"Bearer {pat}"
    api = f"{base_url.rstrip('/')}/rest/api"
    # Verify
    r = s.session = s
    resp = s.get(f"{api}/space", params={"limit": 1}, timeout=10)
    if resp.status_code != 200:
        print(f"[ERROR] API returned {resp.status_code}: {resp.text[:200]}")
        sys.exit(1)
    print(f"[OK] Connected to {api}")
    return s, api


def find_page_id(session, api, space_key, title):
    """Find page ID by space key + title."""
    cql = f'space="{space_key}" AND title="{title}" AND type=page'
    resp = session.get(f"{api}/content/search", params={"cql": cql, "limit": 5})
    if resp.status_code != 200:
        print(f"[ERROR] CQL search failed: {resp.status_code}")
        sys.exit(1)
    results = resp.json().get("results", [])
    if not results:
        print(f"[ERROR] No page found for space={space_key}, title={title}")
        sys.exit(1)
    page = results[0]
    print(f"[OK] Found page: {page['title']} (id={page['id']})")
    return page["id"]


def fetch_method_1_current(session, api, page_id):
    """Method 1: GET /content/{id}?expand=body.storage,version (no status param)"""
    resp = session.get(
        f"{api}/content/{page_id}",
        params={"expand": "body.storage,version"},
        timeout=30,
    )
    return resp


def fetch_method_2_current_view(session, api, page_id):
    """Method 2: GET /content/{id}?expand=body.view,version"""
    resp = session.get(
        f"{api}/content/{page_id}",
        params={"expand": "body.view,version"},
        timeout=30,
    )
    return resp


def fetch_method_3_historical(session, api, page_id, ver_num):
    """Method 3: GET /content/{id}?expand=body.storage,version&status=historical&version=N"""
    resp = session.get(
        f"{api}/content/{page_id}",
        params={
            "expand": "body.storage,version",
            "status": "historical",
            "version": ver_num,
        },
        timeout=30,
    )
    return resp


def fetch_method_4_current_status(session, api, page_id):
    """Method 4: GET /content/{id}?expand=body.storage,version&status=current"""
    resp = session.get(
        f"{api}/content/{page_id}",
        params={
            "expand": "body.storage,version",
            "status": "current",
        },
        timeout=30,
    )
    return resp


def fetch_method_5_cql(session, api, page_id):
    """Method 5: CQL search by ID with body expand"""
    cql = f'id={page_id} AND type=page'
    resp = session.get(
        f"{api}/content/search",
        params={"cql": cql, "expand": "body.storage,version", "limit": 1},
        timeout=30,
    )
    return resp


def get_version_list(session, api, page_id):
    """Get all version numbers from /content/{id}/version endpoint."""
    versions = []
    start = 0
    while True:
        resp = session.get(
            f"{api}/content/{page_id}/version",
            params={"start": start, "limit": 200},
            timeout=30,
        )
        if resp.status_code != 200:
            print(f"  /version endpoint returned {resp.status_code}, falling back to /history")
            # Fallback to history
            resp2 = session.get(f"{api}/content/{page_id}/history", timeout=30)
            if resp2.status_code == 200:
                latest = resp2.json().get("lastUpdated", {}).get("number", 1)
                return list(range(1, latest + 1)), "history_range"
            return [], "failed"
        data = resp.json()
        for v in data.get("results", []):
            versions.append(v.get("number"))
        if data.get("size", 0) < 200:
            break
        start += 200
    return sorted(versions), "version_api"


def short_hash(text):
    return hashlib.md5(text.encode()).hexdigest()[:12]


def extract_body(resp_or_data, body_type="storage"):
    """Extract body HTML from response, handling both direct and CQL responses."""
    if isinstance(resp_or_data, requests.Response):
        if resp_or_data.status_code != 200:
            return None, f"HTTP {resp_or_data.status_code}"
        data = resp_or_data.json()
    else:
        data = resp_or_data

    # CQL search wraps results in an array
    if "results" in data:
        results = data["results"]
        if not results:
            return None, "empty results"
        data = results[0]

    body = data.get("body", {})
    storage = body.get(body_type, {})
    value = storage.get("value", "")
    ver = data.get("version", {}).get("number", "?")
    return value, f"v{ver}"


def main():
    parser = argparse.ArgumentParser(description="Debug Confluence page version fetching")
    parser.add_argument("--url", required=True, help="Confluence base URL")
    parser.add_argument("--pat", required=True, help="Personal Access Token")
    parser.add_argument("--page-id", help="Page ID to inspect")
    parser.add_argument("--space", help="Space key (used with --title)")
    parser.add_argument("--title", help="Page title (used with --space)")
    parser.add_argument("--compare-versions", type=str, default="",
                        help="Comma-separated version numbers to compare, e.g. '43,44,45'. Empty = last 3 + current methods")
    args = parser.parse_args()

    session, api = make_session(args.url, args.pat)

    if args.page_id:
        page_id = args.page_id
    elif args.space and args.title:
        page_id = find_page_id(session, api, args.space, args.title)
    else:
        print("[ERROR] Provide --page-id or both --space and --title")
        sys.exit(1)

    # =========================================================================
    # Step 1: Get version list
    # =========================================================================
    print(f"\n{'='*70}")
    print(f"PAGE ID: {page_id}")
    print(f"{'='*70}")

    ver_nums, ver_source = get_version_list(session, api, page_id)
    print(f"\nVersions: {len(ver_nums)} (source: {ver_source})")
    if ver_nums:
        print(f"  Range: {ver_nums[0]} .. {ver_nums[-1]}")
        # Check for gaps
        expected = set(range(ver_nums[0], ver_nums[-1] + 1))
        actual = set(ver_nums)
        gaps = sorted(expected - actual)
        if gaps:
            print(f"  GAPS (missing versions): {gaps}")
        else:
            print(f"  No gaps (contiguous)")

    latest = ver_nums[-1] if ver_nums else 1

    # =========================================================================
    # Step 2: Fetch current page using ALL methods
    # =========================================================================
    print(f"\n{'='*70}")
    print("FETCHING CURRENT VERSION VIA DIFFERENT API METHODS")
    print(f"{'='*70}")

    methods = {}

    print("\n[Method 1] GET /content/{id}?expand=body.storage,version")
    r1 = fetch_method_1_current(session, api, page_id)
    b1, info1 = extract_body(r1)
    methods["M1_current_storage"] = b1
    print(f"  Status: {r1.status_code}, Version: {info1}, Body: {len(b1 or '')}b, Hash: {short_hash(b1 or '')}")

    print("\n[Method 2] GET /content/{id}?expand=body.view,version")
    r2 = fetch_method_2_current_view(session, api, page_id)
    b2, info2 = extract_body(r2, body_type="view")
    methods["M2_current_view"] = b2
    print(f"  Status: {r2.status_code}, Version: {info2}, Body: {len(b2 or '')}b, Hash: {short_hash(b2 or '')}")

    print(f"\n[Method 3] GET /content/{{id}}?expand=body.storage&status=historical&version={latest}")
    r3 = fetch_method_3_historical(session, api, page_id, latest)
    b3, info3 = extract_body(r3)
    methods[f"M3_historical_v{latest}"] = b3
    print(f"  Status: {r3.status_code}, Version: {info3}, Body: {len(b3 or '')}b, Hash: {short_hash(b3 or '')}")

    print("\n[Method 4] GET /content/{id}?expand=body.storage&status=current")
    r4 = fetch_method_4_current_status(session, api, page_id)
    b4, info4 = extract_body(r4)
    methods["M4_status_current"] = b4
    print(f"  Status: {r4.status_code}, Version: {info4}, Body: {len(b4 or '')}b, Hash: {short_hash(b4 or '')}")

    print(f"\n[Method 5] CQL id={page_id} with expand=body.storage")
    r5 = fetch_method_5_cql(session, api, page_id)
    b5, info5 = extract_body(r5)
    methods["M5_cql_search"] = b5
    print(f"  Status: {r5.status_code}, Version: {info5}, Body: {len(b5 or '')}b, Hash: {short_hash(b5 or '')}")

    # Also fetch previous version for comparison
    if latest > 1:
        prev = latest - 1
        print(f"\n[Method 6] GET historical version {prev} (previous)")
        r6 = fetch_method_3_historical(session, api, page_id, prev)
        b6, info6 = extract_body(r6)
        methods[f"M6_historical_v{prev}"] = b6
        print(f"  Status: {r6.status_code}, Version: {info6}, Body: {len(b6 or '')}b, Hash: {short_hash(b6 or '')}")

    # =========================================================================
    # Step 3: Compare all methods
    # =========================================================================
    print(f"\n{'='*70}")
    print("COMPARISON")
    print(f"{'='*70}")

    # Group by content hash
    hash_groups = {}
    for name, body in methods.items():
        if body is None:
            print(f"  {name}: FAILED (no body)")
            continue
        h = short_hash(body)
        hash_groups.setdefault(h, []).append(name)

    print("\nContent groups (same hash = identical content):")
    for h, names in hash_groups.items():
        first_body = methods[names[0]]
        print(f"  [{h}] {len(first_body)}b — {', '.join(names)}")

    if len(hash_groups) == 1:
        print("\n  ✓ ALL methods return IDENTICAL content")
    else:
        print(f"\n  ⚠ MISMATCH! {len(hash_groups)} different versions of content returned")

        # Show diff between Method 1 (current) and Method 3 (historical latest)
        m1_key = "M1_current_storage"
        m3_key = f"M3_historical_v{latest}"
        if methods.get(m1_key) and methods.get(m3_key) and methods[m1_key] != methods[m3_key]:
            print(f"\n{'='*70}")
            print(f"DIFF: {m1_key} vs {m3_key}")
            print(f"{'='*70}")
            lines1 = (methods[m1_key] or "").splitlines(keepends=True)
            lines3 = (methods[m3_key] or "").splitlines(keepends=True)
            diff = list(difflib.unified_diff(lines3, lines1,
                                              fromfile=m3_key, tofile=m1_key,
                                              n=2))
            if len(diff) > 100:
                for line in diff[:80]:
                    print(line, end="")
                print(f"\n... ({len(diff) - 80} more diff lines)")
            else:
                for line in diff:
                    print(line, end="")

    # =========================================================================
    # Step 4: Compare specific versions if requested
    # =========================================================================
    compare_vers = []
    if args.compare_versions:
        compare_vers = [int(v.strip()) for v in args.compare_versions.split(",")]
    elif len(ver_nums) >= 3:
        # Auto-compare last 3 versions
        compare_vers = ver_nums[-3:]
    elif ver_nums:
        compare_vers = ver_nums[-2:] if len(ver_nums) >= 2 else ver_nums

    if compare_vers:
        print(f"\n{'='*70}")
        print(f"VERSION-BY-VERSION COMPARISON: {compare_vers}")
        print(f"{'='*70}")

        ver_bodies = {}
        for vn in compare_vers:
            resp = fetch_method_3_historical(session, api, page_id, vn)
            body, info = extract_body(resp)
            ver_bodies[vn] = body
            status = "OK" if body else "EMPTY/FAILED"
            print(f"  v{vn}: {status}, {len(body or '')}b, hash={short_hash(body or '')}")

        # Show what changed between consecutive versions
        for i in range(1, len(compare_vers)):
            v_prev = compare_vers[i - 1]
            v_curr = compare_vers[i]
            b_prev = ver_bodies.get(v_prev, "")
            b_curr = ver_bodies.get(v_curr, "")
            if not b_prev and not b_curr:
                print(f"\n  v{v_prev} → v{v_curr}: both empty")
            elif b_prev == b_curr:
                print(f"\n  v{v_prev} → v{v_curr}: IDENTICAL content ({len(b_curr)}b)")
            else:
                lines_p = (b_prev or "").splitlines(keepends=True)
                lines_c = (b_curr or "").splitlines(keepends=True)
                diff = list(difflib.unified_diff(lines_p, lines_c,
                                                  fromfile=f"v{v_prev}", tofile=f"v{v_curr}",
                                                  n=1))
                added = sum(1 for l in diff if l.startswith('+') and not l.startswith('+++'))
                removed = sum(1 for l in diff if l.startswith('-') and not l.startswith('---'))
                print(f"\n  v{v_prev} → v{v_curr}: {len(b_prev)}b → {len(b_curr)}b (+{added}/-{removed} lines)")
                if len(diff) <= 30:
                    for line in diff:
                        print(f"    {line}", end="")
                else:
                    for line in diff[:20]:
                        print(f"    {line}", end="")
                    print(f"    ... ({len(diff) - 20} more diff lines)")

    # =========================================================================
    # Step 5: Dump raw bodies to files for manual inspection
    # =========================================================================
    print(f"\n{'='*70}")
    print("RAW HTML DUMPS (for manual inspection)")
    print(f"{'='*70}")

    dump_dir = f"/tmp/confluence_debug_{page_id}"
    import os
    os.makedirs(dump_dir, exist_ok=True)

    for name, body in methods.items():
        if body:
            fpath = os.path.join(dump_dir, f"{name}.html")
            with open(fpath, "w") as f:
                f.write(body)
            print(f"  {fpath} ({len(body)}b)")

    for vn, body in (ver_bodies.items() if compare_vers else []):
        if body:
            fpath = os.path.join(dump_dir, f"historical_v{vn}.html")
            with open(fpath, "w") as f:
                f.write(body)
            print(f"  {fpath} ({len(body)}b)")

    print(f"\n  Files saved to: {dump_dir}/")
    print(f"  Compare with: diff {dump_dir}/M1_current_storage.html {dump_dir}/M3_historical_v{latest}.html")


if __name__ == "__main__":
    main()
