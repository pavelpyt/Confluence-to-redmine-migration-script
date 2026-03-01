#!/usr/bin/env python3
"""
Confluence Migration Load Simulator

Simulates the exact request pattern that confluence_to_redmine_api.py generates
against the Confluence server, WITHOUT actually migrating anything.

Measures response times per request and identifies when the server would
become overloaded. Use this to tune --concurrency, --batch-size, --delay,
--page-cooldown, and --space-cooldown before running a real migration.
"""

import argparse
import json
import sys
import time

import requests


def simulate_migration(base_url, session, api_base, spaces, max_versions=200,
                       batch_size=25, delay=0.3, page_cooldown=0, space_cooldown=0):
    """Simulate the migration request pattern and measure load."""

    WARN_THRESHOLD = 5.0    # seconds
    CRITICAL_THRESHOLD = 15.0

    stats = {
        "total_requests": 0,
        "total_time": 0.0,
        "max_response": 0.0,
        "warnings": 0,
        "criticals": 0,
        "errors": 0,
        "pages_processed": 0,
        "versions_fetched": 0,
        "timeline": [],  # (timestamp, response_time, request_type)
    }

    def timed_get(url, params=None, label="", timeout=20):
        """Make a GET and record timing."""
        t0 = time.time()
        try:
            resp = session.get(url, params=params, timeout=timeout)
            elapsed = time.time() - t0
            stats["total_requests"] += 1
            stats["total_time"] += elapsed
            stats["max_response"] = max(stats["max_response"], elapsed)
            stats["timeline"].append((time.time(), elapsed, label))

            status = "OK"
            if elapsed > CRITICAL_THRESHOLD:
                stats["criticals"] += 1
                status = "CRITICAL"
                print(f"  !! CRITICAL: {elapsed:.1f}s — {label}")
            elif elapsed > WARN_THRESHOLD:
                stats["warnings"] += 1
                status = "WARN"
                print(f"  !  WARN: {elapsed:.1f}s — {label}")

            if resp.status_code != 200:
                stats["errors"] += 1
                print(f"  x  HTTP {resp.status_code} — {label}")

            return resp, elapsed, status
        except Exception as e:
            elapsed = time.time() - t0
            stats["total_requests"] += 1
            stats["errors"] += 1
            stats["timeline"].append((time.time(), elapsed, f"ERROR:{label}"))
            print(f"  x  ERROR ({type(e).__name__}) — {label}")
            return None, elapsed, "ERROR"

    # Health check baseline
    print("=" * 70)
    print("BASELINE HEALTH CHECK")
    print("=" * 70)
    _, base_rt, _ = timed_get(f"{api_base}/space", {"limit": 1}, "baseline health")
    print(f"  Baseline response time: {base_rt:.2f}s\n")

    start_time = time.time()

    for space_idx, space_key in enumerate(spaces):
        print(f"\n{'='*70}")
        print(f"SPACE: {space_key} ({space_idx+1}/{len(spaces)})")
        print(f"{'='*70}")

        # Cooldown between spaces
        if space_idx > 0 and space_cooldown > 0:
            print(f"  [COOLDOWN] {space_cooldown}s between spaces...")
            time.sleep(space_cooldown)

        # Step 1: Fetch pages (simulates get_space_pages with CQL)
        pages = []
        page_start = 0
        while True:
            resp, rt, status = timed_get(
                f"{api_base}/content/search",
                {"cql": f'space="{space_key}" AND type=page', "start": page_start,
                 "limit": 25, "expand": "version,ancestors"},
                f"page_list [{space_key}] start={page_start}"
            )
            if not resp or resp.status_code != 200:
                break
            data = resp.json()
            results = data.get("results", [])
            pages.extend(results)
            if len(results) < 25 or len(pages) >= data.get("totalSize", 0):
                break
            page_start += 25
            time.sleep(0.1)

        print(f"  Found {len(pages)} pages")
        pages_in_space = 0

        for pidx, page in enumerate(pages):
            page_id = page["id"]
            title = page.get("title", f"page_{page_id}")[:50]
            stats["pages_processed"] += 1
            pages_in_space += 1

            # Health check between pages
            if pidx > 0 and pidx % 10 == 0:
                _, hrt, hstatus = timed_get(
                    f"{api_base}/space", {"limit": 1},
                    f"health_check after {pidx} pages"
                )
                if hstatus == "CRITICAL":
                    print(f"  !! Server overloaded at page {pidx}/{len(pages)} — would pause in real migration")

            # Page cooldown
            if page_cooldown > 0 and pages_in_space % 50 == 0 and pages_in_space > 0:
                print(f"  [PAGE COOLDOWN] {page_cooldown}s after {pages_in_space} pages...")
                time.sleep(page_cooldown)

            # Step 2: Get version count (simulates get_page_versions)
            resp, rt, _ = timed_get(
                f"{api_base}/content/{page_id}/version",
                {"start": 0, "limit": 1},
                f"version_count [{title}]"
            )
            if not resp or resp.status_code != 200:
                continue

            ver_data = resp.json()
            total_versions = ver_data.get("size", 0)
            # Estimate total from response
            if total_versions == 0:
                total_versions = 1

            # Only simulate version fetching for pages with history
            old_versions = min(max(total_versions - 1, 0), max_versions)
            if old_versions <= 0:
                time.sleep(delay)
                continue

            print(f"\n  [{title}] {total_versions} versions, fetching {old_versions}...")

            # Step 3: Simulate fetching version bodies in batches
            versions_fetched = 0
            for batch_start in range(0, old_versions, batch_size):
                batch_count = min(batch_size, old_versions - batch_start)

                # Simulate sequential version fetches with delay
                for v in range(batch_count):
                    ver_num = batch_start + v + 1
                    resp, rt, status = timed_get(
                        f"{api_base}/content/{page_id}",
                        {"expand": "body.storage,version", "status": "historical",
                         "version": ver_num},
                        f"v{ver_num}/{total_versions} [{title}]"
                    )
                    versions_fetched += 1
                    stats["versions_fetched"] += 1

                    if status == "CRITICAL":
                        print(f"  !! Server hit CRITICAL at version {ver_num} of [{title}]")
                        print(f"  !! After {versions_fetched} version fetches, {stats['total_requests']} total requests")

                    time.sleep(delay)

                # Between-batch pause
                if batch_start + batch_size < old_versions:
                    print(f"    batch {batch_start + batch_count}/{old_versions} done "
                          f"(avg {stats['total_time']/stats['total_requests']:.2f}s/req)", flush=True)
                    time.sleep(3.0)

            # Extra cooldown after heavy pages
            if old_versions > 50 and page_cooldown > 0:
                cool = min(page_cooldown, 30)
                print(f"    [COOLDOWN] heavy page ({old_versions} vers) — waiting {cool}s...")
                time.sleep(cool)

            print(f"    [{title}] done: {versions_fetched} fetched")

    # Final health check
    print(f"\n{'='*70}")
    print("FINAL HEALTH CHECK")
    print("=" * 70)
    _, final_rt, _ = timed_get(f"{api_base}/space", {"limit": 1}, "final health")

    # Summary
    total_elapsed = time.time() - start_time
    avg_rt = stats["total_time"] / stats["total_requests"] if stats["total_requests"] > 0 else 0

    print(f"\n{'='*70}")
    print("SIMULATION RESULTS")
    print(f"{'='*70}")
    print(f"  Total requests:      {stats['total_requests']}")
    print(f"  Total time:          {total_elapsed:.0f}s ({total_elapsed/60:.1f} min)")
    print(f"  Pages processed:     {stats['pages_processed']}")
    print(f"  Versions fetched:    {stats['versions_fetched']}")
    print(f"  Avg response time:   {avg_rt:.2f}s")
    print(f"  Max response time:   {stats['max_response']:.2f}s")
    print(f"  Baseline response:   {base_rt:.2f}s")
    print(f"  Final response:      {final_rt:.2f}s")
    print(f"  Server degradation:  {final_rt/base_rt:.1f}x vs baseline")
    print(f"  WARN events (>{WARN_THRESHOLD}s):    {stats['warnings']}")
    print(f"  CRITICAL events (>{CRITICAL_THRESHOLD}s): {stats['criticals']}")
    print(f"  HTTP errors:         {stats['errors']}")

    if stats["criticals"] > 0:
        print(f"\n  !! SERVER WOULD OVERLOAD with these settings")
        print(f"  !! Suggestions:")
        print(f"       --batch-size {max(batch_size // 2, 5)}")
        print(f"       --delay {delay * 2}")
        print(f"       --page-cooldown {max(page_cooldown, 60)}")
    elif stats["warnings"] > 0:
        print(f"\n  ! Server showed pressure — current settings are borderline")
        print(f"  ! Consider --page-cooldown {max(page_cooldown, 30)} for safety")
    else:
        print(f"\n  Server handled the load well — settings are safe")

    # Find the danger zone in timeline
    if stats["timeline"]:
        # Find sequences of slow responses
        slow_streak = 0
        max_streak = 0
        streak_start_label = ""
        for ts, rt, label in stats["timeline"]:
            if rt > WARN_THRESHOLD:
                slow_streak += 1
                if slow_streak > max_streak:
                    max_streak = slow_streak
                    streak_start_label = label
            else:
                slow_streak = 0
        if max_streak > 0:
            print(f"\n  Longest slow streak: {max_streak} consecutive slow responses")
            print(f"  Started at: {streak_start_label}")


def main():
    parser = argparse.ArgumentParser(
        description="Simulate Confluence migration load without actually migrating",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Simulate with conservative settings
  python3 confluence_load_simulator.py \\
    --confluence-url https://confluence.example.com \\
    --confluence-user admin --confluence-pass secret \\
    --spaces MYSPACE --max-versions 50

  # Simulate multiple spaces with cooldowns
  python3 confluence_load_simulator.py \\
    --confluence-url https://confluence.example.com \\
    --confluence-pat TOKEN \\
    --spaces SPACE1,SPACE2 --space-cooldown 300 --page-cooldown 60
        """)
    parser.add_argument("--confluence-url", required=True)
    parser.add_argument("--confluence-user")
    parser.add_argument("--confluence-pass")
    parser.add_argument("--confluence-pat")
    parser.add_argument("--no-verify-ssl", action="store_true")
    parser.add_argument("--spaces", required=True, help="Comma-separated space keys to simulate")
    parser.add_argument("--max-versions", type=int, default=200)
    parser.add_argument("--batch-size", type=int, default=25)
    parser.add_argument("--delay", type=float, default=0.3,
                        help="Delay between individual requests (default: 0.3s)")
    parser.add_argument("--page-cooldown", type=int, default=0)
    parser.add_argument("--space-cooldown", type=int, default=0)
    args = parser.parse_args()

    # Set up session
    session = requests.Session()
    session.verify = not args.no_verify_ssl
    if not session.verify:
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    if args.confluence_pat:
        session.headers["Authorization"] = f"Bearer {args.confluence_pat}"
    elif args.confluence_user and args.confluence_pass:
        import base64
        creds = base64.b64encode(f"{args.confluence_user}:{args.confluence_pass}".encode()).decode()
        session.headers["Authorization"] = f"Basic {creds}"
    else:
        print("[ERROR] Provide --confluence-pat or --confluence-user/--confluence-pass")
        sys.exit(1)

    base_url = args.confluence_url.rstrip("/")
    api_base = f"{base_url}/rest/api"

    spaces = [s.strip() for s in args.spaces.split(",")]

    print(f"Confluence Load Simulator")
    print(f"  URL: {base_url}")
    print(f"  Spaces: {spaces}")
    print(f"  Max versions: {args.max_versions}")
    print(f"  Batch size: {args.batch_size}")
    print(f"  Request delay: {args.delay}s")
    print(f"  Page cooldown: {args.page_cooldown}s")
    print(f"  Space cooldown: {args.space_cooldown}s")
    print()

    simulate_migration(
        base_url, session, api_base, spaces,
        max_versions=args.max_versions,
        batch_size=args.batch_size,
        delay=args.delay,
        page_cooldown=args.page_cooldown,
        space_cooldown=args.space_cooldown,
    )


if __name__ == "__main__":
    main()
