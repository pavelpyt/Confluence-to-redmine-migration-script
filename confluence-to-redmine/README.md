# Confluence to Redmine Migration Toolkit

Migrate Confluence On-Prem wiki spaces to Redmine wiki pages via REST API. Handles full page hierarchy, version history, attachments, images, links, user mentions, and formatting — with built-in server health monitoring to prevent overloading legacy Confluence instances.

## Scripts

| Script | Purpose |
|--------|---------|
| `confluence_to_redmine_api.py` | **Main migration pipeline** — REST API-based migration with full history, attachments, health monitoring |
| `confluence_to_redmine.py` | XML export-based migration (alternative approach using Confluence XML exports) |
| `confluence_load_simulator.py` | Server load tester — simulates migration request patterns without writing to Redmine |
| `confluence_version_debug.py` | Debug tool — fetches a single page via all API methods and compares results |

## Quick Start

```bash
pip install -r requirements.txt

# List available Confluence spaces
python confluence_to_redmine_api.py \
  --confluence-url https://confluence.example.com \
  --confluence-user admin --confluence-pass secret \
  --redmine-url https://redmine.example.com \
  --redmine-key YOUR_API_KEY \
  --list-spaces --no-verify-ssl

# Dry run (preview what would be migrated)
python confluence_to_redmine_api.py \
  --confluence-url https://confluence.example.com \
  --confluence-user admin --confluence-pass secret \
  --redmine-url https://redmine.example.com \
  --redmine-key YOUR_API_KEY \
  --spaces MYSPACE --dry-run --no-verify-ssl

# Full migration with history
python confluence_to_redmine_api.py \
  --confluence-url https://confluence.example.com \
  --confluence-user admin --confluence-pass secret \
  --redmine-url https://redmine.example.com \
  --redmine-key YOUR_API_KEY \
  --spaces MYSPACE --with-history --no-verify-ssl
```

## Server Protection Settings

The Confluence server's JVM heap is the bottleneck. These settings prevent GC death spirals on constrained servers:

```bash
# Conservative (recommended for legacy servers with limited heap)
python confluence_to_redmine_api.py \
  --confluence-url ... --redmine-url ... --redmine-key ... \
  --spaces SPACE1,SPACE2 \
  --with-history \
  --max-versions 200 \
  --version-delay 1.0 \
  --batch-size 15 \
  --page-cooldown 60 \
  --space-cooldown 300 \
  --no-verify-ssl

# Aggressive (for servers with adequate resources)
python confluence_to_redmine_api.py \
  --confluence-url ... --redmine-url ... --redmine-key ... \
  --spaces SPACE1,SPACE2 \
  --with-history \
  --max-versions 0 \
  --version-delay 0.3 \
  --batch-size 25 \
  --concurrency 4 \
  --no-verify-ssl
```

### Parameter Reference

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--with-history` | off | Import page version history (not just current version) |
| `--max-versions` | 200 | Max old versions per page (0 = unlimited) |
| `--version-delay` | 1.0s | Delay between Confluence version fetch requests |
| `--batch-size` | 25 | Versions fetched per batch before pushing to Redmine |
| `--concurrency` | 2 | Max parallel Confluence requests for attachments |
| `--delay` | 0.1s | Delay between Redmine push requests |
| `--page-cooldown` | 0s | Pause every 50 pages within a space (recommended: 60) |
| `--space-cooldown` | 0s | Pause between spaces for server GC (recommended: 300) |
| `--create-projects` | off | Auto-create missing Redmine projects |
| `--excel-map` | - | Excel file mapping Confluence spaces to Redmine projects |
| `--format` | markdown | Redmine text format: `markdown` or `textile` |

## Health Monitoring

The migration includes a background health monitor that pings the Confluence server every 30 seconds:

- **Normal** (< 3s response): full speed
- **Warn** (3-8s): adds 5s delay between requests
- **Critical** (> 8s): pauses migration, waits up to 5 minutes for recovery
- **Active probe** between version batches detects GC pressure in real-time

```
[HEALTH] Monitor started — ping every 30s, warn>3.0s, critical>8.0s
[HEALTH] WARN: 4.2s response — server under pressure, throttling
[HEALTH] Probe: 9.1s — server under GC pressure, waiting 30s...
[HEALTH] CRITICAL: 12.3s response — server overloaded, pausing migration
[HEALTH] Waiting for server to recover (up to 300s)...
[HEALTH] Server recovered after 65s
```

## Load Simulator

Test your settings before running a real migration:

```bash
python confluence_load_simulator.py \
  --confluence-url https://confluence.example.com \
  --confluence-user admin --confluence-pass secret \
  --spaces MYSPACE \
  --max-versions 200 --batch-size 15 --delay 1.0 \
  --no-verify-ssl
```

Reports per-request response times, identifies overload points, and suggests parameter adjustments.

## Features

### Content Conversion
- **HTML to Markdown/Textile**: Full conversion of Confluence body.view HTML
- **Images**: Extracted from body.view, downloaded, uploaded as Redmine attachments
- **Links**: Internal page links, cross-project links, anchor links converted to Redmine wiki syntax
- **User mentions**: `@DisplayName` from both body.view and body.storage formats
- **Formatting**: Bold, italic, strikethrough, underline, code blocks, blockquotes
- **Lists**: Ordered/unordered with nested sub-lists, task checkboxes, `start=` attribute
- **Tables**: Preserved as HTML (Redmine renders inline HTML)
- **Emoticons**: Converted to FontAwesome icons via `{{fa()}}` macro

### Migration
- **Full page hierarchy**: Parent/child relationships preserved
- **Version history**: Old versions imported chronologically with author/timestamp
- **Attachments**: Downloaded from Confluence, uploaded to Redmine wiki pages
- **Cross-project links**: Global page ID map resolves links across spaces
- **Excel mapping**: Map Confluence spaces to Redmine projects via spreadsheet
- **Jira key resolution**: Resolve Jira project keys to Confluence space keys

### Server Protection
- **Sequential version fetching**: One request at a time with configurable delay
- **Batch streaming**: Fetch batch -> push to Redmine -> discard -> next batch (constant memory)
- **Health monitoring**: Background thread with automatic pause/resume
- **Active health probes**: Real-time GC pressure detection between batches
- **Circuit breaker**: Stops after 5 consecutive failures
- **Configurable cooldowns**: Per-page, per-space, and per-version-batch
- **Garbage collection**: Explicit `gc.collect()` after heavy pages

## Authentication

### Confluence
```bash
# Basic auth
--confluence-user USERNAME --confluence-pass PASSWORD

# Personal Access Token
--confluence-pat YOUR_TOKEN
```

### Redmine
```bash
--redmine-key YOUR_API_KEY
```

The Redmine API key needs admin permissions or at minimum: wiki edit + file upload on target projects.

## Requirements

- Python 3.8+
- `requests` (HTTP client)
- `openpyxl` (optional, for Excel mapping files)
