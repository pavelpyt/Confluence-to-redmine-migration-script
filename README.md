# Confluence to Redmine Migration Toolkit

Migrate Confluence On-Prem wiki spaces to Redmine wiki pages via REST API.

Built for real-world enterprise migrations where the Confluence server is a legacy instance with limited resources that cannot be upgraded or tuned. The toolkit handles full page hierarchies, version history, attachments, images, internal links, user mentions, and formatting conversion — while actively monitoring the Confluence server's health to prevent JVM garbage collection death spirals.

---

## The Problem

Migrating from Confluence On-Prem to Redmine sounds straightforward: read pages from one API, write to another. In practice, several challenges make it difficult:

1. **Server heap pressure** — Confluence's REST API builds response objects in JVM heap memory. Fetching hundreds of page versions in succession fills the heap faster than garbage collection can free it. On constrained servers (512MB-2GB heap), this causes GC pauses of 10+ seconds, eventually leading to a full stop-the-world GC spiral where the JVM spends 100% of CPU time trying to free memory. We observed a **269-second (4.5 minute) GC pause** that froze the entire server.

2. **Content format mismatch** — Confluence stores content as either `body.storage` (raw Confluence XML with `<ac:>` tags) or `body.view` (server-rendered HTML). Neither maps cleanly to Redmine's Markdown or Textile. Images, links, macros, mentions, and formatting all need specific conversion rules.

3. **Cross-project references** — Confluence pages link to other pages by internal page ID. These IDs are meaningless in Redmine. Links must be resolved across all spaces before migration begins.

4. **Image ownership** — Images in `body.view` HTML can reference attachments from _other_ pages (cross-page embeds). These images won't appear in the current page's attachment list and must be downloaded separately from the Confluence server.

5. **Version fidelity** — Pages with hundreds or thousands of versions need their history preserved. Each version requires a separate API call, and `body.view` for historical versions requires expensive server-side rendering.

---

## How It Works

### Architecture

```
                          ┌─────────────────┐
                          │  Health Monitor  │  (background thread, pings every 30s)
                          │  ┌───────────┐  │
                          │  │ pause /   │  │
                          │  │ throttle  │  │
                          │  └─────┬─────┘  │
                          └───────┼─────────┘
                                  │
┌──────────┐    REST API    ┌─────▼─────┐    REST API    ┌──────────┐
│Confluence├───────────────►│ Migration ├───────────────►│ Redmine  │
│  Server  │◄───────────────┤  Script   │                │  Server  │
│          │  body.storage  │           │  PUT wiki page  │          │
│          │  attachments   │  Convert  │  Upload files   │          │
│          │  images        │  HTML→MD  │                 │          │
└──────────┘                └───────────┘                └──────────┘
```

### Migration Pipeline (7 Steps)

The main script (`confluence_to_redmine_api.py`) runs through these steps:

**Step 1 — Discover spaces**
Fetches all Confluence spaces via `/rest/api/space`. Uses lightweight requests (no body content) to minimize server load.

**Step 2 — Load mapping** (optional)
If `--excel-map` is provided, loads a spreadsheet that maps Confluence space keys to Redmine project identifiers. Also resolves Jira project keys to Confluence space keys.

**Step 3 — Check Redmine projects**
Verifies that target Redmine projects exist. With `--create-projects`, auto-creates missing ones under a parent project.

**Step 4 — Build space-to-project map**
Matches each Confluence space to its Redmine project using the Excel mapping or direct key matching.

**Step 5 — Create missing projects** (if `--create-projects`)
Creates Redmine projects for unmapped spaces with wiki module enabled.

**Step 6 — Build global page ID map**
Fetches page metadata (title + ancestors only, no body content) across ALL spaces. Builds a lookup table: `{confluence_page_id: {title, project}}`. This enables cross-project link resolution during content conversion.

**Step 7 — Migrate spaces**
For each space, processes pages in parent-first order:

1. **Fetch page list** via CQL (25 pages per request, metadata only)
2. **For each page:**
   - If `--with-history`: fetch version list (metadata only, no content)
   - For each old version (in batches):
     - Fetch `body.storage` (raw XML, no server-side rendering)
     - Convert to Markdown
     - Push to Redmine as a new wiki revision
     - Wait `--version-delay` seconds between each fetch
     - Between batches: active health probe, adaptive delay
   - Fetch current version with `body.view` (rendered HTML, 1 server render)
   - Convert HTML to Markdown (handles images, links, mentions, formatting)
   - Download attachments from Confluence attachment API
   - Extract images from `body.view` HTML (cross-page references)
   - Download any missing images directly from Confluence
   - Upload all attachments to Redmine
   - Push final wiki page with attachments
3. **Clean up** temp files, run `gc.collect()`

---

## Scripts

### `confluence_to_redmine_api.py` (main)

The primary migration tool. Uses Confluence REST API directly (no XML export needed).

```bash
# List spaces
python confluence_to_redmine_api.py \
  --confluence-url https://confluence.example.com \
  --confluence-user admin --confluence-pass secret \
  --redmine-url https://redmine.example.com \
  --redmine-key YOUR_API_KEY \
  --list-spaces --no-verify-ssl

# Dry run
python confluence_to_redmine_api.py \
  --confluence-url https://confluence.example.com \
  --confluence-user admin --confluence-pass secret \
  --redmine-url https://redmine.example.com \
  --redmine-key YOUR_API_KEY \
  --spaces MYSPACE --dry-run --no-verify-ssl

# Full migration with history and server protection
python confluence_to_redmine_api.py \
  --confluence-url https://confluence.example.com \
  --confluence-user admin --confluence-pass secret \
  --redmine-url https://redmine.example.com \
  --redmine-key YOUR_API_KEY \
  --spaces SPACE1,SPACE2 \
  --with-history \
  --max-versions 200 \
  --version-delay 1.0 \
  --batch-size 15 \
  --page-cooldown 60 \
  --space-cooldown 300 \
  --no-verify-ssl
```

### `confluence_load_simulator.py`

Runs the exact same request pattern as the migration but **without writing to Redmine**. Measures every response time, identifies where the server would overload, and suggests parameter adjustments.

```bash
python confluence_load_simulator.py \
  --confluence-url https://confluence.example.com \
  --confluence-user admin --confluence-pass secret \
  --spaces MYSPACE \
  --max-versions 200 --batch-size 15 --delay 1.0 \
  --no-verify-ssl
```

Example output:
```
SIMULATION RESULTS
======================================================================
  Total requests:      847
  Total time:          1423s (23.7 min)
  Pages processed:     156
  Versions fetched:    691
  Avg response time:   0.42s
  Max response time:   3.71s
  Baseline response:   0.28s
  Final response:      0.31s
  Server degradation:  1.1x vs baseline
  WARN events (>5s):   0
  CRITICAL events (>15s): 0

  Server handled the load well — settings are safe
```

### `confluence_to_redmine.py`

Alternative migration approach that works from Confluence XML exports instead of the REST API. Use this if you can export spaces from the Confluence admin UI but can't access the API (e.g., authentication restrictions).

### `confluence_version_debug.py`

Diagnostic tool that fetches a single page using ALL possible API methods (`body.view`, `body.storage`, direct GET, CQL, historical version) and compares the results. Useful for debugging content conversion issues.

---

## Parameter Reference

### Connection

| Parameter | Required | Description |
|-----------|----------|-------------|
| `--confluence-url` | Yes | Confluence base URL (e.g., `https://confluence.example.com`) |
| `--confluence-user` | * | Confluence username (basic auth) |
| `--confluence-pass` | * | Confluence password |
| `--confluence-pat` | * | Confluence Personal Access Token (alternative to user/pass) |
| `--redmine-url` | Yes | Redmine base URL |
| `--redmine-key` | Yes | Redmine API key (needs wiki edit + file upload permissions) |
| `--no-verify-ssl` | No | Skip SSL certificate verification |

\* Provide either `--confluence-pat` or `--confluence-user`/`--confluence-pass`.

### Scope

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--spaces` | all | Comma-separated Confluence space keys or Jira keys to migrate |
| `--exclude-spaces` | none | Comma-separated space keys to skip |
| `--excel-map` | none | Excel file with Confluence-to-Redmine project mapping |
| `--with-history` | off | Import page version history (not just current version) |
| `--max-versions` | 200 | Max old versions per page (0 = unlimited) |
| `--create-projects` | off | Auto-create missing Redmine projects |
| `--format` | markdown | Redmine wiki format: `markdown` or `textile` |
| `--dry-run` | off | Preview what would be migrated without making changes |
| `--list-spaces` | off | List all Confluence spaces and exit |

### Performance and Server Protection

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--version-delay` | 1.0s | Seconds between each Confluence version fetch request. This is the most important setting for preventing server overload. Each request loads a historical page version into JVM heap. |
| `--batch-size` | 25 | Number of versions fetched per batch before pushing to Redmine and freeing memory. Lower values reduce peak server memory usage. |
| `--concurrency` | 2 | Max parallel requests for attachment downloads. Does not affect version fetching (always sequential). |
| `--delay` | 0.1s | Seconds between Redmine PUT requests. Redmine is usually not the bottleneck. |
| `--page-cooldown` | 0 | Seconds to pause every 50 pages within a space. Also pauses after any page with 50+ versions (capped at 30s). Recommended: `60` for large spaces. |
| `--space-cooldown` | 0 | Seconds to wait between finishing one space and starting the next. Lets Confluence GC reclaim heap from the previous space's API responses. Recommended: `300` (5 min). |
| `--tmp-dir` | /tmp/confluence_migration | Temp directory for downloaded attachments (cleaned up per space). |

### Recommended Profiles

**Legacy server (limited heap, cannot be tuned):**
```bash
--version-delay 1.0 --batch-size 15 --page-cooldown 60 --space-cooldown 300
```

**Normal server (adequate resources):**
```bash
--version-delay 0.5 --batch-size 25 --page-cooldown 0 --space-cooldown 60
```

**Powerful server (dedicated migration window):**
```bash
--version-delay 0.3 --batch-size 25 --concurrency 4 --page-cooldown 0 --space-cooldown 0
```

---

## Health Monitoring

### Background Monitor

A daemon thread pings `/rest/api/space?limit=1` every 30 seconds and categorizes the response time:

| Response Time | Status | Action |
|---------------|--------|--------|
| < 3s | Normal | Full speed |
| 3 - 8s | Warn | Adds 5s delay between requests |
| > 8s | Critical | Pauses migration, waits up to 5 minutes |
| No response | Down | Pauses migration, waits up to 5 minutes |

### Active Health Probes

Between each batch of version fetches, the script sends its own probe request (separate from the background monitor). This provides real-time GC pressure detection:

```
[HEALTH] Probe: 1.2s — OK, continuing with 5s pause
[HEALTH] Probe: 4.8s — server warm, waiting 10s...
[HEALTH] Probe: 11.3s — server under GC pressure, waiting 30s...
[HEALTH] Probe failed — server unresponsive, waiting 60s...
```

### Why This Matters

Confluence On-Prem runs on the JVM. Every REST API response is built as a Java object in heap memory. When the migration script fetches page versions in rapid succession:

1. Each response creates objects in heap (even `body.storage` responses)
2. JVM garbage collector runs periodically to free unused objects
3. If requests arrive faster than GC can free memory, the heap fills up
4. JVM switches to full GC (stop-the-world pause) — the entire server freezes
5. If the heap is truly full, GC runs continuously (death spiral) — observed pauses of **4+ minutes**

The health monitoring system detects steps 3-4 (via response time degradation) and automatically slows down or pauses before step 5 occurs.

---

## Content Conversion

### What Gets Converted

| Confluence Element | Redmine Output |
|---|---|
| `<strong>`, `<b>` | `**bold**` |
| `<em>`, `<i>` | `*italic*` |
| `<del>`, `<s>` | `~~strikethrough~~` |
| `<u>` | `<u>underline</u>` (HTML passthrough) |
| `<code>` | `` `inline code` `` |
| `<pre><code>` | ```` ``` code block ``` ```` |
| `<h1>` - `<h6>` | `#` - `######` headings |
| `<ol>`, `<ul>` | `1.` / `-` with nested indentation |
| `<table>` | HTML passthrough (Redmine renders tables) |
| `<blockquote>` | `> quoted text` |
| `<a href="...">` | `[label](url)` or `[[Wiki_Page\|label]]` |
| `<a href="#anchor">` | `[label](#Redmine-Anchor)` |
| `<a href="/pages/viewpage.action?pageId=123">` | `[[Wiki_Title\|label]]` or `{{wiki(project, page, label)}}` |
| `<a class="confluence-userlink">` | `@Display Name` |
| `<ac:link><ri:user>` | `@Display Name` |
| `<img src="/download/attachments/...">` | `![filename](filename)` |
| `<ac:image>` | `![filename](filename)` |
| `<ac:structured-macro name="code">` | ```` ``` code ``` ```` with language |
| `<ac:structured-macro name="info/warning/note/tip">` | `> **Type:** content` |
| Confluence emoticons | `{{fa(icon-name, color=...)}}` (FontAwesome) |
| `{{child_pages}}` | Appended to pages with children |

### Link Resolution

Links are resolved using a global page ID map built before migration:

- **Same-project links**: `[[Wiki_Title|Display Text]]`
- **Cross-project links**: `{{wiki(project_id, Wiki_Title, Display Text)}}`
- **Anchor links**: Converted to Redmine's anchor format (`#Section-Name`)
- **External links**: Standard markdown `[text](url)`
- **Unresolvable Confluence links**: Converted to plain text (label only)

### Image Handling

Images go through a multi-step process:

1. Page attachments are fetched via `/content/{id}/child/attachment` API
2. Each attachment is downloaded and uploaded to Redmine
3. `body.view` HTML is parsed for `<img src="/download/attachments/...">` URLs
4. Any images referenced in HTML but missing from the attachment list (cross-page embeds) are downloaded directly from the Confluence URL
5. All images are uploaded as wiki page attachments in Redmine
6. Image references in the converted Markdown use `![filename](filename)` syntax

---

## Authentication

### Confluence

```bash
# Basic authentication
--confluence-user USERNAME --confluence-pass PASSWORD

# Personal Access Token (recommended)
--confluence-pat YOUR_TOKEN
```

If `--confluence-pass` is omitted, the script prompts interactively.

### Redmine

```bash
--redmine-key YOUR_API_KEY
```

The API key needs permissions for:
- Wiki page create/edit on target projects
- File upload (for attachments)
- Project creation (if using `--create-projects`)

Find your API key in Redmine under **My Account > API access key**.

---

## Excel Mapping

The `--excel-map` option loads a spreadsheet to map Confluence spaces to Redmine projects. Expected columns:

| Column | Purpose |
|--------|---------|
| Confluence Space Key | The space key in Confluence (e.g., `VFOMAN`) |
| Redmine Project Identifier | Target project ID in Redmine (e.g., `vfoman`) |
| Jira Project Key | (Optional) Jira key that resolves to the Confluence space |

This allows migrating spaces into differently-named Redmine projects and resolving Jira project keys passed via `--spaces`.

---

## Troubleshooting

### "Garbage collection exceeded time limit" in Confluence logs

This is the JVM telling you the server is running out of heap memory. Increase `--version-delay` and decrease `--batch-size`:

```bash
--version-delay 2.0 --batch-size 10
```

### `[HEALTH] CRITICAL` during migration

The Confluence server response time exceeded 8 seconds. The migration will automatically pause and wait for recovery. If this happens frequently, your settings are too aggressive for the server.

### Images show as broken icons in Redmine

Check that:
1. The image files were actually uploaded (look for `[OK] page_name, N att` in output)
2. The filename in the Markdown matches the uploaded attachment name
3. Redmine's wiki formatting is set to **Markdown** (Admin > Settings > General)

### DNS resolution errors for Redmine

```
NameResolutionError: Failed to resolve 'redmine.example.com'
```

Transient network issue. The script retries Redmine calls 3 times with backoff. If persistent, check VPN/DNS configuration.

### Pages with thousands of versions

Pages with 500+ versions are warned about in the output. Use `--max-versions 200` to cap the imported history. The most recent N versions are imported; older versions are skipped.

### Empty pages after migration

Some Confluence pages contain only macros (Jira issue lists, dynamic content) that have no static representation. These pages appear empty because the macro content cannot be migrated. The script will show `[WARN] Could not fetch body for PAGE_NAME`.

---

## Requirements

- Python 3.8+
- `requests` >= 2.28.0
- `openpyxl` >= 3.0.0 (optional, only for `--excel-map`)

```bash
pip install -r requirements.txt
```

---

## License

Internal tool. Not licensed for external distribution.
