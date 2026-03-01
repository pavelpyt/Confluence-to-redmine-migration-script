#!/usr/bin/env python3
"""
Confluence XML Export → Redmine Wiki Importer (Pilot)

Parses a Confluence space XML export (entities.xml), extracts pages with
parent/child hierarchy, converts Confluence storage XHTML to Redmine-compatible
textile/markdown, and imports into a Redmine project wiki via REST API.

Features:
  - Parses Page objects with parent/child (originalVersion) relationships
  - Uses ONLY the latest version of each page (highest version number)
  - Converts Confluence XHTML storage format to Redmine textile
  - Uploads attachments (images, files) and links them in wiki pages
  - Creates wiki page hierarchy (parent pages)
  - Idempotent: can be re-run safely (PUT = create or update)

Usage:
    # 1) Extract your Confluence XML export zip
    unzip confluence-export.zip -d ./confluence-export

    # 2) Dry run
    python confluence_to_redmine.py \
        --export-dir ./confluence-export \
        --redmine-url https://redmine.example.com \
        --api-key YOUR_KEY \
        --project vfomanrm \
        --dry-run

    # 3) Real import
    python confluence_to_redmine.py \
        --export-dir ./confluence-export \
        --redmine-url https://redmine.example.com \
        --api-key YOUR_KEY \
        --project vfomanrm
"""

import argparse
import json
import os
import re
import sys
import time
import unicodedata
from pathlib import Path
from xml.etree import ElementTree as ET

import requests

# Optional: better HTML→textile conversion
try:
    import markdownify
    HAS_MARKDOWNIFY = True
except ImportError:
    HAS_MARKDOWNIFY = False


# =============================================================================
# 1) PARSING entities.xml
# =============================================================================

def parse_entities_xml(export_dir: str) -> dict:
    """
    Parse entities.xml from a Confluence XML export using streaming iterparse.
    Handles large files (100MB+) with low memory usage.

    Returns dict with:
        pages: {page_id: {title, parent_id, version, body_content_ids, ...}}
        body_contents: {bc_id: {body, content_id, body_type}}
        attachments: {att_id: {filename, content_id (page), version, original_version_id}}
    """
    entities_path = os.path.join(export_dir, "entities.xml")
    if not os.path.exists(entities_path):
        print(f"[ERROR] entities.xml not found in {export_dir}")
        sys.exit(1)

    file_size_mb = os.path.getsize(entities_path) / (1024 * 1024)
    print(f"[INFO] Parsing {entities_path} ({file_size_mb:.0f} MB) ...")
    print(f"[INFO] This may take a minute for large files...")

    pages = {}
    body_contents = {}
    attachments = {}

    obj_count = 0
    start_time = time.time()

    # Use iterparse to stream through XML without loading it all into memory
    context = ET.iterparse(entities_path, events=("end",))

    for event, elem in context:
        if elem.tag != "object":
            continue

        obj_count += 1
        if obj_count % 10000 == 0:
            elapsed = time.time() - start_time
            print(f"[INFO]   Processed {obj_count} objects ({elapsed:.1f}s) | "
                  f"Pages: {len(pages)}, Bodies: {len(body_contents)}, Attachments: {len(attachments)}")

        obj_class = elem.get("class", "")

        if obj_class == "Page":
            page = _parse_page_object(elem)
            if page:
                pages[page["id"]] = page

        elif obj_class == "BodyContent":
            bc = _parse_body_content(elem)
            if bc:
                body_contents[bc["id"]] = bc

        elif obj_class == "Attachment":
            att = _parse_attachment(elem)
            if att:
                attachments[att["id"]] = att

        # Free memory — clear the element after processing
        elem.clear()

    elapsed = time.time() - start_time
    print(f"[INFO] Parsed {obj_count} total objects in {elapsed:.1f}s")
    print(f"[INFO]   Pages: {len(pages)}, BodyContents: {len(body_contents)}, Attachments: {len(attachments)}")
    return {
        "pages": pages,
        "body_contents": body_contents,
        "attachments": attachments,
    }


def _get_id(obj) -> str:
    """Extract the <id name="id">VALUE</id> from an object."""
    id_elem = obj.find("id")
    if id_elem is not None:
        return id_elem.text.strip() if id_elem.text else None
    return None


def _get_property(obj, name: str) -> str:
    """Extract text value of <property name="NAME">VALUE</property>."""
    for prop in obj.findall("property"):
        if prop.get("name") == name:
            return prop.text.strip() if prop.text else ""
    return ""


def _get_property_ref_id(obj, name: str) -> str:
    """Extract referenced ID from <property name="NAME" class="..."><id>VAL</id></property>."""
    for prop in obj.findall("property"):
        if prop.get("name") == name:
            id_elem = prop.find("id")
            if id_elem is not None:
                return id_elem.text.strip() if id_elem.text else None
    return None


def _get_collection_ids(obj, name: str) -> list:
    """Extract IDs from <collection name="NAME"><element><id>VAL</id></element>...</collection>."""
    ids = []
    for coll in obj.findall("collection"):
        if coll.get("name") == name:
            for elem in coll.findall("element"):
                id_elem = elem.find("id")
                if id_elem is not None and id_elem.text:
                    ids.append(id_elem.text.strip())
    return ids


def _parse_page_object(obj) -> dict:
    """Parse a Page object from entities.xml."""
    page_id = _get_id(obj)
    if not page_id:
        return None

    title = _get_property(obj, "title")
    version = _get_property(obj, "version")
    status = _get_property(obj, "contentStatus")

    # Parent page reference (may or may not exist directly on Page)
    parent_id = _get_property_ref_id(obj, "parent")

    # Original version (for versioned pages — points to the "root" page)
    original_version_id = _get_property_ref_id(obj, "originalVersion")

    # Body content IDs (may be in collection or linked via BodyContent.content ref)
    body_content_ids = _get_collection_ids(obj, "bodyContents")

    # Position / ordering
    position = _get_property(obj, "position")

    # Space reference
    space_id = _get_property_ref_id(obj, "space")

    # Date and author info (for version history)
    created = _get_property(obj, "creationDate")
    modified = _get_property(obj, "lastModificationDate")
    creator = _get_property(obj, "creatorName")
    modifier = _get_property(obj, "lastModifierName")

    return {
        "id": page_id,
        "title": title,
        "version": int(version) if version else 0,
        "status": status,
        "parent_id": parent_id,
        "original_version_id": original_version_id,
        "body_content_ids": body_content_ids,
        "position": int(position) if position else 0,
        "space_id": space_id,
        "created": created,
        "modified": modified,
        "creator": creator,
        "modifier": modifier,
    }


def _parse_body_content(obj) -> dict:
    """Parse a BodyContent object from entities.xml."""
    bc_id = _get_id(obj)
    if not bc_id:
        return None

    body = _get_property(obj, "body")
    content_id = _get_property_ref_id(obj, "content")
    body_type = _get_property(obj, "bodyType")

    # Fix Confluence CDATA escaping: ]] > → ]]>
    if body:
        body = body.replace("]] >", "]]>")

    return {
        "id": bc_id,
        "body": body,
        "content_id": content_id,
        "body_type": body_type,
    }


def _parse_attachment(obj) -> dict:
    """Parse an Attachment object from entities.xml."""
    att_id = _get_id(obj)
    if not att_id:
        return None

    filename = _get_property(obj, "fileName") or _get_property(obj, "title")
    # Confluence uses 'containerContent' (not 'content') to reference the parent page
    content_id = _get_property_ref_id(obj, "containerContent") or _get_property_ref_id(obj, "content")
    version = _get_property(obj, "version")
    original_version_id = _get_property(obj, "originalVersionId")
    content_type = _get_property(obj, "contentType")
    content_status = _get_property(obj, "contentStatus")
    file_size = _get_property(obj, "fileSize")

    return {
        "id": att_id,
        "filename": filename,
        "content_id": content_id,  # page ID this attachment belongs to
        "version": int(version) if version else 1,
        "original_version_id": original_version_id if original_version_id else None,
        "content_type": content_type,
        "content_status": content_status,
        "file_size": file_size,
    }


# =============================================================================
# 2) BUILD PAGE TREE
# =============================================================================

def _get_body_for_page(pid, pages, body_by_page, body_contents):
    """Get body HTML for a page ID."""
    page_bodies = list(body_by_page.get(pid, []))
    page_obj = pages.get(pid, {})
    for bc_id in page_obj.get("body_content_ids", []):
        if bc_id in body_contents:
            page_bodies.append(body_contents[bc_id])
    # Deduplicate
    seen = set()
    unique = []
    for bc in page_bodies:
        if bc["id"] not in seen:
            seen.add(bc["id"])
            unique.append(bc)
    # Prefer bodyType 2 (XHTML)
    for bc in unique:
        if bc.get("body_type") == "2" and bc.get("body"):
            return bc["body"]
    for bc in unique:
        if bc.get("body"):
            return bc["body"]
    return ""


def build_page_tree(parsed: dict, with_history: bool = False) -> list:
    """
    Build a flat list of pages to import.
    
    If with_history=False: only latest version of each page.
    If with_history=True: includes version history per page (oldest→newest).

    Returns list of dicts with 'versions' key containing ordered version list.
    """
    pages = parsed["pages"]
    body_contents = parsed["body_contents"]
    attachments = parsed["attachments"]

    # Build reverse indexes
    body_by_page = {}
    for bc in body_contents.values():
        page_id = bc.get("content_id")
        if page_id:
            body_by_page.setdefault(page_id, []).append(bc)

    att_by_page = {}
    for att in attachments.values():
        page_id = att.get("content_id")
        if page_id:
            att_by_page.setdefault(page_id, []).append(att)

    # Separate current pages from old versions
    current_pages = {}
    old_versions = {}  # original_version_id -> [page objects]
    old_version_count = 0

    for pid, page in pages.items():
        if page["original_version_id"]:
            old_version_count += 1
            old_versions.setdefault(page["original_version_id"], []).append(page)
        else:
            current_pages[pid] = page

    # Build result
    result = []
    total_versions = 0

    for pid, page in current_pages.items():
        if page["status"] in ("draft", "deleted"):
            continue
        if not page["title"]:
            continue

        body = _get_body_for_page(pid, pages, body_by_page, body_contents)

        # Get attachments
        page_attachments = []
        for att in att_by_page.get(pid, []):
            if att.get("content_status") != "deleted":
                page_attachments.append(att)

        entry = {
            "id": pid,
            "title": page["title"],
            "parent_id": page["parent_id"],
            "version": page["version"],
            "body_html": body,
            "attachments": page_attachments,
            "position": page["position"],
            "modified": page.get("modified", ""),
            "modifier": page.get("modifier", ""),
            "created": page.get("created", ""),
            "creator": page.get("creator", ""),
        }

        # Build version history if requested
        if with_history:
            version_list = []
            
            # Collect old versions for this page
            page_old_versions = old_versions.get(pid, [])
            
            for old_page in page_old_versions:
                old_body = _get_body_for_page(
                    old_page["id"], pages, body_by_page, body_contents
                )
                version_list.append({
                    "version": old_page["version"],
                    "body_html": old_body,
                    "modified": old_page.get("modified", ""),
                    "modifier": old_page.get("modifier", ""),
                    "title": old_page["title"],
                })

            # Sort old versions by version number (ascending = oldest first)
            version_list.sort(key=lambda v: v["version"])

            # Add current version at the end
            version_list.append({
                "version": page["version"],
                "body_html": body,
                "modified": page.get("modified", ""),
                "modifier": page.get("modifier", ""),
                "title": page["title"],
            })

            entry["versions"] = version_list
            total_versions += len(version_list)
        else:
            entry["versions"] = []

        result.append(entry)

    result.sort(key=lambda p: (p["parent_id"] or "", p["position"]))

    print(f"[INFO] Built page tree: {len(result)} current pages")
    if with_history:
        print(f"[INFO]   Total versions to import: {total_versions}")
        avg = total_versions / len(result) if result else 0
        est_minutes = total_versions * 0.15 / 60  # 0.1s delay + overhead
        print(f"[INFO]   Average versions per page: {avg:.1f}")
        print(f"[INFO]   Estimated import time: ~{est_minutes:.0f} minutes")
    else:
        print(f"[INFO]   Skipped {old_version_count} old versions")
    print(f"[INFO]   Total attachments mapped: {sum(len(p['attachments']) for p in result)}")
    return result


# =============================================================================
# 3) CONVERT Confluence XHTML → Redmine Textile
# =============================================================================

def convert_to_markdown(html_body: str, page_title_map: dict = None) -> str:
    """
    Convert Confluence storage XHTML to Redmine-compatible GFM Markdown.
    """
    if not html_body:
        return ""

    text = html_body

    def _make_safe_filename(filename):
        """Sanitize filename to match what we upload."""
        name, ext = os.path.splitext(filename)
        safe_name = re.sub(r'[^\w\-.]', '_', name)
        safe_name = re.sub(r'_+', '_', safe_name).strip('_')
        return f"{safe_name}{ext}" if safe_name else f"attachment{ext}"

    # =========================================================================
    # PHASE 1: Handle Confluence-specific macros BEFORE stripping HTML
    # =========================================================================

    # Excerpt/include macros
    def replace_include(match):
        full = match.group(0)
        title_match = re.search(r'ri:content-title="([^"]*)"', full)
        if title_match:
            t = title_match.group(1)
            return f"\n> *Included from:* [[{sanitize_wiki_title(t)}|{t}]]\n"
        return ""
    text = re.sub(
        r'<ac:structured-macro[^>]*ac:name="(?:excerpt-include|include)"[^>]*>.*?</ac:structured-macro>',
        replace_include, text, flags=re.DOTALL,
    )

    # Code/noformat macros → code blocks
    def replace_code_macro(match):
        full = match.group(0)
        lang_match = re.search(r'<ac:parameter ac:name="language">([^<]*)</ac:parameter>', full)
        lang = lang_match.group(1) if lang_match else ""
        body_match = re.search(r'<ac:plain-text-body>\s*<!\[CDATA\[(.*?)\]\]>\s*</ac:plain-text-body>', full, re.DOTALL)
        if not body_match:
            body_match = re.search(r'<ac:plain-text-body>(.*?)</ac:plain-text-body>', full, re.DOTALL)
        body = body_match.group(1) if body_match else ""
        return f"\n```{lang}\n{body}\n```\n"
    text = re.sub(
        r'<ac:structured-macro[^>]*ac:name="(?:code|noformat)"[^>]*>.*?</ac:structured-macro>',
        replace_code_macro, text, flags=re.DOTALL,
    )

    # View-file macro
    def replace_view_file(match):
        fn_match = re.search(r'ri:filename="([^"]*)"', match.group(0))
        if fn_match:
            f = fn_match.group(1)
            return f"\n[📎 {f}]({_make_safe_filename(f)})\n"
        return ""
    text = re.sub(
        r'<ac:structured-macro[^>]*ac:name="view-file"[^>]*>.*?</ac:structured-macro>',
        replace_view_file, text, flags=re.DOTALL,
    )

    # Multimedia macro
    def replace_multimedia(match):
        fn_match = re.search(r'ri:filename="([^"]*)"', match.group(0))
        if fn_match:
            f = fn_match.group(1)
            return f"\n[🎬 {f}]({_make_safe_filename(f)})\n"
        return ""
    text = re.sub(
        r'<ac:structured-macro[^>]*ac:name="multimedia"[^>]*>.*?</ac:structured-macro>',
        replace_multimedia, text, flags=re.DOTALL,
    )

    # Swagger/OpenAPI macro → link or code block
    def replace_swagger(match):
        full = match.group(0)
        url_match = re.search(r'<ac:parameter ac:name="(?:url|specUrl)">([^<]*)</ac:parameter>', full)
        if url_match:
            url = url_match.group(1)
            return f"\n> 📋 **API Documentation (Swagger/OpenAPI)**\n> URL: [{url}]({url})\n"
        return "\n> 📋 **API Documentation (Swagger/OpenAPI)** — *embedded content not migrated*\n"
    text = re.sub(
        r'<ac:structured-macro[^>]*ac:name="(?:swagger|open-api|openapi)"[^>]*>.*?</ac:structured-macro>',
        replace_swagger, text, flags=re.DOTALL,
    )

    # Expand macro (collapsible section) → details block or header
    def replace_expand(match):
        full = match.group(0)
        title_match = re.search(r'<ac:parameter ac:name="title">([^<]*)</ac:parameter>', full)
        title = title_match.group(1) if title_match else "Details"
        body_match = re.search(r'<ac:rich-text-body>(.*?)</ac:rich-text-body>', full, re.DOTALL)
        body = body_match.group(1) if body_match else ""
        # Keep the body HTML for later processing, just add a header
        return f"\n**▸ {title}**\n{body}\n"
    text = re.sub(
        r'<ac:structured-macro[^>]*ac:name="expand"[^>]*>.*?</ac:structured-macro>',
        replace_expand, text, flags=re.DOTALL,
    )

    # Anchor macro → anchor placeholder
    text = re.sub(
        r'<ac:structured-macro[^>]*ac:name="anchor"[^>]*>.*?</ac:structured-macro>',
        '', text, flags=re.DOTALL,
    )

    # Children display macro → note
    text = re.sub(
        r'<ac:structured-macro[^>]*ac:name="children"[^>]*>.*?</ac:structured-macro>',
        '\n*See child pages in the sidebar.*\n', text, flags=re.DOTALL,
    )

    # Recently updated macro → note
    text = re.sub(
        r'<ac:structured-macro[^>]*ac:name="recently-updated"[^>]*>.*?</ac:structured-macro>',
        '\n*Recently updated pages — see sidebar for navigation.*\n', text, flags=re.DOTALL,
    )

    # Contributors/profile macros → note
    text = re.sub(
        r'<ac:structured-macro[^>]*ac:name="(?:contributors|profile|contributors-summary)"[^>]*>.*?</ac:structured-macro>',
        '\n*Contributors information — not migrated.*\n', text, flags=re.DOTALL,
    )

    # Jira issue macro → link
    def replace_jira(match):
        full = match.group(0)
        key_match = re.search(r'<ac:parameter ac:name="key">([^<]*)</ac:parameter>', full)
        if key_match:
            key = key_match.group(1)
            return f"`{key}`"
        server_match = re.search(r'<ac:parameter ac:name="jqlQuery">([^<]*)</ac:parameter>', full)
        if server_match:
            return f"\n*Jira query: `{server_match.group(1)}`*\n"
        return ""
    text = re.sub(
        r'<ac:structured-macro[^>]*ac:name="jira"[^>]*>.*?</ac:structured-macro>',
        replace_jira, text, flags=re.DOTALL,
    )

    # Info/note/warning/tip panels → blockquotes
    def replace_panel(match):
        macro_name = match.group(1)
        body = match.group(0)
        # Extract body content
        body_match = re.search(r'<ac:rich-text-body>(.*?)</ac:rich-text-body>', body, re.DOTALL)
        content = body_match.group(1) if body_match else ""
        # Strip inner HTML lightly
        content = re.sub(r'<[^>]+>', ' ', content).strip()
        icons = {"info": "ℹ️", "note": "📝", "warning": "⚠️", "tip": "💡", "panel": "📋"}
        icon = icons.get(macro_name, "📋")
        lines = content.split('\n')
        quoted = '\n'.join(f"> {line}" for line in lines)
        return f"\n> {icon} **{macro_name.capitalize()}**\n{quoted}\n"
    text = re.sub(
        r'<ac:structured-macro[^>]*ac:name="(info|note|warning|tip|panel)"[^>]*>.*?</ac:structured-macro>',
        replace_panel, text, flags=re.DOTALL,
    )

    # TOC macro → placeholder
    text = re.sub(
        r'<ac:structured-macro[^>]*ac:name="toc"[^>]*>.*?</ac:structured-macro>',
        '\n{{>toc}}\n', text, flags=re.DOTALL,
    )
    # Also self-closing toc
    text = re.sub(
        r'<ac:structured-macro[^>]*ac:name="toc"[^>]*/\s*>',
        '\n{{>toc}}\n', text, flags=re.DOTALL,
    )

    # Remaining structured macros → comment
    text = re.sub(
        r'<ac:structured-macro[^>]*ac:name="([^"]*)"[^>]*>.*?</ac:structured-macro>',
        r'<!-- Confluence macro: \1 -->', text, flags=re.DOTALL,
    )

    # =========================================================================
    # PHASE 2: Handle Confluence images and attachment links
    # =========================================================================

    # Images: <ac:image><ri:attachment ri:filename="X"/></ac:image>
    def replace_image(match):
        full = match.group(0)
        fn_match = re.search(r'ri:filename="([^"]*)"', full)
        if not fn_match:
            return ""
        filename = fn_match.group(1)
        safe = _make_safe_filename(filename)
        # Extract width/height if present
        w_match = re.search(r'ac:width="(\d+)"', full)
        return f"\n![{filename}]({safe})\n"
    text = re.sub(
        r'<ac:image[^>]*>.*?</ac:image>',
        replace_image, text, flags=re.DOTALL,
    )

    # Attachment links: <ac:link><ri:attachment ri:filename="X"/>body</ac:link>
    def replace_att_link(match):
        full = match.group(0)
        fn_match = re.search(r'ri:filename="([^"]*)"', full)
        if not fn_match:
            return ""
        filename = fn_match.group(1)
        safe = _make_safe_filename(filename)
        # Get display text
        body_match = re.search(r'<ac:plain-text-link-body>\s*<!\[CDATA\[(.*?)\]\]>\s*</ac:plain-text-link-body>', full, re.DOTALL)
        if not body_match:
            body_match = re.search(r'<ac:link-body>(.*?)</ac:link-body>', full, re.DOTALL)
        display = ""
        if body_match:
            display = re.sub(r'<[^>]+>', '', body_match.group(1)).strip()
        if not display:
            display = filename
        return f" [{display}]({safe}) "

    text = re.sub(
        r'<ac:link[^>]*>.*?<ri:attachment[^/]*/?>.*?</ac:link>',
        replace_att_link, text, flags=re.DOTALL,
    )

    # Wiki page links: <ac:link><ri:page ri:content-title="X"/>body</ac:link>
    def replace_wiki_link(match):
        full = match.group(0)
        title_match = re.search(r'ri:content-title="([^"]*)"', full)
        if not title_match:
            return ""
        title = title_match.group(1)
        wiki_title = sanitize_wiki_title(title)
        # Get display text
        body_match = re.search(r'<ac:plain-text-link-body>\s*<!\[CDATA\[(.*?)\]\]>\s*</ac:plain-text-link-body>', full, re.DOTALL)
        if not body_match:
            body_match = re.search(r'<ac:link-body>(.*?)</ac:link-body>', full, re.DOTALL)
        display = ""
        if body_match:
            display = re.sub(r'<[^>]+>', '', body_match.group(1)).strip()
        if not display:
            display = title
        return f"[[{wiki_title}|{display}]]"

    text = re.sub(
        r'<ac:link[^>]*>.*?<ri:page[^/]*/?>.*?</ac:link>',
        replace_wiki_link, text, flags=re.DOTALL,
    )

    # Handle remaining ac:link tags (URL links, space links, etc.)
    def replace_remaining_link(match):
        full = match.group(0)
        # Try to extract any URL
        url_match = re.search(r'<ri:url ri:value="([^"]*)"', full)
        if url_match:
            url = url_match.group(1)
            body_match = re.search(r'<ac:plain-text-link-body>\s*<!\[CDATA\[(.*?)\]\]>', full, re.DOTALL)
            display = body_match.group(1).strip() if body_match else url
            return f"[{display}]({url})"
        # Extract any display text
        body_match = re.search(r'<ac:link-body>(.*?)</ac:link-body>', full, re.DOTALL)
        if body_match:
            return re.sub(r'<[^>]+>', '', body_match.group(1)).strip()
        return ""
    text = re.sub(r'<ac:link[^>]*>.*?</ac:link>', replace_remaining_link, text, flags=re.DOTALL)

    # Emoticons
    text = re.sub(r'<ac:emoticon ac:name="tick"\s*/?>', '✅', text)
    text = re.sub(r'<ac:emoticon ac:name="cross"\s*/?>', '❌', text)
    text = re.sub(r'<ac:emoticon ac:name="warning"\s*/?>', '⚠️', text)
    text = re.sub(r'<ac:emoticon ac:name="information"\s*/?>', 'ℹ️', text)
    text = re.sub(r'<ac:emoticon ac:name="([^"]*)"\s*/?>', r'[\1]', text)

    # Strip remaining ac:* and ri:* tags
    text = re.sub(r'</?ac:[^>]*/?>', '', text)
    text = re.sub(r'</?ri:[^>]*/?>', '', text)

    # =========================================================================
    # PHASE 3: Convert HTML to Markdown
    # =========================================================================

    # --- Tables (BEFORE other HTML processing) ---
    def convert_table(match):
        table_html = match.group(0)
        rows = re.findall(r'<tr[^>]*>(.*?)</tr>', table_html, re.DOTALL)
        if not rows:
            return ""

        md_rows = []
        max_cols = 0

        for i, row in enumerate(rows):
            # Extract cells — handle nested HTML properly
            cells = []
            for cell_match in re.finditer(r'<t([hd])([^>]*)>(.*?)</t\1>', row, re.DOTALL):
                cell_content = cell_match.group(3)
                # Process inner HTML: strip tags but keep text, handle line breaks
                cell_content = re.sub(r'<br\s*/?>', ' ', cell_content)
                cell_content = re.sub(r'<p[^>]*>', ' ', cell_content)
                cell_content = re.sub(r'</p>', ' ', cell_content)
                # Keep bold/italic markers
                cell_content = re.sub(r'<strong[^>]*>(.*?)</strong>', r'**\1**', cell_content, flags=re.DOTALL)
                cell_content = re.sub(r'<b[^>]*>(.*?)</b>', r'**\1**', cell_content, flags=re.DOTALL)
                cell_content = re.sub(r'<em[^>]*>(.*?)</em>', r'*\1*', cell_content, flags=re.DOTALL)
                # Strip remaining HTML
                cell_content = re.sub(r'<[^>]+>', '', cell_content)
                # Clean up whitespace — collapse to single spaces, trim
                cell_content = re.sub(r'\s+', ' ', cell_content).strip()
                # Escape pipe chars inside cells
                cell_content = cell_content.replace('|', '\\|')
                cells.append(cell_content)

            if not cells:
                continue
            max_cols = max(max_cols, len(cells))
            md_rows.append(cells)

        if not md_rows:
            return ""

        # Normalize column count
        for row in md_rows:
            while len(row) < max_cols:
                row.append("")

        # Build markdown table
        lines = []
        for i, row in enumerate(md_rows):
            lines.append("| " + " | ".join(row) + " |")
            if i == 0:
                lines.append("| " + " | ".join(["---"] * max_cols) + " |")

        return "\n\n" + "\n".join(lines) + "\n\n"

    text = re.sub(r'<table[^>]*>.*?</table>', convert_table, text, flags=re.DOTALL)

    # --- Colors (Confluence uses style="color: ...") ---
    # GFM doesn't support colors natively, but we can preserve the text
    # and add a note about the color
    def handle_colored_span(match):
        color = match.group(1)
        content = match.group(2)
        # Just return the content — can't do colors in standard markdown
        return content
    text = re.sub(r'<span[^>]*style="[^"]*color:\s*([^;"]+)[^"]*"[^>]*>(.*?)</span>', handle_colored_span, text, flags=re.DOTALL)

    # --- Headers ---
    text = re.sub(r'<h1[^>]*>(.*?)</h1>', r'\n# \1\n', text, flags=re.DOTALL)
    text = re.sub(r'<h2[^>]*>(.*?)</h2>', r'\n## \1\n', text, flags=re.DOTALL)
    text = re.sub(r'<h3[^>]*>(.*?)</h3>', r'\n### \1\n', text, flags=re.DOTALL)
    text = re.sub(r'<h4[^>]*>(.*?)</h4>', r'\n#### \1\n', text, flags=re.DOTALL)
    text = re.sub(r'<h5[^>]*>(.*?)</h5>', r'\n##### \1\n', text, flags=re.DOTALL)
    text = re.sub(r'<h6[^>]*>(.*?)</h6>', r'\n###### \1\n', text, flags=re.DOTALL)

    # --- Bold, italic, strikethrough ---
    text = re.sub(r'<strong[^>]*>(.*?)</strong>', r'**\1**', text, flags=re.DOTALL)
    text = re.sub(r'<b[^>]*>(.*?)</b>', r'**\1**', text, flags=re.DOTALL)
    text = re.sub(r'<em[^>]*>(.*?)</em>', r'*\1*', text, flags=re.DOTALL)
    text = re.sub(r'<i[^>]*>(.*?)</i>', r'*\1*', text, flags=re.DOTALL)
    text = re.sub(r'<del[^>]*>(.*?)</del>', r'~~\1~~', text, flags=re.DOTALL)
    text = re.sub(r'<s[^>]*>(.*?)</s>', r'~~\1~~', text, flags=re.DOTALL)
    text = re.sub(r'<u[^>]*>(.*?)</u>', r'\1', text, flags=re.DOTALL)

    # --- Code blocks ---
    text = re.sub(r'<pre[^>]*><code[^>]*>(.*?)</code></pre>', r'\n```\n\1\n```\n', text, flags=re.DOTALL)
    text = re.sub(r'<pre[^>]*>(.*?)</pre>', r'\n```\n\1\n```\n', text, flags=re.DOTALL)
    text = re.sub(r'<code[^>]*>(.*?)</code>', r'`\1`', text, flags=re.DOTALL)

    # --- Links ---
    text = re.sub(r'<a[^>]*href="([^"]*)"[^>]*>(.*?)</a>', r'[\2](\1)', text, flags=re.DOTALL)

    # --- Images (standard HTML) ---
    text = re.sub(r'<img[^>]*src="([^"]*)"[^>]*alt="([^"]*)"[^>]*/?>',r'![\2](\1)', text, flags=re.DOTALL)
    text = re.sub(r'<img[^>]*src="([^"]*)"[^>]*/?>',r'![](\1)', text, flags=re.DOTALL)

    # --- Lists ---
    # Handle nested lists properly
    def convert_list(match):
        list_html = match.group(0)
        list_type = "ol" if match.group(0).startswith("<ol") else "ul"
        items = re.findall(r'<li[^>]*>(.*?)</li>', list_html, re.DOTALL)
        result = "\n"
        for idx, item in enumerate(items):
            # Strip inner HTML but keep text
            item_text = re.sub(r'<[^>]+>', '', item).strip()
            item_text = re.sub(r'\s+', ' ', item_text)
            if list_type == "ol":
                result += f"{idx+1}. {item_text}\n"
            else:
                result += f"- {item_text}\n"
        return result + "\n"

    text = re.sub(r'<ul[^>]*>.*?</ul>', convert_list, text, flags=re.DOTALL)
    text = re.sub(r'<ol[^>]*>.*?</ol>', convert_list, text, flags=re.DOTALL)

    # --- Paragraphs & line breaks ---
    text = re.sub(r'<p[^>]*>', '\n', text)
    text = re.sub(r'</p>', '\n', text)
    text = re.sub(r'<br\s*/?>', '\n', text)
    text = re.sub(r'<hr\s*/?>', '\n---\n', text)

    # --- Strip remaining HTML tags ---
    text = re.sub(r'<[^>]+>', '', text)

    # =========================================================================
    # PHASE 4: Clean up
    # =========================================================================

    # Decode HTML entities
    import html as html_module
    text = html_module.unescape(text)

    # Fix # at start of line that's not a header (Redmine interprets as issue ref)
    # Only escape # that's followed by a digit (issue reference pattern)
    text = re.sub(r'^(#{1,6}\s)', r'\1', text, flags=re.MULTILINE)  # Keep real headers
    # Escape standalone # + number that's NOT a header
    lines = text.split('\n')
    fixed_lines = []
    for line in lines:
        stripped = line.lstrip()
        # If line starts with # but is NOT a markdown header (# followed by space + text)
        if re.match(r'^#\d', stripped):
            # This is a Redmine issue reference pattern — escape it
            line = line.replace('#', '\\#', 1)
        fixed_lines.append(line)
    text = '\n'.join(fixed_lines)

    # Clean up excessive whitespace
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = re.sub(r'[ \t]+\n', '\n', text)  # trailing whitespace
    text = text.strip()

    return text


def sanitize_wiki_title(title: str) -> str:
    """
    Convert a page title to a valid Redmine wiki page title.
    Redmine wiki uses underscores for spaces, strips special chars.
    Wiki titles CANNOT start with a digit — prefix with 'Page_' if they do.
    """
    # Normalize unicode (strip diacritics)
    normalized = unicodedata.normalize("NFKD", title)
    stripped = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    # Replace spaces with underscores
    result = re.sub(r'[^\w\s-]', '', stripped)
    result = re.sub(r'[\s]+', '_', result.strip())
    if not result:
        return "Untitled"
    # Redmine wiki titles cannot start with a digit
    if result[0].isdigit():
        result = f"Page_{result}"
    return result[:255]


# =============================================================================
# 4) IMPORT TO REDMINE
# =============================================================================


def _sanitize_filename(filename: str) -> str:
    """Sanitize attachment filename for Redmine upload."""
    # Keep the extension
    name, ext = os.path.splitext(filename)
    # Replace problematic chars with underscores
    name = re.sub(r'[^\w\-.]', '_', name)
    # Collapse multiple underscores
    name = re.sub(r'_+', '_', name).strip('_')
    return f"{name}{ext}" if name else f"attachment{ext}"


def upload_attachment(req_headers: dict, base_url: str, filepath: str, filename: str = None) -> str:
    """Upload a file to Redmine and return the upload token."""
    upload_headers = dict(req_headers)
    upload_headers["Content-Type"] = "application/octet-stream"
    if filename:
        # URL-encode the filename for the header
        from urllib.parse import quote
        upload_headers["Content-Disposition"] = f'attachment; filename="{quote(filename)}"'
    with open(filepath, "rb") as f:
        resp = requests.post(
            f"{base_url}/uploads.json",
            headers=upload_headers,
            data=f,
        )
    if resp.status_code in (200, 201):
        return resp.json()["upload"]["token"]
    else:
        raise Exception(f"Upload failed: {resp.status_code} {resp.text[:100]}")


def create_wiki_page(
    req_headers: dict,
    base_url: str,
    project_id: str,
    title: str,
    text: str,
    parent_title: str = None,
    uploads: list = None,
    comments: str = None,
) -> bool:
    """Create or update a Redmine wiki page via REST API (JSON)."""
    wiki_title = sanitize_wiki_title(title)
    wiki_url = f"{base_url}/projects/{project_id}/wiki/{wiki_title}.json"

    payload = {"wiki_page": {"text": text}}
    if parent_title:
        payload["wiki_page"]["parent_title"] = parent_title
    if uploads:
        payload["wiki_page"]["uploads"] = uploads
    if comments:
        payload["wiki_page"]["comments"] = comments

    resp = requests.put(wiki_url, headers=req_headers, json=payload)

    if resp.status_code in (200, 201, 204):
        return True
    elif resp.status_code == 422:
        text_preview = text[:100].replace("\n", "\\n") if text else "<EMPTY>"
        print(f"    [DEBUG 422] wiki/{wiki_title}: text_len={len(text)}, preview='{text_preview}'")
        print(f"    [DEBUG 422] Response: {resp.text[:300]}")
        return False
    else:
        print(f"    [ERROR] PUT wiki/{wiki_title}: {resp.status_code} - {resp.text[:200]}")
        return False


def import_to_redmine(
    pages: list,
    export_dir: str,
    base_url: str,
    api_key: str = None,
    auth: tuple = None,
    session_cookies: dict = None,
    csrf_token: str = None,
    project_id: str = "",
    dry_run: bool = False,
    delay: float = 0.5,
    with_history: bool = False,
):
    """Import parsed pages into Redmine wiki."""
    req_headers = {}
    if api_key:
        req_headers["X-Redmine-API-Key"] = api_key

    # Quick auth test
    if not dry_run:
        test_resp = requests.get(f"{base_url}/users/current.json", headers=req_headers)
        if test_resp.status_code == 200:
            user_info = test_resp.json().get("user", {})
            print(f"[INFO] Authenticated as: {user_info.get('login', 'unknown')} ({user_info.get('firstname', '')} {user_info.get('lastname', '')})")
        else:
            print(f"[WARN] Auth test returned {test_resp.status_code} — requests may fail")

    id_to_title = {p["id"]: p["title"] for p in pages}

    # BFS ordering: parents before children, siblings by position
    pages_by_id = {p["id"]: p for p in pages}
    children_by_parent = {}
    roots = []
    for p in pages:
        parent_id = p.get("parent_id")
        if parent_id and parent_id in pages_by_id:
            children_by_parent.setdefault(parent_id, []).append(p)
        else:
            roots.append(p)
    roots.sort(key=lambda p: p.get("position", 0))
    for pid in children_by_parent:
        children_by_parent[pid].sort(key=lambda p: p.get("position", 0))

    ordered = []
    queue = list(roots)
    while queue:
        page = queue.pop(0)
        ordered.append(page)
        queue.extend(children_by_parent.get(page["id"], []))

    # Count total work
    total_versions = 0
    if with_history:
        for p in ordered:
            total_versions += max(len(p.get("versions", [])), 1)
    
    print(f"\n{'='*60}")
    print(f"Importing {len(ordered)} pages to {base_url}/projects/{project_id}/wiki")
    if with_history:
        print(f"  With version history: {total_versions} total versions")
        est_min = total_versions * 0.15 / 60
        print(f"  Estimated time: ~{est_min:.0f} minutes")
    print(f"{'='*60}")

    created = 0
    version_count = 0
    errors = 0
    created_wiki_titles = set()

    for page_idx, page in enumerate(ordered):
        title = page["title"]
        wiki_title = sanitize_wiki_title(title)

        # Parent reference
        parent_title = None
        if page["parent_id"] and page["parent_id"] in id_to_title:
            parent_raw = id_to_title[page["parent_id"]]
            parent_sanitized = sanitize_wiki_title(parent_raw)
            if parent_sanitized in created_wiki_titles:
                parent_title = parent_sanitized

        # Build child page listing
        child_pages_of_this = [
            p for p in pages if p.get("parent_id") == page["id"]
        ]
        child_list_md = ""
        if child_pages_of_this:
            child_pages_of_this.sort(key=lambda p: p.get("position", 0))
            child_list_md = "\n\n---\n\n## Child pages\n\n"
            for cp in child_pages_of_this:
                child_list_md += f"- [[{sanitize_wiki_title(cp['title'])}|{cp['title']}]]\n"

        def _build_full_text(body_html, page_title):
            """Convert body and build full page text."""
            md = convert_to_markdown(body_html, id_to_title)
            if not md:
                md = "*Empty page migrated from Confluence*"
            md += child_list_md
            md = md.replace("\x00", "")
            md = md.encode("utf-8", errors="replace").decode("utf-8")
            return f"# {page_title}\n\n{md}"

        # === VERSION HISTORY IMPORT ===
        if with_history and page.get("versions") and len(page["versions"]) > 1:
            versions = page["versions"]

            if dry_run:
                print(f"  [DRY-RUN] {wiki_title} ({len(versions)} versions, {len(page['attachments'])} attachments)")
                created += 1
                version_count += len(versions)
                continue

            # Import each version oldest→newest
            for vi, ver in enumerate(versions):
                is_last = (vi == len(versions) - 1)
                ver_text = _build_full_text(ver["body_html"], ver.get("title", title))

                # Version comment with metadata
                date_str = ver.get("modified", "")[:19] if ver.get("modified") else ""
                author = ver.get("modifier", "unknown")
                comment = f"v{ver['version']} by {author}"
                if date_str:
                    comment += f" ({date_str})"

                # Only set parent and upload attachments on the LAST version
                ok = create_wiki_page(
                    req_headers, base_url, project_id,
                    title, ver_text,
                    parent_title=parent_title if is_last else None,
                    uploads=None,  # attachments only on last version (below)
                    comments=comment,
                )
                if ok:
                    version_count += 1
                else:
                    errors += 1
                    break  # Stop version chain on error
                time.sleep(delay)

            # Upload attachments on the final version
            if ok:
                upload_tokens = _upload_page_attachments(
                    req_headers, base_url, export_dir, page
                )
                if upload_tokens:
                    # Re-PUT the last version with attachments
                    final_text = _build_full_text(page["body_html"], title)
                    create_wiki_page(
                        req_headers, base_url, project_id,
                        title, final_text,
                        parent_title=parent_title,
                        uploads=upload_tokens,
                        comments="Final version with attachments",
                    )
                    time.sleep(delay)

                print(f"  [OK] {wiki_title} ({len(versions)} versions, {len(upload_tokens)} attachments)")
                created += 1
                created_wiki_titles.add(wiki_title)

        # === SINGLE VERSION IMPORT (no history) ===
        else:
            full_text = _build_full_text(page["body_html"], title)

            if dry_run:
                att_count = len(page["attachments"])
                parent_info = f" (parent: {parent_title})" if parent_title else ""
                print(f"  [DRY-RUN] {wiki_title}{parent_info} | body: {len(full_text)} chars | attachments: {att_count}")
                created += 1
                continue

            upload_tokens = _upload_page_attachments(
                req_headers, base_url, export_dir, page
            )

            try:
                comment = None
                if page.get("modified") and page.get("modifier"):
                    comment = f"Migrated from Confluence — last edit by {page['modifier']} ({page['modified'][:19]})"
                ok = create_wiki_page(
                    req_headers, base_url, project_id,
                    title, full_text,
                    parent_title=parent_title,
                    uploads=upload_tokens if upload_tokens else None,
                    comments=comment,
                )
                if ok:
                    print(f"  [OK] {wiki_title} ({len(upload_tokens)} attachments)")
                    created += 1
                    created_wiki_titles.add(wiki_title)
                else:
                    errors += 1
                time.sleep(delay)
            except Exception as ex:
                print(f"  [ERROR] {wiki_title}: {ex}")
                errors += 1

        # Progress update every 50 pages
        if (page_idx + 1) % 50 == 0:
            print(f"  --- Progress: {page_idx+1}/{len(ordered)} pages, {version_count} versions imported ---")

    print(f"\n{'='*60}")
    summary = f"SUMMARY: {created} pages"
    if with_history:
        summary += f", {version_count} versions imported"
    summary += f", {errors} errors, {len(ordered)} total"
    print(summary)
    print(f"{'='*60}")


def _upload_page_attachments(req_headers, base_url, export_dir, page):
    """Upload attachments for a page and return upload tokens."""
    upload_tokens = []
    for att in page["attachments"]:
        att_path = _find_attachment_file(export_dir, page["id"], att["id"], att.get("version", 1))
        if att_path and os.path.exists(att_path):
            try:
                safe_filename = _sanitize_filename(att["filename"])
                token = upload_attachment(req_headers, base_url, att_path, safe_filename)
                upload_tokens.append({
                    "token": token,
                    "filename": safe_filename,
                    "content_type": att.get("content_type", "application/octet-stream"),
                })
                time.sleep(0.2)
            except Exception as ex:
                print(f"    [WARN] Failed to upload attachment '{att['filename']}': {ex}")
        else:
            print(f"    [WARN] Attachment file not found: page={page['id']}, att={att['id']}, file={att['filename']}")
    return upload_tokens


def _find_attachment_file(export_dir: str, page_id: str, att_id: str, version: int = 1) -> str:
    """
    Find attachment file in the Confluence export directory.
    Confluence stores attachments as: attachments/<page_id>/<att_id>/<version>
    
    Also tries: attachments/<att_id>/<version> (some exports use this)
    """
    att_base = os.path.join(export_dir, "attachments")
    
    # Standard path: attachments/<page_id>/<att_id>/<version>
    path = os.path.join(att_base, str(page_id), str(att_id), str(version))
    if os.path.exists(path):
        return path

    # Try version 1 as fallback
    if version != 1:
        path_v1 = os.path.join(att_base, str(page_id), str(att_id), "1")
        if os.path.exists(path_v1):
            return path_v1

    # Try without version: attachments/<page_id>/<att_id>
    path2 = os.path.join(att_base, str(page_id), str(att_id))
    if os.path.isfile(path2):
        return path2

    # Try: attachments/<att_id>/<version>
    path3 = os.path.join(att_base, str(att_id), str(version))
    if os.path.exists(path3):
        return path3

    # Try: look inside attachments/<page_id>/<att_id>/ for any file
    dir_path = os.path.join(att_base, str(page_id), str(att_id))
    if os.path.isdir(dir_path):
        files = os.listdir(dir_path)
        if files:
            # Return the highest-numbered version file
            try:
                versions = sorted([int(f) for f in files if f.isdigit()], reverse=True)
                if versions:
                    return os.path.join(dir_path, str(versions[0]))
            except ValueError:
                pass
            # Just return the first file
            return os.path.join(dir_path, files[0])

    return None


# =============================================================================
# 5) MANIFEST EXPORT (optional — for debugging)
# =============================================================================

def export_manifest(pages: list, output_path: str):
    """Export parsed page tree as JSON manifest for review."""
    manifest = []
    for p in pages:
        manifest.append({
            "id": p["id"],
            "title": p["title"],
            "parent_id": p["parent_id"],
            "version": p["version"],
            "body_length": len(p["body_html"]) if p["body_html"] else 0,
            "attachment_count": len(p["attachments"]),
            "attachments": [{"id": a["id"], "filename": a["filename"]} for a in p["attachments"]],
        })

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)
    print(f"[INFO] Manifest saved to {output_path}")


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Import Confluence XML export into Redmine wiki",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--export-dir", required=True, help="Path to extracted Confluence XML export directory")
    parser.add_argument("--redmine-url", required=True, help="Redmine base URL")
    parser.add_argument("--api-key", help="Redmine API key")
    parser.add_argument("--username", help="Redmine username (alternative to API key)")
    parser.add_argument("--password", help="Redmine password (alternative to API key)")
    parser.add_argument("--session-file", help="Session cookie file from redmine_browser_auth.py (for SAML/SSO)")
    parser.add_argument("--project", required=True, help="Redmine project identifier")
    parser.add_argument("--dry-run", action="store_true", help="Parse and preview without importing")
    parser.add_argument("--with-history", action="store_true", help="Import all page versions (not just latest)")
    parser.add_argument("--manifest", help="Export JSON manifest to this path (optional)")
    parser.add_argument("--delay", type=float, default=0.5, help="Delay between API calls (default: 0.5s)")
    args = parser.parse_args()

    # Auto-reduce delay for history imports
    delay = args.delay
    if args.with_history and delay >= 0.5:
        delay = 0.1
        print(f"[INFO] History mode: delay reduced to {delay}s")

    # 1) Parse
    parsed = parse_entities_xml(args.export_dir)

    # 2) Build page tree
    pages = build_page_tree(parsed, with_history=args.with_history)

    if not pages:
        print("[WARN] No pages found to import.")
        sys.exit(0)

    # 3) Optional manifest
    if args.manifest:
        export_manifest(pages, args.manifest)

    # Determine auth method
    auth = None
    api_key = args.api_key
    session_cookies = None
    csrf_token = None

    if args.session_file:
        with open(args.session_file, "r") as f:
            session_data = json.load(f)
        session_cookies = session_data.get("cookies", {})
        csrf_token = session_data.get("csrf_token")
        print(f"[INFO] Using browser session auth ({len(session_cookies)} cookies loaded)")
        if csrf_token:
            print(f"[INFO] CSRF token loaded")
    elif args.username:
        password = args.password
        if not password:
            import getpass
            password = getpass.getpass(f"Password for {args.username}: ")
        auth = (args.username, password)
        print(f"[INFO] Using basic auth as '{args.username}'")
    elif api_key:
        print(f"[INFO] Using API key auth")
    else:
        print("[ERROR] Provide --api-key, --username, or --session-file for authentication.")
        sys.exit(1)

    # 4) Import
    import_to_redmine(
        pages=pages,
        export_dir=args.export_dir,
        base_url=args.redmine_url.rstrip("/"),
        api_key=api_key,
        auth=auth,
        session_cookies=session_cookies,
        csrf_token=csrf_token,
        project_id=args.project,
        dry_run=args.dry_run,
        delay=delay,
        with_history=args.with_history,
    )


if __name__ == "__main__":
    main()