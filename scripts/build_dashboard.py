#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Build a static dashboard HTML from a YAML file and HTML template.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Tuple
import argparse
import csv
from datetime import datetime
import shutil


BASE_DIR = Path(__file__).resolve().parent.parent
DASH_DIR = BASE_DIR / "dashboards"
SITE_DIR = BASE_DIR / "docs"


def _strip_quotes(value: str) -> str:
    if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
        return value[1:-1]
    return value


def _parse_scalar(value: str) -> Any:
    value = value.strip()
    value = _strip_quotes(value)
    return value


def _parse_block(lines: List[str], start: int, indent: int) -> Tuple[Any, int]:
    obj: Any = None
    i = start
    while i < len(lines):
        raw = lines[i]
        if not raw.strip() or raw.lstrip().startswith("#"):
            i += 1
            continue
        curr_indent = len(raw) - len(raw.lstrip(" "))
        if curr_indent < indent:
            break
        if curr_indent > indent:
            # Unexpected indent; treat as part of previous block.
            break
        line = raw[indent:]
        if line.startswith("- "):
            if obj is None:
                obj = []
            item = line[2:].strip()
            if item == "":
                val, i = _parse_block(lines, i + 1, indent + 2)
                obj.append(val)
                continue
            if item.startswith(("'", '"')):
                obj.append(_parse_scalar(item))
                i += 1
                continue
            if ":" in item:
                key, rest = item.split(":", 1)
                rest = rest.strip()
                item_obj: Dict[str, Any] = {}
                if rest == "|":
                    val, i = _parse_block_scalar(lines, i + 1, indent + 2)
                    item_obj[key.strip()] = val
                elif rest == "":
                    val, i = _parse_block(lines, i + 1, indent + 2)
                    item_obj[key.strip()] = val
                else:
                    item_obj[key.strip()] = _parse_scalar(rest)
                    i += 1
                # Parse additional mapping lines for this list item
                if i < len(lines):
                    nxt = lines[i]
                    nxt_indent = len(nxt) - len(nxt.lstrip(" "))
                    if nxt_indent == indent + 2 and not nxt.lstrip().startswith("- "):
                        extra, i = _parse_map(lines, i, indent + 2)
                        item_obj.update(extra)
                obj.append(item_obj)
            else:
                obj.append(_parse_scalar(item))
                i += 1
        else:
            if obj is None:
                obj = {}
            if ":" not in line:
                i += 1
                continue
            key, rest = line.split(":", 1)
            key = key.strip()
            rest = rest.strip()
            if rest == "|":
                val, i = _parse_block_scalar(lines, i + 1, indent + 2)
                obj[key] = val
            elif rest == "":
                val, i = _parse_block(lines, i + 1, indent + 2)
                obj[key] = val
            else:
                obj[key] = _parse_scalar(rest)
                i += 1
    return obj, i


def _parse_map(lines: List[str], start: int, indent: int) -> Tuple[Dict[str, Any], int]:
    data: Dict[str, Any] = {}
    i = start
    while i < len(lines):
        raw = lines[i]
        if not raw.strip() or raw.lstrip().startswith("#"):
            i += 1
            continue
        curr_indent = len(raw) - len(raw.lstrip(" "))
        if curr_indent < indent:
            break
        if curr_indent > indent:
            break
        line = raw[indent:]
        if line.startswith("- "):
            break
        if ":" not in line:
            i += 1
            continue
        key, rest = line.split(":", 1)
        key = key.strip()
        rest = rest.strip()
        if rest == "|":
            val, i = _parse_block_scalar(lines, i + 1, indent + 2)
            data[key] = val
        elif rest == "":
            val, i = _parse_block(lines, i + 1, indent + 2)
            data[key] = val
        else:
            data[key] = _parse_scalar(rest)
            i += 1
    return data, i


def _parse_block_scalar(lines: List[str], start: int, indent: int) -> Tuple[str, int]:
    buf: List[str] = []
    i = start
    while i < len(lines):
        raw = lines[i]
        curr_indent = len(raw) - len(raw.lstrip(" "))
        if curr_indent < indent:
            break
        buf.append(raw[indent:])
        i += 1
    return "\n".join(buf).rstrip(), i


def parse_yaml(path: Path) -> Dict[str, Any]:
    lines = path.read_text(encoding="utf-8").splitlines()
    data, _ = _parse_block(lines, 0, 0)
    if not isinstance(data, dict):
        raise ValueError("YAML root must be a mapping")
    return data


def _format_people(items: List[Any]) -> str:
    parts: List[str] = []
    for item in items:
        if isinstance(item, dict):
            name = (item.get("name") or "").strip()
            url = (item.get("url") or "").strip()
            if url:
                parts.append(f'<a href="{url}" target="_blank" rel="noopener">{name}</a>')
            else:
                parts.append(name)
        else:
            parts.append(str(item))
    return ", ".join([p for p in parts if p])


def render_header(cfg: Dict[str, Any]) -> str:
    logos = cfg.get("logos", [])
    logo_links = cfg.get("logo_links", [])
    logos_html_parts = []
    for i, p in enumerate(logos):
        link = logo_links[i] if i < len(logo_links) else ""
        img = f'<img src="{p}" alt="logo">'
        if link:
            logos_html_parts.append(f'<a href="{link}" target="_blank" rel="noopener">{img}</a>')
        else:
            logos_html_parts.append(img)
    logos_html = "".join(logos_html_parts)
    authors = cfg.get("authors", [])
    author_text = _format_people(authors)
    author_prefix = cfg.get("authors_prefix", "")
    if author_text and author_prefix:
        author_text = f"{author_prefix}{author_text}"
    contributors = cfg.get("contributors", [])
    contributor_text = _format_people(contributors)
    contact = cfg.get("contact", "")
    contact_html = f"<div class=\"authors\">Contact: {contact}</div>" if contact else ""
    other_dashboard_text = cfg.get("other_dashboard_text", "")
    other_dashboard_url = cfg.get("other_dashboard_url", "")
    other_dashboard_html = ""
    if other_dashboard_text and other_dashboard_url:
        other_dashboard_html = (
            "<div class=\"authors other-dashboard\">"
            "<strong>"
            + other_dashboard_text.replace(
                "[link]", f'<a href="{other_dashboard_url}" target="_blank" rel="noopener">'
            ).replace("[/link]", "</a>")
            + "</strong></div>"
        )
    title = cfg.get("title", "")
    title_html = title.replace(" | ", "<br>")
    contributor_html = f"<div class=\"authors\">Contributors: {contributor_text}</div>" if contributor_text else ""
    return (
        "<header>"
        "<div>"
        f"<h1 class=\"title\">{title_html}</h1>"
        f"<p class=\"description\">{cfg.get('description','')}</p>"
        f"<div class=\"authors\">{author_text}</div>"
        f"{contributor_html}"
        f"{contact_html}"
        f"{other_dashboard_html}"
        "</div>"
        f"<div class=\"logo-bar\">{logos_html}</div>"
        "</header>"
    )


def parse_date(value: str) -> datetime:
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return datetime.min


def resolve_csv_path(period_dir: Path, csv_path: str) -> Path:
    if Path(csv_path).is_absolute():
        return Path(csv_path)
    candidate = (period_dir / csv_path).resolve()
    if candidate.exists():
        return candidate
    return (BASE_DIR / csv_path.lstrip("./")).resolve()


def render_summary(cfg: Dict[str, Any], period_dir: Path) -> str:
    summary = cfg.get("summary", {})
    heading = summary.get("heading", "")
    csv_path = summary.get("csv", "")
    if csv_path:
        csv_file = resolve_csv_path(period_dir, csv_path)
        rows = []
        with csv_file.open(newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                date = (row.get("Date") or "").strip()
                location = (row.get("Location") or "").strip()
                findings = (row.get("Impact (reported)") or "").strip()
                source = (row.get("Source (publisher)") or "").strip()
                url = (row.get("URL") or "").strip()
                rows.append((parse_date(date), date, location, findings, source, url))
        rows.sort(key=lambda r: r[0], reverse=True)
        items = []
        for _, date, location, findings, source, url in rows:
            label = f"{date} - {location}: {findings}"
            if url:
                label += f' <a href="{url}" target="_blank" rel="noopener">[{source}]</a>'
            else:
                label += f" [{source}]"
            items.append(f"<li>{label}</li>")
        li = "".join(items)
        return f"<section class=\"summary\"><h2>{heading}</h2><ul>{li}</ul></section>"

    bullets = summary.get("bullets", [])
    li = "".join([f"<li>{b}</li>" for b in bullets])
    sources = summary.get("sources", [])
    src_items = []
    for src in sources:
        label = src.get("label", "")
        url = src.get("url", "")
        if url:
            src_items.append(f"<li><a href=\"{url}\" target=\"_blank\" rel=\"noopener\">{label or url}</a></li>")
        else:
            src_items.append(f"<li>{label}</li>")
    sources_html = ""
    if src_items:
        sources_html = f"<h3>Sources</h3><ul class=\"sources\">{''.join(src_items)}</ul>"
    return f"<section class=\"summary\"><h2>{heading}</h2><ul>{li}</ul>{sources_html}</section>"


def render_tip(cfg: Dict[str, Any]) -> str:
    tip = cfg.get("tip", "")
    if not tip:
        return ""
    if tip.strip().lower().startswith("tip:"):
        tip = "💡 " + tip.strip()
    return f"<section class=\"tip\">{tip}</section>"


def render_about(cfg: Dict[str, Any]) -> str:
    about = cfg.get("about", "")
    if not about:
        return ""
    return f"<section class=\"about\"><h2>About the project</h2><p>{about}</p></section>"


def render_citation(cfg: Dict[str, Any]) -> str:
    citation = cfg.get("citation", "")
    license_text = cfg.get("license", "")
    if not citation and not license_text:
        return ""
    citation_html = f"<div><strong>Citation:</strong> {citation}</div>" if citation else ""
    license_html = f"<div><strong>License:</strong> {license_text}</div>" if license_text else ""
    return f"<section class=\"summary\"><div class=\"meta\">{citation_html}{license_html}</div></section>"


def render_panels(cfg: Dict[str, Any]) -> str:
    panels = cfg.get("panels", [])
    blocks = []
    for panel in panels:
        title = panel.get("title", "")
        plot = panel.get("plot", "")
        bullets = panel.get("bullets", [])
        note = panel.get("note", "")
        source = panel.get("source", "")
        li = "".join([f"<li>{b}</li>" for b in bullets])
        meta_bits = []
        if note:
            meta_bits.append(f"<div><strong>Note:</strong> {note}</div>")
        if source:
            meta_bits.append(f"<div><strong>Source:</strong> {source}</div>")
        meta_html = f"<div class=\"meta\">{''.join(meta_bits)}</div>" if meta_bits else ""
        block = (
            "<article class=\"panel\">"
            f"<h3>{title}</h3>"
            f"<button class=\"expand-btn\" data-expand data-title=\"{title}\" data-src=\"{plot}\">Expand</button>"
            f"<iframe src=\"{plot}\" loading=\"lazy\"></iframe>"
            f"{meta_html}"
            f"<ul>{li}</ul>"
            "</article>"
        )
        blocks.append(block)
    return "".join(blocks)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build dashboard HTML")
    parser.add_argument("period", help="Dashboard subfolder name, e.g. Jan_2026")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    period_dir = DASH_DIR / args.period
    yaml_path = period_dir / "dashboard.yaml"
    template_path = period_dir / "template.html"
    site_dir = SITE_DIR / args.period
    out_html = site_dir / "index.html"

    cfg = parse_yaml(yaml_path)
    template = template_path.read_text(encoding="utf-8")
    html = (
        template.replace("{{TITLE}}", cfg.get("title", "Dashboard"))
        .replace("{{HEADER}}", render_header(cfg))
        .replace("{{SUMMARY}}", render_summary(cfg, period_dir))
        .replace("{{TIP}}", render_tip(cfg))
        .replace("{{PANELS}}", render_panels(cfg))
        .replace("{{ABOUT}}", render_about(cfg))
        .replace("{{CITATION}}", render_citation(cfg))
    )
    site_dir.mkdir(parents=True, exist_ok=True)
    (site_dir / "plots").mkdir(parents=True, exist_ok=True)
    (site_dir / "logos").mkdir(parents=True, exist_ok=True)
    out_html.write_text(html, encoding="utf-8")

    # Copy plots and logos into docs for GitHub Pages.
    plots_src = BASE_DIR / "plots" / args.period
    for plot in plots_src.glob("*.html"):
        shutil.copy2(plot, site_dir / "plots" / plot.name)

    for logo in (BASE_DIR / "logos").iterdir():
        if logo.is_file():
            shutil.copy2(logo, site_dir / "logos" / logo.name)


if __name__ == "__main__":
    main()
