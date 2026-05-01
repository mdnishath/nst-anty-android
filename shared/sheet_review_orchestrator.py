"""
shared/sheet_review_orchestrator.py — Write Review op driven by a
Google Sheet workbook where every business has its own tab.

Tab layout (matches the user's "Project_Management" template):
  Row 1   : ◀ Back to Dashboard / Back to All Post — navigation links
  Row 2   : <Business Name>                                      | # X / Y
  Row 3   : GMB Link    Category    Orders/month    Rating/Notes
  Row 4   : <gmb url>   <category>  <count>         <rating>
  Row 5/6 : Address / Phone / City / Proxy region (key/value)
  Row 8/9 : Stats row — Orders | Posted | Live | Pending | Rejected | Done
  Row 11  : Review table HEADER:
            # | Date | Review Text | Review Live Link | Status | Email |
            Proxy/IP | Worker | Notes | Order # | Direct Review Link
  Row 12+ : Review rows.

Public API
----------
parse_business_tab(resources_path, sheet_id, tab_name) -> dict
    Locate the review table and return the business info + every row
    that's eligible (Status blank, has Review Text and Direct Review
    Link).

list_tabs_summary(resources_path, sheet_id, tab_names) -> dict
    For a chosen list of tabs, return per-tab counts (eligible /
    posted / total) so the UI can show the user what's available
    before they pick how many to post.

start_review_from_sheet(resources_path, sheet_id, tabs_config,
                        num_workers) -> dict
    Kick off the actual posting flow. tabs_config is a list of
    {tab_name, count}. For each tab we take *count* eligible rows in
    order, match their Email column to existing profiles, run the
    standard write_review op for each, and write the result back to
    the sheet (Date, Review Live Link, Status='Live', Notes=stars,
    Worker, Order #).
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

from shared import sheets_integration as _si

# Header names we expect inside the review table (case-insensitive).
HEADERS_REQUIRED = ['Date', 'Review Text', 'Review Live Link', 'Status',
                    'Email', 'Notes', 'Direct Review Link']
HEADER_OPTIONAL = ['#', 'Proxy / IP', 'Worker', 'Order #', 'Order#']


def _find_header_row(values: list[list]) -> Optional[int]:
    """Locate the row that contains 'Review Text' and 'Direct Review Link'.
    Returns the 0-based row index or None."""
    for ri, row in enumerate(values):
        if not row:
            continue
        cells = [str(c or '').strip().lower() for c in row]
        if 'review text' in cells and any(
            ('direct review link' in c) or (c == 'direct review link')
            for c in cells
        ):
            return ri
    return None


def _index_headers(header_row: list) -> dict[str, int]:
    """Map header name (lowercased, trimmed) → 0-based column index."""
    out = {}
    for i, h in enumerate(header_row):
        key = str(h or '').strip().lower()
        if key:
            out[key] = i
    return out


def _row_value(row: list, col_idx: int) -> str:
    if col_idx is None or col_idx < 0:
        return ''
    if col_idx >= len(row):
        return ''
    return str(row[col_idx] or '').strip()


def _parse_int(v) -> int:
    try:
        return int(str(v).strip())
    except Exception:
        return 0


def parse_business_tab(resources_path, sheet_id: str, tab_name: str) -> dict:
    """Read one business tab and return its review table + business info."""
    res = _si.read_sheet(resources_path, sheet_id, tab_name)
    if not res.get('success'):
        return {'success': False, 'message': res.get('message')}
    values = res.get('values') or []
    if not values:
        return {'success': False, 'message': f"Tab '{tab_name}' is empty"}

    # Business name is usually in row 2 col A.
    business_name = ''
    if len(values) > 1 and values[1]:
        business_name = str(values[1][0] or '').strip()
    if not business_name:
        business_name = tab_name

    # GMB URL is typically the first http(s) cell in rows 1-7
    gmb_url = ''
    for r in values[:8]:
        for c in r or []:
            s = str(c or '').strip()
            if s.startswith('http') and ('google.com/maps' in s or 'goo.gl' in s):
                gmb_url = s
                break
        if gmb_url:
            break

    # Locate review table header
    h_idx = _find_header_row(values)
    if h_idx is None:
        return {
            'success': False,
            'message': (f"Could not find review table in '{tab_name}'. "
                        "Expected a row containing 'Review Text' and "
                        "'Direct Review Link'."),
        }
    headers = values[h_idx]
    cols = _index_headers(headers)

    # Required columns
    missing_cols = []
    for needed in HEADERS_REQUIRED:
        key = needed.lower()
        # Tolerate "Order #" / "Order#" naming variance
        if key == 'order #' and ('order #' in cols or 'order#' in cols):
            continue
        if key not in cols:
            missing_cols.append(needed)
    # 'Notes' / 'Order #' / 'Direct Review Link' are required for posting
    if any(c in missing_cols for c in
           ('Direct Review Link', 'Review Text', 'Status', 'Email')):
        return {
            'success': False,
            'message': (f"Tab '{tab_name}' is missing required columns: "
                        f"{', '.join(missing_cols)}"),
        }

    date_c = cols.get('date')
    text_c = cols.get('review text')
    link_c = cols.get('review live link')
    status_c = cols.get('status')
    email_c = cols.get('email')
    proxy_c = cols.get('proxy / ip', cols.get('proxy/ip'))
    worker_c = cols.get('worker')
    notes_c = cols.get('notes')
    order_c = cols.get('order #', cols.get('order#'))
    direct_c = cols.get('direct review link')

    eligible = []
    posted = 0
    pending = 0
    total_rows = 0
    skipped_no_text_or_link = 0

    for offset, row in enumerate(values[h_idx + 1:], start=h_idx + 2):
        # offset is the 1-based row number on the sheet
        row = row or []
        review_text = _row_value(row, text_c)
        direct_link = _row_value(row, direct_c)
        if not review_text and not direct_link:
            # Empty row — stop counting if this is the tail of the table
            continue
        total_rows += 1
        status = _row_value(row, status_c).lower()
        email = _row_value(row, email_c)
        notes = _row_value(row, notes_c)
        date_v = _row_value(row, date_c)
        live_link = _row_value(row, link_c)
        if status in ('live', 'done'):
            posted += 1
            continue
        if status in ('pending', 'queued', 'in progress'):
            pending += 1
            continue
        if not review_text or not direct_link:
            skipped_no_text_or_link += 1
            continue
        eligible.append({
            'row': offset,
            'date': date_v,
            'review_text': review_text,
            'review_live_link': live_link,
            'status': status,
            'email': email,
            'proxy': _row_value(row, proxy_c),
            'worker': _row_value(row, worker_c),
            'notes': notes,           # holds the star count
            'order': _row_value(row, order_c),
            'direct_review_link': direct_link,
        })

    return {
        'success': True,
        'tab': tab_name,
        'business_name': business_name,
        'gmb_url': gmb_url,
        'headers': headers,
        'header_row': h_idx + 1,
        'columns': {
            'date': (date_c + 1) if date_c is not None else None,
            'review_text': (text_c + 1) if text_c is not None else None,
            'review_live_link': (link_c + 1) if link_c is not None else None,
            'status': (status_c + 1) if status_c is not None else None,
            'email': (email_c + 1) if email_c is not None else None,
            'proxy': (proxy_c + 1) if proxy_c is not None else None,
            'worker': (worker_c + 1) if worker_c is not None else None,
            'notes': (notes_c + 1) if notes_c is not None else None,
            'order': (order_c + 1) if order_c is not None else None,
            'direct_review_link': (direct_c + 1) if direct_c is not None else None,
        },
        'eligible': eligible,
        'eligible_count': len(eligible),
        'posted_count': posted,
        'pending_count': pending,
        'total_review_rows': total_rows,
        'skipped_no_text_or_link': skipped_no_text_or_link,
    }


def plan_run(resources_path, sheet_id: str,
             tabs_config: list[dict]) -> dict:
    """Resolve a tabs_config list ([{tab_name, count}, ...]) into the
    concrete per-row work plan that the worker will execute.

    Returns:
      {
        success: True,
        tabs:    [
            {tab, business_name, gmb_url, columns, rows: [...top N eligible...]},
        ],
        total_planned: int,
      }
    """
    out = []
    total = 0
    for cfg in tabs_config:
        tab = cfg.get('tab_name') or cfg.get('tab') or ''
        count = max(0, int(cfg.get('count') or 0))
        if not tab or count == 0:
            continue
        info = parse_business_tab(resources_path, sheet_id, tab)
        if not info.get('success'):
            out.append({'tab': tab, 'error': info.get('message'), 'rows': []})
            continue
        rows = (info.get('eligible') or [])[:count]
        out.append({
            'tab': tab,
            'business_name': info.get('business_name'),
            'gmb_url': info.get('gmb_url'),
            'columns': info.get('columns'),
            'rows': rows,
        })
        total += len(rows)
    return {'success': True, 'tabs': out, 'total_planned': total}


def list_tabs_summary(resources_path, sheet_id: str,
                      tab_names: list[str]) -> dict:
    """Per-tab eligible/posted counts. Used by the modal preview."""
    out = []
    grand_eligible = 0
    grand_posted = 0
    for tab in tab_names:
        info = parse_business_tab(resources_path, sheet_id, tab)
        if not info.get('success'):
            out.append({
                'tab': tab, 'error': info.get('message'),
                'eligible_count': 0, 'posted_count': 0, 'total_review_rows': 0,
                'business_name': tab,
            })
            continue
        out.append({
            'tab': tab,
            'business_name': info.get('business_name', tab),
            'gmb_url': info.get('gmb_url', ''),
            'eligible_count': info.get('eligible_count', 0),
            'posted_count': info.get('posted_count', 0),
            'pending_count': info.get('pending_count', 0),
            'total_review_rows': info.get('total_review_rows', 0),
        })
        grand_eligible += info.get('eligible_count', 0)
        grand_posted += info.get('posted_count', 0)
    return {
        'success': True,
        'tabs': out,
        'totals': {
            'eligible': grand_eligible,
            'posted': grand_posted,
        },
    }
