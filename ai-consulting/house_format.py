#!/usr/bin/env python3
"""Alan Cohen · AI Consulting — house document format.

Single source of truth for how every proposal, work plan and quote from
this business looks. Client documents import from here and supply content
only; nothing in this module is client-specific.

Origin: the format was reverse-engineered from the Neve Tzedek Properties
work plan, which was a Chrome print-to-PDF of an HTML page (Skia/PDF
producer, Arial only). Documents are therefore built as HTML and printed
with headless Chromium rather than composed in ReportLab — that keeps
Hebrew bidi native instead of hand-rolled, and keeps the output visually
identical to the original.

Every token below was sampled from a render of that reference, not chosen
by eye. Change them here and every client document follows.

Both writing directions are supported from one template: pass lang="he"
for RTL or lang="en" for LTR and the alignment, callout accent edge, list
indent and table column flush all mirror.
"""

import html
import os
import subprocess
import tempfile

# ---------------------------------------------------------------- tokens
GOLD = "#ECCF9F"        # top band, section rules
BRONZE = "#C98F5E"      # callout accent, list markers
INK_BAND = "#0B0805"    # header band
INK_TEXT = "#1A130D"    # headings and body
MUTED = "#6B5D4F"       # subtitle, table headers
FAINT = "#8A7F72"       # notes, footer
CREAM = "#FAF5EC"       # callout fill
RULE_HEAD = "#D9CBB8"   # rule under a table header row
RULE_HAIR = "#EFE7DA"   # rule between table rows
MARK_TAG = "#958F87"    # "AI CONSULTING" lockup line
CLIENT_TXT = "#A8A198"  # client block in the header band

MARGIN = "25pt"         # content margin
BAND_H = "3pt"          # gold band
HEAD_H = "46pt"         # dark header band
HEAD_GAP = "30pt"       # breathing room under the header, every page

FONT = "'Liberation Sans', Arial, Helvetica, sans-serif"

BUSINESS = "Alan Cohen"
BUSINESS_TAG = "AI CONSULTING"
CONTACT = "Alan Cohen · AI Consulting · acohen.tlv@gmail.com"

CHROME_CANDIDATES = [
    "/opt/pw-browsers/chromium_headless_shell-1194/chrome-linux/headless_shell",
    "/opt/pw-browsers/chromium-1194/chrome-linux/chrome",
]


def find_chrome():
    for p in CHROME_CANDIDATES:
        if os.path.exists(p):
            return p
    for root, _, files in os.walk("/opt/pw-browsers"):
        for f in files:
            if f in ("headless_shell", "chrome"):
                return os.path.join(root, f)
    raise RuntimeError("no chromium binary found")


# ---------------------------------------------------------------- blocks
def e(s):
    return html.escape(s, quote=False)


def title(text):
    return f'<h1 class="title">{e(text)}</h1>'


def subtitle(text):
    return f'<p class="subtitle">{text}</p>'


def section(heading, *blocks):
    return f'<h2 class="sec">{e(heading)}</h2>\n' + "\n".join(blocks)


def h3(text):
    return f"<h3>{e(text)}</h3>"


def para(text, cls="body"):
    return f'<p class="{cls}">{text}</p>'


def note(text):
    return para(text, "note")


def callout(text):
    return f'<div class="callout">{text}</div>'


def bullets(items):
    return "<ul>" + "".join(f"<li>{t}</li>" for t in items) + "</ul>"


def b(t):
    return f"<b>{e(t)}</b>"


def ltr(t):
    """Isolate a Latin/numeric run so RTL reordering leaves it intact.

    Without this, a range like "$0-49" reorders to "49-$0" in Hebrew text.
    """
    return f'<span dir="ltr">{e(t)}</span>'


def table(headers, rows, widths=None, first_bold=True, flush_last=True):
    """flush_last mirrors the reference, where the trailing column is a short
    duration pinned to the far edge. Turn it off when that column carries
    prose — flushing it just leaves the paragraph ragged.
    """
    cols = ""
    if widths:
        cols = "<colgroup>" + "".join(
            f'<col style="width:{w}">' for w in widths) + "</colgroup>"
    head = "".join(f"<th>{e(h)}</th>" for h in headers)
    body = ""
    for r in rows:
        cells = ""
        for i, c in enumerate(r):
            cls = ' class="lead"' if (first_bold and i == 0) else ""
            cells += f"<td{cls}>{c}</td>"
        body += f"<tr>{cells}</tr>"
    cls = "data" if flush_last else "data noflush"
    return (f'<table class="{cls}">{cols}<thead><tr>{head}</tr></thead>'
            f"<tbody>{body}</tbody></table>")


# ---------------------------------------------------------------- styles
def css(rtl):
    start, end = ("right", "left") if rtl else ("left", "right")
    return f"""
@page {{ size: A4; margin: 0; }}
* {{ box-sizing: border-box; }}
html, body {{ margin: 0; padding: 0; }}
body {{
  font-family: {FONT};
  font-size: 8.4pt; line-height: 1.55; color: {INK_TEXT};
  direction: {'rtl' if rtl else 'ltr'}; text-align: {start};
  -webkit-print-color-adjust: exact; print-color-adjust: exact;
}}

/* Running header/footer as thead/tfoot of a document-wide table.
   position:fixed is wrong here: it overlays rather than reserves space, so
   continuation pages slide their first lines under the header band. A
   repeating thead both prints on every page and pushes content down. */
table.doc {{ width: 100%; border-collapse: collapse; }}
.doc > thead > tr > td, .doc > tfoot > tr > td {{ padding: 0; }}
.band {{ height: {BAND_H}; background: {GOLD}; }}
.head {{ height: {HEAD_H}; background: {INK_BAND}; padding: 9pt {MARGIN} 0;
        direction: ltr; display: flex; justify-content: space-between; }}
.headgap {{ height: {HEAD_GAP}; }}
.head .name {{ color: {GOLD}; font-size: 10.5pt; letter-spacing: 3.2pt; }}
.head .tag  {{ color: {MARK_TAG}; font-size: 6.6pt; letter-spacing: 2.1pt;
              margin-top: 2pt; }}
.head .who  {{ color: {CLIENT_TXT}; font-size: 7pt; line-height: 1.5;
              text-align: right; }}
.foot {{ padding: 6pt {MARGIN} 10pt; border-top: 0.6pt solid {RULE_HAIR};
        color: {FAINT}; font-size: 6.6pt;
        direction: ltr; display: flex; justify-content: space-between; }}
.page {{ padding: 0 {MARGIN} 16pt; }}

h1.title {{ font-size: 15pt; line-height: 1.3; margin: 0 0 4pt;
           color: {INK_TEXT}; }}
p.subtitle {{ color: {MUTED}; font-size: 8pt; margin: 0 0 16pt;
             line-height: 1.6; }}

h2.sec {{ font-size: 9.6pt; margin: 17pt 0 0; padding-bottom: 4pt;
         border-bottom: 1.2pt solid {GOLD}; break-after: avoid; }}
h3 {{ font-size: 8.6pt; margin: 12pt 0 4pt; break-after: avoid; }}
p.body {{ margin: 8pt 0; }}
p.note {{ color: {FAINT}; font-size: 7.4pt; margin: 6pt 0 0;
         line-height: 1.55; }}

.callout {{ background: {CREAM}; border-{start}: 3pt solid {BRONZE};
           padding: 9pt 12pt; margin: 10pt 0; line-height: 1.6;
           break-inside: avoid; }}

table.data {{ width: 100%; border-collapse: collapse; margin: 9pt 0 0;
             font-size: 7.8pt; }}
.data thead {{ display: table-header-group; }}
.data th {{ text-align: {start}; font-weight: bold; color: {MUTED};
           font-size: 7.4pt; padding: 0 0 5pt;
           border-bottom: 1pt solid {RULE_HEAD}; }}
/* trailing column sits flush to the far edge, as in the reference; never
   set `direction` here — it reorders mixed runs and scrambles prose */
.data th:last-child, .data td:last-child {{ text-align: {end}; }}
.data.noflush th:last-child, .data.noflush td:last-child
  {{ text-align: {start}; }}
.data td {{ padding: 6pt 0; border-bottom: 0.6pt solid {RULE_HAIR};
           vertical-align: top; line-height: 1.5; }}
.data th + th, .data td + td {{ padding-{start}: 10pt; }}
.data td.lead {{ font-weight: bold; white-space: nowrap; }}
.data tr {{ break-inside: avoid; }}

ul {{ margin: 8pt 0; padding-{start}: 14pt; padding-{end}: 0; }}
li {{ margin: 0 0 5pt; line-height: 1.6; }}
li::marker {{ color: {BRONZE}; }}
b {{ font-weight: bold; }}
i {{ font-style: italic; color: {FAINT}; }}
"""


# ---------------------------------------------------------------- render
def render(body, out_path, lang, client, date, doc_title, kind=None):
    """Print an HTML body to PDF in the house format.

    body       already-assembled HTML from the block helpers above
    lang       "he" (RTL) or "en" (LTR)
    client     shown in the header band and the footer
    doc_title  the PDF's title metadata / browser tab name
    kind       label in the footer; defaults per language
    """
    rtl = lang == "he"
    if kind is None:
        kind = "הצעת עבודה" if rtl else "Work proposal"
    foot_r = (f'<span dir="rtl">{e(kind)} · הוכנה עבור {e(client)} · '
              f"{e(date)}</span>" if rtl
              else f"{e(kind)} · prepared for {e(client)} · {e(date)}")

    doc = f"""<!doctype html>
<html lang="{lang}" dir="{'rtl' if rtl else 'ltr'}"><head><meta charset="utf-8">
<title>{e(doc_title)}</title><style>{css(rtl)}</style></head><body>
<table class="doc">
<thead><tr><td>
  <div class="band"></div>
  <div class="head">
    <div class="mark">
      <div class="name">{e(BUSINESS.upper())}</div>
      <div class="tag">{e(BUSINESS_TAG)}</div>
    </div>
    <div class="who">{e(client)}<br>{e(date)}</div>
  </div>
  <div class="headgap"></div>
</td></tr></thead>
<tfoot><tr><td>
  <div class="foot"><div>{e(CONTACT)}</div><div>{foot_r}</div></div>
</td></tr></tfoot>
<tbody><tr><td class="page">
{body}
</td></tr></tbody>
</table>
</body></html>"""

    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
    with tempfile.TemporaryDirectory() as td:
        src = os.path.join(td, f"doc_{lang}.html")
        with open(src, "w", encoding="utf-8") as fh:
            fh.write(doc)
        subprocess.run(
            [find_chrome(), "--headless", "--disable-gpu", "--no-sandbox",
             "--no-pdf-header-footer", f"--print-to-pdf={out_path}", src],
            check=True, capture_output=True)
    print(f"Wrote {out_path}")
