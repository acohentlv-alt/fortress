#!/usr/bin/env python3
"""Generate the Hebrew (RTL) Rosabella TLV e-commerce proposal PDF.

Branding: Alan — indie AI builder & consultant. Same warm editorial palette
as the English edition (ink charcoal, dusty rose, sand), mirrored for RTL.

RTL notes: ReportLab's wordWrap='RTL' reverses glyph order incorrectly, so
this script does its own line-breaking in logical order and applies the
Unicode bidi algorithm per line (see wrap_rtl). Each visual line is emitted
as its own right-aligned Paragraph, and every wrap width is kept strictly
below the available frame/column width — if ReportLab re-wraps a line that
is already in visual order, words land in the wrong place.

Usage:  python3 generate_proposal_he.py  ->  Rosabella-TLV-Proposal-HE.pdf
"""

import os

from bidi.algorithm import get_display
from reportlab.lib import colors
from reportlab.lib.enums import TA_RIGHT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import (
    BaseDocTemplate,
    Frame,
    HRFlowable,
    KeepTogether,
    NextPageTemplate,
    PageBreak,
    PageTemplate,
    Paragraph,
    Spacer,
    Table,
    TableStyle,
)

# ---------------------------------------------------------------- fonts
DV = "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"
DVB = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
pdfmetrics.registerFont(TTFont("He", DV))
pdfmetrics.registerFont(TTFont("He-Bold", DVB))
pdfmetrics.registerFontFamily("He", normal="He", bold="He-Bold")

# ---------------------------------------------------------------- palette
INK = colors.HexColor("#26211E")
ROSE = colors.HexColor("#B85C6B")
ROSE_DEEP = colors.HexColor("#8F3E4D")
SAND = colors.HexColor("#F6F0E8")
SAND_DEEP = colors.HexColor("#EAE0D2")
SLATE = colors.HexColor("#6B625C")
LINE = colors.HexColor("#D9CFC2")

PAGE_W, PAGE_H = A4
MARGIN = 20 * mm
W = PAGE_W - 2 * MARGIN          # frame width
SAFE = W - 6                      # wrap ceiling, keeps ReportLab from re-wrapping

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                   "Rosabella-TLV-Proposal-HE.pdf")


# ---------------------------------------------------------------- rtl core
def wrap_rtl(text, font, size, maxw):
    """Wrap logical-order text to maxw, return lines in visual order."""
    words, lines, cur = text.split(), [], []
    for word in words:
        trial = " ".join(cur + [word])
        if cur and pdfmetrics.stringWidth(trial, font, size) > maxw:
            lines.append(" ".join(cur))
            cur = [word]
        else:
            cur.append(word)
    if cur:
        lines.append(" ".join(cur))
    return [get_display(ln, base_dir="R") for ln in lines]


def esc(s):
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def rtl(text, font="He", size=10, leading=15, color=INK, maxw=SAFE,
        space_after=0, space_before=0):
    """A logical-order Hebrew string -> list of right-aligned Paragraphs."""
    lines = wrap_rtl(text, font, size, maxw)
    out = []
    for i, ln in enumerate(lines):
        st = ParagraphStyle(
            f"r{id(text)}_{i}", fontName=font, fontSize=size, leading=leading,
            alignment=TA_RIGHT, textColor=color,
            spaceBefore=space_before if i == 0 else 0,
            spaceAfter=space_after if i == len(lines) - 1 else 0)
        out.append(Paragraph(esc(ln), st))
    return out


def h1(text):
    return rtl(text, "He-Bold", 18, 25, INK, SAFE, space_after=8, space_before=2)


def h2(text):
    return rtl(text, "He-Bold", 12, 17, INK, SAFE, space_after=4, space_before=9)


def body(text):
    return rtl(text, "He", 10, 16, INK, SAFE, space_after=6)


def muted(text):
    return rtl(text, "He", 9, 14, SLATE, SAFE, space_after=5)


def kicker(text):
    return rtl(text, "He-Bold", 8.5, 12, ROSE, SAFE, space_after=3)


def rule():
    return HRFlowable(width="100%", thickness=0.7, color=LINE,
                      spaceBefore=2, spaceAfter=9)


def bullets(items):
    """RTL bullets — marker sits at the right edge of the first visual line."""
    flow = []
    for item in items:
        lines = wrap_rtl(item, "He", 10, SAFE - 14)
        for i, ln in enumerate(lines):
            st = ParagraphStyle(
                f"b{id(item)}_{i}", fontName="He", fontSize=10, leading=15,
                alignment=TA_RIGHT, textColor=INK, rightIndent=0 if i == 0 else 14,
                spaceAfter=4 if i == len(lines) - 1 else 0)
            txt = esc(ln)
            if i == 0:
                txt = f"{txt} <font color='#B85C6B'>&#9642;</font>"
            flow.append(Paragraph(txt, st))
    return flow


def data_table(header, rows, widths, highlight_last=False):
    """RTL table: column order reversed, every cell right-aligned."""
    def cell(text, font, color):
        pad = 14
        idx = len(cell.widths) - 1 - cell.col
        lines = wrap_rtl(text, font, 9, cell.widths[cell.col] - pad)
        st = ParagraphStyle(f"c{id(text)}", fontName=font, fontSize=9,
                            leading=12.5, alignment=TA_RIGHT, textColor=color)
        return [Paragraph(esc(ln), st) for ln in lines]

    cell.widths = widths
    data = []
    hdr_cells = []
    for i, h in enumerate(reversed(header)):
        cell.col = i
        hdr_cells.append(cell(h, "He-Bold", colors.white))
    data.append(hdr_cells)
    for r in rows:
        rc = []
        for i, c in enumerate(reversed(r)):
            cell.col = i
            rc.append(cell(c, "He", INK))
        data.append(rc)

    t = Table(data, colWidths=widths, repeatRows=1, hAlign="RIGHT")
    style = [
        ("BACKGROUND", (0, 0), (-1, 0), INK),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, SAND]),
        ("LINEBELOW", (0, 0), (-1, -1), 0.4, LINE),
        ("TOPPADDING", (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ("LEFTPADDING", (0, 0), (-1, -1), 7),
        ("RIGHTPADDING", (0, 0), (-1, -1), 7),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
    ]
    if highlight_last:
        style += [("BACKGROUND", (0, -1), (-1, -1), SAND_DEEP),
                  ("LINEABOVE", (0, -1), (-1, -1), 0.9, ROSE)]
    t.setStyle(TableStyle(style))
    return t


def signoff():
    inner = []
    inner += rtl("אלן — ייעוץ ובנייה בינה מלאכותית", "He-Bold", 12.5, 17,
                 colors.white, W - 30, space_after=2)
    inner += rtl("בונה עצמאי של כלי AI", "He", 9, 13, ROSE, W - 30,
                 space_after=7)
    inner += rtl("בונה עצמאי של כלים קטנים וחדים לעסקים אמיתיים. אדם אחד, "
                 "מקצה לקצה, בלי העברות בין גורמים — מי שאתם מדברים איתו הוא "
                 "מי שבונה.", "He", 9, 13.5, colors.HexColor("#D9D0C9"),
                 W - 30, space_after=7)
    inner += rtl("השלב הבא: שיחה של 30 דקות לעבור על חמש השאלות שלמעלה. עם "
                 "התשובות ביד, אשלח הסכם חתום ולוח זמנים מתוארך להשקה באפריל.",
                 "He-Bold", 9.5, 14, colors.white, W - 30)
    t = Table([[inner]], colWidths=[W], hAlign="RIGHT")
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), INK),
        ("LINEAFTER", (0, 0), (-1, -1), 2.5, ROSE),
        ("TOPPADDING", (0, 0), (-1, -1), 12),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 12),
        ("LEFTPADDING", (0, 0), (-1, -1), 14),
        ("RIGHTPADDING", (0, 0), (-1, -1), 14),
    ]))
    return t


# ---------------------------------------------------------------- pages
def he(s):
    return get_display(s, base_dir="R")


def draw_cover(canv, doc):
    canv.saveState()
    canv.setFillColor(SAND)
    canv.rect(0, 0, PAGE_W, PAGE_H, stroke=0, fill=1)
    # rose band mirrored to the right edge for RTL
    canv.setFillColor(ROSE)
    canv.rect(PAGE_W - 7 * mm, 0, 7 * mm, PAGE_H, stroke=0, fill=1)

    canv.setFillColor(INK)
    canv.setFont("He-Bold", 10.5)
    canv.drawRightString(PAGE_W - MARGIN, PAGE_H - 22 * mm, he("אלן"))
    canv.setFillColor(ROSE_DEEP)
    canv.setFont("He", 10.5)
    canv.drawRightString(PAGE_W - MARGIN - 14 * mm, PAGE_H - 22 * mm,
                         he("בונה עצמאי של כלי AI  /"))
    canv.setStrokeColor(LINE)
    canv.setLineWidth(0.7)
    canv.line(MARGIN, PAGE_H - 26 * mm, PAGE_W - MARGIN, PAGE_H - 26 * mm)

    y = PAGE_H - 90 * mm
    canv.setFillColor(ROSE_DEEP)
    canv.setFont("He-Bold", 9.5)
    canv.drawRightString(PAGE_W - MARGIN, y + 34 * mm,
                         he("הצעה להקמת חנות אונליין"))
    canv.setFillColor(INK)
    canv.setFont("He-Bold", 30)
    canv.drawRightString(PAGE_W - MARGIN, y + 20 * mm, he("רוזבלה תל אביב"))
    canv.setFont("He", 12)
    canv.setFillColor(SLATE)
    canv.drawRightString(PAGE_W - MARGIN, y + 10 * mm,
                         he("מרצפת הבוטיק לחנות אונליין — רזה, אוטומטי, נאמן למותג."))

    canv.setFillColor(colors.white)
    canv.roundRect(MARGIN, y - 46 * mm, PAGE_W - 2 * MARGIN, 40 * mm,
                   3 * mm, stroke=0, fill=1)
    cx = PAGE_W - MARGIN - 8 * mm
    rows = [
        ("עבור", "רוזבלה תל אביב — בוטיק אופנה רב-מותגי"),
        ("כתובת", "שבזי 31, נווה צדק, תל אביב"),
        ("קהל", "11,000 עוקבים באינסטגרם"),
        ("הוכן על ידי", "אלן  ·  יעד השקה: אפריל"),
    ]
    ry = y - 12 * mm
    for label, val in rows:
        canv.setFillColor(ROSE_DEEP)
        canv.setFont("He-Bold", 8.5)
        canv.drawRightString(cx, ry, he(label))
        canv.setFillColor(INK)
        canv.setFont("He", 10)
        canv.drawRightString(cx - 30 * mm, ry, he(val))
        ry -= 8 * mm

    canv.setFillColor(SLATE)
    canv.setFont("He", 8.5)
    canv.drawRightString(PAGE_W - MARGIN, 18 * mm,
                         he("כלים קטנים, אוטומציות חדות, היקף עבודה כן."))
    canv.restoreState()


def draw_page(canv, doc):
    canv.saveState()
    canv.setFillColor(INK)
    canv.setFont("He-Bold", 8)
    canv.drawRightString(PAGE_W - MARGIN, PAGE_H - 12 * mm, he("אלן — ייעוץ AI"))
    canv.setFillColor(SLATE)
    canv.setFont("He", 8)
    canv.drawString(MARGIN, PAGE_H - 12 * mm,
                    he("רוזבלה תל אביב — הצעה להקמת חנות אונליין"))
    canv.setStrokeColor(LINE)
    canv.setLineWidth(0.6)
    canv.line(MARGIN, PAGE_H - 14.5 * mm, PAGE_W - MARGIN, PAGE_H - 14.5 * mm)
    canv.line(MARGIN, 15 * mm, PAGE_W - MARGIN, 15 * mm)
    canv.setFillColor(SLATE)
    canv.setFont("He", 8)
    canv.drawRightString(PAGE_W - MARGIN, 10.5 * mm,
                         he("חסוי — הוכן עבור רוזבלה תל אביב"))
    canv.setFillColor(ROSE_DEEP)
    canv.drawString(MARGIN, 10.5 * mm, f"{canv.getPageNumber()}")
    canv.restoreState()


# ---------------------------------------------------------------- content
def build():
    doc = BaseDocTemplate(
        OUT, pagesize=A4, leftMargin=MARGIN, rightMargin=MARGIN,
        topMargin=24 * mm, bottomMargin=22 * mm,
        title="רוזבלה תל אביב — הצעה להקמת חנות אונליין",
        author="אלן — ייעוץ AI")
    frame = Frame(MARGIN, 22 * mm, W, PAGE_H - 46 * mm, id="body")
    doc.addPageTemplates([
        PageTemplate(id="Cover", frames=[Frame(MARGIN, 22 * mm, W,
                     PAGE_H - 46 * mm, id="cover")], onPage=draw_cover),
        PageTemplate(id="Body", frames=[frame], onPage=draw_page),
    ])

    el = [NextPageTemplate("Body"), PageBreak()]

    # ---- 1. overview
    el += kicker("01 · סקירת הפרויקט")
    el += h1("חנות אונליין ברמת בוטיק, בלי התקורה של משרד פרסום")
    el += [rule()]
    el += body("לרוזבלה תל אביב יש בדיוק את מה שרוב החנויות המקוונות רודפות "
               "אחריו שנים: קולקציה רב-מותגית מוקפדת, חנות באחד הרחובות "
               "המהלכים ביותר בתל אביב, וקהל של 11,000 עוקבים באינסטגרם. מה "
               "שחסר הוא דרך לקהל הזה לקנות כשהוא לא עומד ברחוב שבזי.")
    el += body("ההצעה הזו מכסה עיצוב, בנייה והשקה של חנות Shopify עבור רוזבלה "
               "תל אביב, מחוברת לאוטומציות קלות כדי שהתפעול היומיומי יישאר "
               "פשוט לצוות קטן. המטרה ברורה: להפוך תשומת לב באינסטגרם "
               "להזמנות, לשמור על מלאי אמין בין החנות לאתר, והכול ללא תשלום "
               "מראש — בלי דמי בנייה, בלי ריטיינרים מנופחים. אני מקבל תשלום "
               "מתוך מה שהאתר מכניס, או שלא בכלל.")
    el += h2("איך נראית הצלחה")
    el += bullets([
        "חנות Shopify חיה, מותאמת קודם כול לנייד, שנושאת את המראה של הבוטיק.",
        "קישורים מהביו ומהפוסטים באינסטגרם ישירות לעמודי מוצר.",
        "מלאי מסונכרן בתהליך CSV פשוט שהצוות מריץ פעם בשבוע.",
        "הודעות הזמנה, משלוח ומעקב שנשלחות אוטומטית בוואטסאפ ובמייל.",
        "חנות שהצוות מפעיל ביומיום בלי תלות בי — בזמן שאני ממשיך לתחזק "
        "ולשפר אותה לאורך כל תקופת השותפות.",
    ])

    # ---- 2. timeline
    el += [Spacer(1, 8)]
    el += kicker("02 · הערכת לוחות זמנים")
    el += h1("שבועיים עד שלושה של עבודה ממוקדת")
    el += [rule()]
    el += body("הבנייה מתומחרת לשבועיים עד שלושה מקצה לקצה. שבוע 3 הוא "
               "חיץ: הוא סופג עיכובי תוכן (צילומים, טקסטים, נתוני ספקים) ולא "
               "מאריך את העבודה הטכנית עצמה.")
    el += [data_table(
        ["שלב", "מוקד", "תוצרים"],
        [
            ["שבוע 1 — תשתית",
             "נעילת תשובות הבירור, פתיחת חשבון Shopify Basic, בחירת ערכת עיצוב "
             "ומיתוג, דומיין ואמצעי תשלום",
             "שלד חנות מעוצב, הזמנת בדיקה"],
            ["שבוע 2 — קטלוג ואוטומציה",
             "ייבוא מלאי מ-CSV, קולקציות לפי מותג, תהליכי Zapier להזמנות "
             "והתראות, תבניות וואטסאפ ומייל",
             "קטלוג מלא, אוטומציות עובדות מקצה לקצה"],
            ["שבוע 3 — ליטוש והשקה (חיץ)",
             "בדיקות בנייד, כללי צ׳קאאוט ומשלוח, חיבור Instagram Shopping, "
             "הדרכת צוות, עלייה לאוויר",
             "צ׳קליסט השקה מאושר, החנות חיה"],
        ],
        [W * 0.34, W * 0.42, W * 0.24])]
    el += muted("בהנחה שתמונות ומחירי מוצרים זמינים עד סוף שבוע 1 — שאלות "
                "הבירור בעמוד האחרון נועדו בדיוק כדי להקטין את הסיכון הזה.")

    # ---- 3. stack
    el += [Spacer(1, 8)]
    el += kicker("03 · המלצה על מערך טכנולוגי")
    el += h1("Shopify Basic ועוד Zapier: מערך קטן, מינוף גדול")
    el += [rule()]
    el += body("לבוטיק בסניף אחד עם קטלוג מוקפד, המערך הנכון הוא הקטן ביותר "
               "שעושה את העבודה. אני ממליץ על Shopify Basic כחנות ועל Zapier "
               "כדבק האוטומציה — שניהם מוכחים, שניהם ניתנים להחלפה, ואף אחד "
               "מהם לא דורש מפתח בריטיינר.")
    el += [data_table(
        ["רכיב", "בחירה", "למה דווקא זה"],
        [
            ["חנות", "Shopify Basic",
             "צ׳קאאוט מהטובים בשוק, אינטגרציה מובנית לאינסטגרם ולפייסבוק, "
             "ערכות עיצוב שתומכות בעברית ובכיווניות ימין-לשמאל, וייבוא "
             "מוצרים מ-CSV מובנה."],
            ["אוטומציה", "Zapier",
             "מחבר את Shopify לוואטסאפ, למייל ול-Google Sheets בלי קוד. "
             "הצוות רואה ועורך כל תהליך — בלי קופסאות שחורות."],
            ["מייל", "Shopify Email",
             "כלול ב-Shopify (10,000 שליחות בחודש חינם). מטפל בקבלות, "
             "עדכוני משלוח וקמפיינים פשוטים. Klaviyo הוא מסלול השדרוג."],
            ["וואטסאפ", "WhatsApp Business",
             "אפליקציה חינמית להתחלה; ספק Business API (כמו Wati או Twilio) "
             "אפשר להוסיף בהמשך להודעות אוטומטיות בהיקף גדול."],
            ["גשר מלאי", "Google Sheets ו-CSV",
             "הצוות עורך גיליון מוכר; קובץ CSV נקי זורם לתוך Shopify. "
             "אין תוכנה חדשה ללמוד."],
        ],
        [W * 0.46, W * 0.28, W * 0.26])]

    # ---- 4. costs
    el += [PageBreak()]
    el += kicker("04 · פירוט עלויות חודשיות")
    el += h1("עלויות תפעול, מפורטות")
    el += [rule()]
    el += body("עלויות ספקים חיצוניים אחרי ההשקה. המחירים הם מחירון נוכחי "
               "בדולרים וכדאי לאמת אותם מחדש במעמד החתימה; חיוב חודשי יקר "
               "מעט יותר מחיוב שנתי.")
    el += [data_table(
        ["ספק / שירות", "תוכנית", "חודשי (USD)", "הערות"],
        [
            ["Shopify", "Basic (חיוב שנתי)", "$29",
             "$39 בחיוב חודשי. כולל אחסון, SSL, צ׳קאאוט ו-Shopify Email "
             "בשכבה החינמית."],
            ["Zapier", "Professional (חיוב שנתי)", "$19.99",
             "מכסה תהליכים רב-שלביים להזמנות והודעות. השכבה החינמית מספיקה "
             "לתקופת הבנייה."],
            ["דומיין", "rosabellatlv.com (או co.il.)", "~$1.25",
             "כ-$15 לשנה, בחיוב שנתי."],
            ["אפליקציית WhatsApp Business", "—", "$0",
             "שכבה חינמית להתחלה; תגובות מהירות ותוויות."],
            ["אופציונלי: ספק WhatsApp API", "Wati / Twilio, אם רוצים אוטומציה מלאה",
             "$0–49", "רק אם נפח ההודעות גדל מעבר לאפליקציה החינמית."],
            ["אופציונלי: Klaviyo", "שכבה חינמית עד 250 אנשי קשר", "$0–20",
             "מסלול שדרוג לקמפיינים ותהליכים מעבר ל-Shopify Email."],
            ["סה״כ ליבה", "", "≈ $50 לחודש",
             "≈ $99–119 לחודש עם שתי התוספות האופציונליות"],
        ],
        [W * 0.32, W * 0.16, W * 0.28, W * 0.24],
        highlight_last=True)]
    el += muted("אלה חשבונות של רוזבלה, המשולמים ישירות לכל ספק — הם לא עוברים "
                "דרכי ואינם חלק מחלוקת ההכנסות שמתוארת בפרק הבא. עמלות סליקה "
                "של Shopify (אחוז לכל עסקה, משתנה לפי ספק ואזור) נמצאות מחוץ "
                "לטבלה הזו.")

    # ---- 5. commercial model
    el += [PageBreak()]
    el += kicker("05 · המודל העסקי")
    el += h1("בלי דמי בנייה — 25% ממה שהחנות מכניסה")
    el += [rule()]
    el += body("אני לא גובה על זמן. אין תעריף שעתי, אין תעריף יומי ואין דמי "
               "בנייה קבועים. אני מעצב, בונה ומשיק את החנות על חשבוני, ובתמורה "
               "אני מקבל 25% מההכנסות שהחנות מייצרת. אם האתר לא מכניס — אני לא "
               "מרוויח.")
    el += h2("איך מחושבים ה-25%")
    el += [data_table(
        ["סעיף", "הגדרה"],
        [
            ["בסיס", "הכנסות נטו מהזמנות שבוצעו דרך חנות ה-Shopify."],
            ["נקודת מוצא",
             "אפס. לרוזבלה אין היום מכירות מקוונות, ולכן כל הזמנה שהאתר קולט "
             "היא הכנסה חדשה. מכירות בחנות הפיזית ולקוחות מזדמנים מוחרגים "
             "לחלוטין — אין לי שום תביעה על המסחר הקיים של הבוטיק."],
            ["מנוכה תחילה",
             "מע״מ, דמי משלוח שנגבו מהלקוח, עמלות סליקה, וכל זיכוי, החזרה או "
             "ביטול חיוב. החלוקה חלה על מה שבאמת נכנס בפועל."],
            ["חלקי", "25% מהסכום נטו. רוזבלה שומרת 75%."],
            ["מקור אמת",
             "דוחות הניהול של Shopify עצמה — אותה מערכת ששנינו רואים. בלי "
             "הנהלת חשבונות נפרדת ובלי שרוזבלה תצטרך להכין דבר."],
            ["תשלום",
             "חודשי, עד 14 יום מתום החודש, על הזמנות שנסלקו בחודש הקודם."],
            ["תקופה",
             "ללא מועד סיום. השותפות רצה כל עוד החנות פעילה — זו שותפות "
             "בנכס שנבנה, לא פרויקט עם תאריך סיום."],
            ["מכירת החלק",
             "מהחודש ה-12 ואילך אני רשאי למכור או להעביר את חלקי — לרוזבלה "
             "עצמה או לצד שלישי. לרוזבלה תהיה זכות סירוב ראשונה: כל הצעה "
             "חיצונית תוצג לה תחילה, ותהיה לה תקופה של 30 יום להשוות אותה "
             "ולרכוש את החלק בעצמה."],
        ],
        [W * 0.76, W * 0.24])]

    el += [Spacer(1, 6)]
    el += h2("איך החלוקה נראית בפועל")
    el += [data_table(
        ["הכנסה חודשית נטו מהאתר", "החלק שלי (25%)", "מה שנשאר לרוזבלה"],
        [
            ["₪10,000", "₪2,500", "₪7,500"],
            ["₪25,000", "₪6,250", "₪18,750"],
            ["₪50,000", "₪12,500", "₪37,500"],
            ["₪100,000", "₪25,000", "₪75,000"],
        ],
        [W * 0.32, W * 0.30, W * 0.38])]
    el += muted("מספרים להמחשה בלבד, לא תחזית — הנפח בפועל תלוי בגודל הקטלוג, "
                "בתמחור ובשיעור ההמרה של קהל האינסטגרם.")

    el += [Spacer(1, 6)]
    el += h2("למה המודל הזה, בכנות")
    el += bullets([
        "רוזבלה לא נושאת בסיכון הבנייה. אין תשלום על העיצוב, הבנייה או ההשקה. "
        "אם החנות לא תצליח, ההוצאה היחידה היא כ-50 דולר בחודש על מנויי ספקים.",
        "האינטרס שלי זהה לשלכם. אני מקבל תשלום רק כשהחנות מוכרת, ולכן יש לי "
        "תמריץ להמשיך לשפר את ההמרה ולא למסור מפתחות ולהיעלם.",
        "התמורה ההוגנת לכך: זו שותפות ארוכת טווח ולא תשלום חד-פעמי. אם החנות "
        "תצליח מאוד, 25% לאורך זמן יסתכמו ביותר מדמי בנייה קבועים. זה המחיר "
        "של כך שאני נושא בסיכון הבנייה במקום רוזבלה — ובדיוק מהסיבה הזו קיימת "
        "זכות הסירוב הראשונה, שמאפשרת לרוזבלה לרכוש את החלק ולסגור את הנושא.",
    ])

    el += [Spacer(1, 6)]
    el += h2("מה כלול לאורך כל התקופה")
    el += body("החלק המתמשך אינו פסיבי. כל עוד השותפות רצה אני מתחזק את החנות: "
               "תיקוני אוטומציה, התאמות בערכת העיצוב ובצ׳קאאוט, תמיכה בקטלוג "
               "ובייבוא, וסקירה חודשית של מה שממיר ומה שלא. בלי חשבונית תמיכה "
               "נפרדת.")
    el += muted("התנאים המסחריים כאן הם מסגרת מוסכמת, ויעוגנו בהסכם משפטי "
                "מסודר לפני תחילת העבודה. מומלץ שכל צד יעביר אותו לעורך דין "
                "מטעמו.")

    # ---- 6. inventory
    el += [Spacer(1, 10)]
    el += kicker("06 · אסטרטגיית מלאי")
    el += h1("CSV קודם כול, עם גיבוי של סקרייפינג")
    el += [rule()]
    el += h2("המסלול הראשי: תהליך ה-CSV")
    el += body("Shopify מייבאת מוצרים מקובץ CSV באופן מובנה. אקים תבנית אב "
               "ב-Google Sheets — שורה לכל וריאנט מוצר עם מותג, מידה, צבע, "
               "מחיר, מלאי וקישור לתמונה — ממופה בדיוק לפורמט הייבוא של "
               "Shopify. השגרה השבועית הופכת ל: לעדכן את הגיליון, לייצא, "
               "לייבא. עשר דקות, בלי ידע טכני, והגיליון משמש גם כספר המלאי "
               "של הבוטיק.")
    el += body("במקומות שבהם קיימים גיליונות ספק (PDF או Excel), אבנה ממירים "
               "קטנים בעזרת AI שהופכים אותם לשורות ייבוא נקיות — בדיוק סוג "
               "האוטומציה הלא זוהרת שבשבילה קיים בונה AI עצמאי.")
    el += h2("גיבוי: סקרייפינג מובנה, אם יידרש")
    el += body("אם חלק מהמותגים לא מספקים נתוני מוצר שמישים — בלי גיליונות "
               "ובלי אקסלים — אוכל לבנות סקרייפרים ממוקדים לעמודי הקטלוג "
               "הפומביים של אותם מותגים, כדי לחלץ כותרות, תיאורים, מפרטים "
               "ותמונות לאותו פורמט CSV. הערות היקף:")
    el += bullets([
        "משמש רק כגיבוי, לכל מותג בנפרד, היכן שאין מקור נקי יותר.",
        "מוגבל לעמודים פומביים ולנתוני מוצר — בלי מחירי מתחרים ובלי מידע "
        "אישי כלשהו.",
        "כפוף לתנאי השימוש של כל אתר; אם מותג אוסר זאת במפורש, נחזור להזנה "
        "ידנית לאותו מותג.",
        "התוצר תמיד נבדק ידנית לפני שהוא עולה לאתר.",
    ])

    # ---- 7. communication
    el += [PageBreak()]
    el += kicker("07 · תקשורת עם לקוחות")
    el += h1("וואטסאפ ומייל, אוטומטיים אבל אישיים")
    el += [rule()]
    el += body("בישראל וואטסאפ הוא ערוץ ברירת המחדל, והמייל הוא הגיבוי "
               "הרשמי. שני הערוצים מחוברים לאירועי Shopify דרך Zapier, כך "
               "שאף לקוח לא נשאר בלי מענה — ובלי שמישהו בצוות יצטרך לזכור "
               "לשלוח.")
    el += [data_table(
        ["טריגר (Shopify)", "ערוץ", "הודעה אוטומטית"],
        [
            ["הזמנה בוצעה", "מייל",
             "אישור ממותג עם סיכום ההזמנה (מובנה ב-Shopify)."],
            ["הזמנה בוצעה", "וואטסאפ",
             "תודה אישית עם אפשרויות איסוף או משלוח, דרך Zapier."],
            ["הזמנה נשלחה / מוכנה", "וואטסאפ ומייל",
             "קישור מעקב או הודעה ׳מוכן לאיסוף בשבזי 31׳."],
            ["3 ימים לאחר מסירה", "וואטסאפ",
             "בדיקה קצרה שהפריט התאים ובקשה עדינה לביקורת."],
            ["עגלה נטושה", "מייל",
             "תזכורת עם הפריטים שנשארו מאחור (מובנה ב-Shopify)."],
            ["חדש במלאי (שבועי)", "מייל",
             "קמפיין קצר בסגנון לוקבוק שמשקף את פיד האינסטגרם."],
            ["לקוח משיב בוואטסאפ", "וואטסאפ (אנושי)",
             "מנותב לטלפון של הבוטיק — האוטומציה פותחת את השיחה, אדם ממשיך אותה."],
        ],
        [W * 0.46, W * 0.22, W * 0.32])]
    el += muted("כל הודעה אוטומטית נכתבת פעם אחת, בשפה של רוזבלה, במהלך שבוע "
                "2 — בגרסה עברית ואנגלית היכן שרלוונטי. שום דבר לא נשלח בלי "
                "שהצוות אישר אותו.")

    # ---- 8. next steps
    el += [Spacer(1, 8)]
    el += kicker("08 · השלבים הבאים")
    el += h1("הדרך להשקה באפריל")
    el += [rule()]
    el += body("בעבודה לאחור מעלייה לאוויר בשבוע הראשון של אפריל, כשהבנייה "
               "בת השבועיים-שלושה ממוקמת כך שיישאר אוויר לנשימה:")
    el += [data_table(
        ["מתי", "אבן דרך", "אחריות"],
        [
            ["השבוע", "רוזבלה עוברת על ההצעה; שיחה של 30 דקות למעבר על שאלות "
             "הבירור (בעמוד הבא).", "שנינו"],
            ["תוך שבוע", "התשובות ביד — תנאי השותפות וההיקף נחתמים. נפתחים "
             "חשבונות Shopify ודומיין.", "אלן"],
            ["תחילת–אמצע מרץ", "שבועות בנייה 1–2: חנות, ייבוא קטלוג, "
             "אוטומציות.", "אלן"],
            ["אמצע–סוף מרץ", "שבוע 3 כחיץ: בדיקות, הדרכה, השלמת תוכן.",
             "שנינו"],
            ["השבוע האחרון של מרץ", "השקה רכה לרשימה קטנה; הזמנות בדיקה מקצה "
             "לקצה.", "שנינו"],
            ["השבוע הראשון של אפריל",
             "השקה פומבית, בהכרזה לקהל 11 האלף באינסטגרם.", "שנינו"],
        ],
        [W * 0.20, W * 0.58, W * 0.22])]
    el += body("ההשקה היא תחילת השותפות, לא סופה. מכאן ואילך אני ממשיך לתחזק "
               "את החנות ולשפר את ההמרה שלה ללא תשלום נוסף. חשבון ה-Shopify, "
               "הדומיין, רשימת הלקוחות והקטלוג הם רכושה של רוזבלה לאורך כל "
               "הדרך, ונשארים שלה גם אם השותפות מסתיימת.")

    # ---- 9. discovery
    el += [PageBreak()]
    el += kicker("09 · לפני שמתחילים")
    el += h1("חמש שאלות בירור לרוזבלה")
    el += [rule()]
    el += body("התשובות לחמש השאלות האלה הופכות את ההצעה הזו להסכם חתום "
               "וללוח זמנים נעול:")
    qs = [
        ("1. באיזו מערכת מלאי הבוטיק משתמש היום?",
         "תוכנת קופה, גיליון אלקטרוני או נייר — זה קובע איך גיליון האב "
         "ב-CSV מתמלא בפעם הראשונה והאם אפשר לאוטמט סנכרון שוטף."),
        ("2. האם הספקים שלכם מספקים גיליונות מוצר?",
         "גיליונות מותג (PDF או Excel עם מוצרים, מק״טים, מחירים ותמונות) "
         "חוסכים הזנה ידנית — ומגלים אם בכלל נדרש גיבוי הסקרייפינג."),
        ("3. מהו תאריך ההשקה המבוקש?",
         "ההצעה מניחה את השבוע הראשון של אפריל — אישור התאריך המדויק (וכל "
         "אירוע שקשור אליו) נועל את לוח הזמנים."),
        ("4. כמה פוסטים באינסטגרם יוצאים בשבוע היום?",
         "נפח הפרסום הנוכחי מלמד כמה תוכן מוכן לקנייה קיים ביום הראשון ואיך "
         "לתזמן את מייל ׳חדש במלאי׳ שמשקף את הפיד."),
        ("5. מהן מגבלות התקציב?",
         "הבנייה עצמה לא עולה דבר מראש, ולכן מדובר בעיקר בכ-50 דולר בחודש "
         "על מנויי ספקים ובשאלה האם התוספות האופציונליות (WhatsApp API, "
         "Klaviyo) אפשריות עכשיו או בהמשך — וכן האם מודל של שותפות ארוכת "
         "טווח ב-25% הוא מבנה שהעסק מרגיש בנוח איתו."),
    ]
    for q, why in qs:
        el += rtl(q, "He-Bold", 10.5, 15, INK, SAFE, space_after=1)
        el += rtl(why, "He", 9, 14, SLATE, SAFE, space_after=6)
    el += [Spacer(1, 10), signoff()]

    doc.build(el)
    print(f"Wrote {OUT}")


if __name__ == "__main__":
    build()
