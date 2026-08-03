#!/usr/bin/env python3
"""Rosabella TLV proposal, rendered in the NZP house format.

The reference (Neve Tzedek Properties work plan) was a Chrome print-to-PDF of
an HTML page — Skia/PDF producer, Arial only. This rebuilds that pipeline
rather than approximating it in ReportLab, which also gets native RTL for
free instead of hand-rolled bidi.

Design tokens were sampled from the reference render, not guessed:
  gold band / section rules  #ECCF9F      header band       #0B0805
  callout accent (right)     #C98F5E      callout fill      #FAF5EC
  table header rule          #D9CBB8      table hairline    #EFE7DA
  title ink                  #1A130D      muted body        #6B5D4F
  content margin 25pt · gold band 3pt · header band 46pt · Arial metrics

Usage:  python3 generate_proposal_nzp_format.py
        -> Rosabella-TLV-Proposal-NZP-Format.pdf
"""

import html
import os
import subprocess
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "Rosabella-TLV-Proposal-NZP-Format.pdf")

CLIENT = "Rosabella TLV"
DATE = "03.08.2026"

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


# ---------------------------------------------------------------- helpers
def e(s):
    return html.escape(s, quote=False)


def section(title, *blocks):
    return (f'<h2 class="sec">{e(title)}</h2>\n' + "\n".join(blocks))


def para(text, cls="body"):
    return f'<p class="{cls}">{text}</p>'


def callout(text):
    return f'<div class="callout">{text}</div>'


def table(headers, rows, widths=None, first_bold=True):
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
    return (f'<table>{cols}<thead><tr>{head}</tr></thead>'
            f"<tbody>{body}</tbody></table>")


def bullets(items):
    li = "".join(f"<li>{t}</li>" for t in items)
    return f"<ul>{li}</ul>"


def b(t):
    return f"<b>{e(t)}</b>"


def ltr(t):
    """Isolate a Latin/numeric run so RTL reordering leaves it intact."""
    return f'<span dir="ltr">{e(t)}</span>'


# ---------------------------------------------------------------- content
def content():
    el = []

    el.append('<h1 class="title">הצעת עבודה — הקמת חנות אונליין</h1>')
    el.append('<p class="subtitle">רוזבלה תל אביב · בוטיק אופנה רב-מותגי, '
              'שבזי 31, נווה צדק · 11,000 עוקבים באינסטגרם. '
              'הצעה לבנייה, השקה ותפעול של חנות מקוונת — ללא תשלום מראש.</p>')

    # 1
    el.append(section(
        "סקירת הפרויקט",
        callout(
            "לרוזבלה יש בדיוק את מה שרוב החנויות המקוונות רודפות אחריו שנים: "
            "קולקציה רב-מותגית מוקפדת, חנות באחד הרחובות המהלכים בתל אביב, "
            "וקהל של 11,000 עוקבים. " + b("מה שחסר הוא דרך לקהל הזה לקנות "
            "כשהוא לא עומד ברחוב שבזי.") + " ההצעה מכסה עיצוב, בנייה והשקה של "
            "חנות Shopify עם אוטומציות קלות, כך שהתפעול היומיומי יישאר פשוט "
            "לצוות קטן — " + b("ללא דמי בנייה וללא ריטיינר") + "."),
        '<h3>איך נראית הצלחה</h3>',
        bullets([
            b("חנות חיה, מותאמת קודם כול לנייד") + " — נושאת את המראה של הבוטיק.",
            b("אינסטגרם שמוכר") + " — קישורים מהביו ומהפוסטים ישירות לעמודי מוצר.",
            b("מלאי אמין") + " — מסונכרן בתהליך CSV פשוט שהצוות מריץ פעם בשבוע.",
            b("תקשורת אוטומטית") + " — הודעות הזמנה, משלוח ומעקב בוואטסאפ ובמייל.",
            b("עצמאות תפעולית") + " — הצוות מפעיל את החנות ביומיום, ואני "
            "ממשיך לתחזק ולשפר ברקע.",
        ])))

    # 2
    el.append(section(
        "לוח זמנים",
        para("הבנייה מתומחרת לשבועיים עד שלושה מקצה לקצה. שבוע 3 הוא חיץ: "
             "הוא סופג עיכובי תוכן (צילומים, טקסטים, נתוני ספקים) ולא מאריך "
             "את העבודה הטכנית עצמה."),
        table(["שלב", "מה עושים", "משך"],
              [["שבוע 1 · תשתית",
                "נעילת תשובות הבירור · פתיחת חשבון Shopify Basic · בחירת ערכת "
                "עיצוב ומיתוג · דומיין ואמצעי תשלום · הזמנת בדיקה",
                "~5 ימים"],
               ["שבוע 2 · קטלוג ואוטומציה",
                "ייבוא מלאי מ-CSV · קולקציות לפי מותג · תהליכי Zapier להזמנות "
                "והתראות · תבניות וואטסאפ ומייל",
                "~5 ימים"],
               ["שבוע 3 · ליטוש והשקה",
                "בדיקות בנייד · כללי צ׳קאאוט ומשלוח · חיבור Instagram "
                "Shopping · הדרכת צוות · עלייה לאוויר",
                "חיץ"]],
              ["26%", "58%", "16%"]),
        para("בהנחה שתמונות ומחירי מוצרים זמינים עד סוף שבוע 1 — שאלות הבירור "
             "בסוף המסמך נועדו להקטין בדיוק את הסיכון הזה.", "note")))

    # 3
    el.append(section(
        "מערך טכנולוגי",
        para("לבוטיק בסניף אחד עם קטלוג מוקפד, המערך הנכון הוא הקטן ביותר "
             "שעושה את העבודה — מוכח, ניתן להחלפה, ולא דורש מפתח בריטיינר."),
        table(["רכיב", "בחירה", "למה דווקא זה"],
              [["חנות", "Shopify Basic",
                "צ׳קאאוט מהטובים בשוק · אינטגרציה מובנית לאינסטגרם ולפייסבוק · "
                "ערכות עיצוב שתומכות בעברית ובימין-לשמאל · ייבוא CSV מובנה"],
               ["אוטומציה", "Zapier",
                "מחבר את Shopify לוואטסאפ, למייל ול-Google Sheets בלי קוד. "
                "הצוות רואה ועורך כל תהליך — בלי קופסאות שחורות"],
               ["מייל", "Shopify Email",
                "כלול (10,000 שליחות בחודש חינם) · קבלות, עדכוני משלוח "
                "וקמפיינים פשוטים · Klaviyo הוא מסלול השדרוג"],
               ["וואטסאפ", "WhatsApp Business",
                "אפליקציה חינמית להתחלה · ספק Business API אפשר להוסיף "
                "בהמשך להודעות בהיקף גדול"],
               ["גשר מלאי", "Google Sheets + CSV",
                "הצוות עורך גיליון מוכר, קובץ נקי זורם ל-Shopify. "
                "אין תוכנה חדשה ללמוד"]],
              ["16%", "20%", "64%"])))

    # 4
    el.append(section(
        "עלויות חודשיות",
        para("עלויות ספקים חיצוניים אחרי ההשקה. אלה חשבונות של רוזבלה, "
             "המשולמים ישירות לכל ספק — הם אינם עוברים דרכי ואינם חלק "
             "מחלוקת ההכנסות."),
        table(["ספק", "תוכנית", "חודשי", "הערות"],
              [["Shopify", "Basic (חיוב שנתי)", ltr("$29"),
                "$39 בחיוב חודשי · כולל אחסון, SSL וצ׳קאאוט"],
               ["Zapier", "Professional", ltr("$19.99"),
                "השכבה החינמית מספיקה לתקופת הבנייה"],
               ["דומיין", "rosabellatlv.com", ltr("~$1.25"), "כ-$15 לשנה"],
               ["WhatsApp Business", "אפליקציה", ltr("$0"),
                "תגובות מהירות ותוויות"],
               ["<i>אופציונלי</i> · WhatsApp API", "Wati / Twilio", ltr("$0–49"),
                "רק אם נפח ההודעות גדל"],
               ["<i>אופציונלי</i> · Klaviyo", "עד 250 אנשי קשר", ltr("$0–20"),
                "לקמפיינים מעבר ל-Shopify Email"]],
              ["24%", "24%", "13%", "39%"]),
        para("סה״כ ליבה: <b>כ-$50 לחודש</b> · כ-$99–119 עם שתי התוספות. "
             "עמלות סליקה של Shopify (אחוז לכל עסקה) נמצאות מחוץ לטבלה.",
             "note")))

    # 5 — commercial model
    el.append(section(
        "המודל העסקי",
        callout(
            "אני לא גובה על זמן. " + b("אין תעריף שעתי, אין תעריף יומי ואין "
            "דמי בנייה.") + " אני מעצב, בונה ומשיק את החנות על חשבוני, "
            "ובתמורה מקבל " + b("25% מההכנסות שהחנות מייצרת") + ". "
            "אם האתר לא מכניס — אני לא מרוויח."),
        table(["סעיף", "הגדרה"],
              [["בסיס", "הכנסות נטו מהזמנות שבוצעו דרך חנות ה-Shopify."],
               ["נקודת מוצא",
                "אפס. לרוזבלה אין היום מכירות מקוונות, ולכן כל הזמנה שהאתר "
                "קולט היא הכנסה חדשה. מכירות בחנות הפיזית מוחרגות לחלוטין — "
                "אין לי שום תביעה על המסחר הקיים של הבוטיק."],
               ["מנוכה תחילה",
                "מע״מ · דמי משלוח שנגבו מהלקוח · עמלות סליקה · זיכויים, "
                "החזרות וביטולי חיוב. החלוקה חלה על מה שנכנס בפועל."],
               ["חלוקה", "25% לי · 75% לרוזבלה."],
               ["מקור אמת",
                "דוחות הניהול של Shopify — אותה מערכת ששנינו רואים. בלי "
                "הנהלת חשבונות נפרדת ובלי שרוזבלה תצטרך להכין דבר."],
               ["תשלום",
                "חודשי, עד 14 יום מתום החודש, על הזמנות שנסלקו בחודש הקודם."],
               ["תקופה",
                "<b>ללא מועד סיום.</b> השותפות רצה כל עוד החנות פעילה — "
                "שותפות בנכס שנבנה, לא פרויקט עם תאריך סיום."],
               ["מכירת החלק",
                "<b>מהחודש ה-12 ואילך אני רשאי למכור או להעביר את חלקי</b> — "
                "לרוזבלה עצמה או לצד שלישי. לרוזבלה זכות סירוב ראשונה: כל "
                "הצעה חיצונית תוצג לה תחילה, עם 30 יום להשוות אותה ולרכוש "
                "את החלק בעצמה."]],
              ["18%", "82%"]),
        '<h3>איך החלוקה נראית בפועל</h3>',
        table(["הכנסה חודשית נטו", "החלק שלי (25%)", "נשאר לרוזבלה"],
              [["₪10,000", "₪2,500", "₪7,500"],
               ["₪25,000", "₪6,250", "₪18,750"],
               ["₪50,000", "₪12,500", "₪37,500"],
               ["₪100,000", "₪25,000", "₪75,000"]],
              ["34%", "33%", "33%"], first_bold=False),
        para("מספרים להמחשה בלבד, לא תחזית — הנפח בפועל תלוי בגודל הקטלוג, "
             "בתמחור ובשיעור ההמרה של קהל האינסטגרם.", "note"),
        '<h3>למה המודל הזה, בכנות</h3>',
        bullets([
            b("רוזבלה לא נושאת בסיכון הבנייה") + " — אין תשלום על העיצוב, "
            "הבנייה או ההשקה. אם החנות לא תצליח, ההוצאה היחידה היא כ-50 דולר "
            "בחודש על מנויי ספקים.",
            b("האינטרס שלי זהה לשלכם") + " — אני מקבל תשלום רק כשהחנות מוכרת, "
            "ולכן יש לי תמריץ להמשיך לשפר את ההמרה ולא למסור מפתחות ולהיעלם.",
            b("התמורה ההוגנת") + " — זו שותפות ארוכת טווח ולא תשלום חד-פעמי. "
            "אם החנות תצליח מאוד, 25% לאורך זמן יסתכמו ביותר מדמי בנייה "
            "קבועים. זה המחיר של כך שאני נושא בסיכון במקום רוזבלה — ולכן "
            "קיימת זכות הסירוב הראשונה, שמאפשרת לה לרכוש את החלק ולסגור "
            "את הנושא.",
        ]),
        '<h3>מה כלול לאורך כל התקופה</h3>',
        para("החלק המתמשך אינו פסיבי. כל עוד השותפות רצה אני מתחזק את החנות: "
             "תיקוני אוטומציה, התאמות בעיצוב ובצ׳קאאוט, תמיכה בקטלוג ובייבוא, "
             "וסקירה חודשית של מה שממיר ומה שלא — בלי חשבונית תמיכה נפרדת."),
        para("התנאים המסחריים כאן הם מסגרת מוסכמת, ויעוגנו בהסכם משפטי מסודר "
             "לפני תחילת העבודה. מומלץ שכל צד יעביר אותו לעורך דין מטעמו.",
             "note")))

    # 6
    el.append(section(
        "אסטרטגיית מלאי",
        '<h3>המסלול הראשי: תהליך ה-CSV</h3>',
        para("Shopify מייבאת מוצרים מקובץ CSV באופן מובנה. אקים תבנית אב "
             "ב-Google Sheets — שורה לכל וריאנט עם מותג, מידה, צבע, מחיר, "
             "מלאי וקישור לתמונה — ממופה בדיוק לפורמט הייבוא. השגרה השבועית: "
             "לעדכן, לייצא, לייבא. עשר דקות, בלי ידע טכני, והגיליון משמש גם "
             "כספר המלאי של הבוטיק."),
        para("היכן שקיימים גיליונות ספק (PDF או Excel), אבנה ממירים קטנים "
             "בעזרת AI שהופכים אותם לשורות ייבוא נקיות."),
        '<h3>גיבוי: סקרייפינג מובנה, אם יידרש</h3>',
        para("אם חלק מהמותגים לא מספקים נתוני מוצר שמישים, אוכל לבנות "
             "סקרייפרים ממוקדים לעמודי הקטלוג הפומביים שלהם ולחלץ כותרות, "
             "תיאורים, מפרטים ותמונות לאותו פורמט CSV. הערות היקף:"),
        bullets([
            "משמש רק כגיבוי, לכל מותג בנפרד, היכן שאין מקור נקי יותר.",
            "מוגבל לעמודים פומביים ולנתוני מוצר — בלי מחירי מתחרים ובלי מידע אישי.",
            "כפוף לתנאי השימוש של כל אתר; אם מותג אוסר זאת, נחזור להזנה ידנית.",
            "התוצר תמיד נבדק ידנית לפני שהוא עולה לאתר.",
        ])))

    # 7
    el.append(section(
        "תקשורת עם לקוחות",
        para("בישראל וואטסאפ הוא ערוץ ברירת המחדל והמייל הוא הגיבוי הרשמי. "
             "שני הערוצים מחוברים לאירועי Shopify דרך Zapier, כך שאף לקוח לא "
             "נשאר בלי מענה — ובלי שמישהו בצוות צריך לזכור לשלוח."),
        table(["טריגר", "ערוץ", "הודעה"],
              [["הזמנה בוצעה", "מייל", "אישור ממותג עם סיכום ההזמנה"],
               ["הזמנה בוצעה", "וואטסאפ",
                "תודה אישית עם אפשרויות איסוף או משלוח"],
               ["נשלחה / מוכנה", "וואטסאפ + מייל",
                "קישור מעקב או ׳מוכן לאיסוף בשבזי 31׳"],
               ["3 ימים לאחר מסירה", "וואטסאפ",
                "בדיקה שהפריט התאים ובקשה עדינה לביקורת"],
               ["עגלה נטושה", "מייל", "תזכורת עם הפריטים שנשארו מאחור"],
               ["חדש במלאי (שבועי)", "מייל",
                "קמפיין קצר בסגנון לוקבוק שמשקף את פיד האינסטגרם"],
               ["לקוח משיב", "וואטסאפ (אנושי)",
                "מנותב לטלפון של הבוטיק — האוטומציה פותחת, אדם ממשיך"]],
              ["24%", "20%", "56%"]),
        para("כל הודעה נכתבת פעם אחת, בשפה של רוזבלה, במהלך שבוע 2 — בעברית "
             "ובאנגלית היכן שרלוונטי. שום דבר לא נשלח בלי שהצוות אישר אותו.",
             "note")))

    # 8
    el.append(section(
        "השלבים הבאים",
        table(["מתי", "אבן דרך", "אחריות"],
              [["השבוע", "מעבר על ההצעה · שיחה של 30 דקות לשאלות הבירור",
                "שנינו"],
               ["תוך שבוע", "תנאי השותפות וההיקף נחתמים · פתיחת חשבונות",
                "אלן"],
               ["תחילת–אמצע מרץ", "שבועות בנייה 1–2: חנות, קטלוג, אוטומציות",
                "אלן"],
               ["אמצע–סוף מרץ", "שבוע חיץ: בדיקות, הדרכה, השלמת תוכן",
                "שנינו"],
               ["סוף מרץ", "השקה רכה לרשימה קטנה · הזמנות בדיקה", "שנינו"],
               ["השבוע הראשון של אפריל",
                "<b>השקה פומבית, בהכרזה לקהל 11 האלף באינסטגרם</b>",
                "שנינו"]],
              ["22%", "62%", "16%"]),
        para("ההשקה היא תחילת השותפות, לא סופה. חשבון ה-Shopify, הדומיין, "
             "רשימת הלקוחות והקטלוג הם רכושה של רוזבלה לאורך כל הדרך, "
             "ונשארים שלה גם אם השותפות מסתיימת.")))

    # 9
    el.append(section(
        "מה נדרש כדי להתחיל",
        para("התשובות לחמש השאלות האלה הופכות את ההצעה להסכם חתום וללוח "
             "זמנים נעול:"),
        bullets([
            b("באיזו מערכת מלאי הבוטיק משתמש היום?") + " — קופה, גיליון או "
            "נייר. זה קובע איך גיליון האב מתמלא בפעם הראשונה והאם אפשר "
            "לאוטמט סנכרון שוטף.",
            b("האם הספקים מספקים גיליונות מוצר?") + " — גיליונות מותג "
            "(PDF או Excel עם מוצרים, מק״טים, מחירים ותמונות) חוסכים הזנה "
            "ידנית, ומגלים אם בכלל נדרש גיבוי הסקרייפינג.",
            b("מהו תאריך ההשקה המבוקש?") + " — ההצעה מניחה את השבוע הראשון "
            "של אפריל. אישור התאריך המדויק נועל את לוח הזמנים.",
            b("כמה פוסטים באינסטגרם יוצאים בשבוע?") + " — נפח הפרסום מלמד "
            "כמה תוכן מוכן לקנייה קיים ביום הראשון ואיך לתזמן את מייל "
            "׳חדש במלאי׳.",
            b("מהן מגבלות התקציב?") + " — הבנייה לא עולה דבר מראש, ולכן "
            "השאלה היא מנויי הספקים, התוספות האופציונליות, והאם מודל של "
            "שותפות ארוכת טווח ב-25% הוא מבנה שהעסק מרגיש בנוח איתו.",
        ])))

    return "\n".join(el)


# ---------------------------------------------------------------- template
CSS = """
@page { size: A4; margin: 0; }
* { box-sizing: border-box; }
html, body { margin: 0; padding: 0; }
body {
  font-family: 'Liberation Sans', Arial, Helvetica, sans-serif;
  font-size: 8.4pt; line-height: 1.55; color: #1A130D;
  direction: rtl; text-align: right;
  -webkit-print-color-adjust: exact; print-color-adjust: exact;
}

/* running header + footer repeat on every printed page */
.band  { position: fixed; top: 0; left: 0; right: 0; height: 3pt;
         background: #ECCF9F; }
.head  { position: fixed; top: 3pt; left: 0; right: 0; height: 46pt;
         background: #0B0805; padding: 9pt 25pt 0; }
.head .mark { float: left; text-align: left; }
.head .name { color: #ECCF9F; font-size: 10.5pt; letter-spacing: 3.2pt; }
.head .tag  { color: #958F87; font-size: 6.6pt; letter-spacing: 2.1pt;
              margin-top: 2pt; }
.head .who  { color: #A8A198; font-size: 7pt; line-height: 1.5; }
.foot  { position: fixed; bottom: 0; left: 0; right: 0; height: 30pt;
         padding: 6pt 25pt 0; border-top: 0.6pt solid #EFE7DA;
         color: #8A7F72; font-size: 6.6pt; }
.foot .l { float: left; text-align: left; direction: ltr; }

.page { padding: 66pt 25pt 40pt; }

h1.title { font-size: 15pt; line-height: 1.3; margin: 0 0 4pt;
           color: #1A130D; }
p.subtitle { color: #6B5D4F; font-size: 8pt; margin: 0 0 16pt;
             line-height: 1.6; }

h2.sec { font-size: 9.6pt; margin: 17pt 0 0; padding-bottom: 4pt;
         border-bottom: 1.2pt solid #ECCF9F; break-after: avoid; }
h3 { font-size: 8.6pt; margin: 12pt 0 4pt; break-after: avoid; }
p.body { margin: 8pt 0; }
p.note { color: #8A7F72; font-size: 7.4pt; margin: 6pt 0 0;
         line-height: 1.55; }

.callout { background: #FAF5EC; border-right: 3pt solid #C98F5E;
           padding: 9pt 12pt; margin: 10pt 0; line-height: 1.6;
           break-inside: avoid; }

table { width: 100%; border-collapse: collapse; margin: 9pt 0 0;
        font-size: 7.8pt; }
thead { display: table-header-group; }
th { text-align: right; font-weight: bold; color: #6B5D4F;
     font-size: 7.4pt; padding: 0 0 5pt; border-bottom: 1pt solid #D9CBB8; }
/* left-align the trailing column like the reference, but never flip its
   base direction — `direction: ltr` reorders mixed Hebrew runs and
   scrambles any cell holding prose rather than a bare duration. */
th:last-child, td:last-child { text-align: left; }
td { padding: 6pt 0; border-bottom: 0.6pt solid #EFE7DA;
     vertical-align: top; line-height: 1.5; }
th + th, td + td { padding-right: 10pt; }
td.lead { font-weight: bold; white-space: nowrap; }
tr { break-inside: avoid; }

ul { margin: 8pt 0; padding: 0 14pt 0 0; }
li { margin: 0 0 5pt; line-height: 1.6; }
li::marker { color: #C98F5E; }
b { font-weight: bold; }
i { font-style: italic; color: #8A7F72; }
"""


def build():
    doc = f"""<!doctype html>
<html lang="he" dir="rtl"><head><meta charset="utf-8">
<title>רוזבלה תל אביב — הצעת עבודה</title>
<style>{CSS}</style></head><body>
<div class="band"></div>
<div class="head">
  <div class="mark">
    <div class="name">ALAN COHEN</div>
    <div class="tag">AI CONSULTING</div>
  </div>
  <div class="who">{e(CLIENT)}<br>{e(DATE)}</div>
</div>
<div class="foot">
  <div class="l">Alan Cohen · AI Consulting · acohen.tlv@gmail.com</div>
  <div>הצעת עבודה · הוכנה עבור {e(CLIENT)} · {e(DATE)}</div>
</div>
<div class="page">
{content()}
</div>
</body></html>"""

    with tempfile.TemporaryDirectory() as td:
        src = os.path.join(td, "proposal.html")
        with open(src, "w", encoding="utf-8") as fh:
            fh.write(doc)
        subprocess.run(
            [find_chrome(), "--headless", "--disable-gpu", "--no-sandbox",
             "--no-pdf-header-footer", f"--print-to-pdf={OUT}", src],
            check=True, capture_output=True)
    print(f"Wrote {OUT}")


if __name__ == "__main__":
    build()
