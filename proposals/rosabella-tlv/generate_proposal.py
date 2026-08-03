#!/usr/bin/env python3
"""Rosabella TLV proposal — Hebrew and English, in the NZP house format.

The NZP reference (Neve Tzedek Properties work plan) was a Chrome
print-to-PDF of an HTML page — Skia/PDF producer, Arial only. This
reproduces that pipeline rather than approximating it in ReportLab, which
also yields native bidi instead of hand-rolled RTL line-breaking.

Design tokens were sampled from the reference render, not guessed:
  gold band / section rules  #ECCF9F      header band       #0B0805
  callout accent             #C98F5E      callout fill      #FAF5EC
  table header rule          #D9CBB8      table hairline    #EFE7DA
  title ink                  #1A130D      muted body        #6B5D4F
  content margin 25pt · gold band 3pt · header band 46pt · Arial metrics

Usage:  python3 generate_proposal.py
        -> Rosabella-TLV-Proposal-HE.pdf, Rosabella-TLV-Proposal-EN.pdf
"""

import html
import os
import subprocess
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
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
    return f'<h2 class="sec">{e(title)}</h2>\n' + "\n".join(blocks)


def para(text, cls="body"):
    return f'<p class="{cls}">{text}</p>'


def callout(text):
    return f'<div class="callout">{text}</div>'


def h3(text):
    return f"<h3>{e(text)}</h3>"


def table(headers, rows, widths=None, first_bold=True, flush_last=True):
    """flush_last mirrors the reference, where the trailing column is a short
    duration pinned to the far edge. Turn it off when that column carries
    prose — flushing it just leaves the paragraph ragged."""
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


def bullets(items):
    return "<ul>" + "".join(f"<li>{t}</li>" for t in items) + "</ul>"


def b(t):
    return f"<b>{e(t)}</b>"


def ltr(t):
    """Isolate a Latin/numeric run so RTL reordering leaves it intact."""
    return f'<span dir="ltr">{e(t)}</span>'


# ---------------------------------------------------------------- hebrew
def content_he():
    el = ['<h1 class="title">הצעת עבודה — הקמת חנות אונליין</h1>',
          '<p class="subtitle">רוזבלה תל אביב · בוטיק אופנה רב-מותגי, '
          'שבזי 31, נווה צדק · 11,000 עוקבים באינסטגרם. '
          'הצעה לבנייה, השקה ותפעול של חנות מקוונת — ללא תשלום מראש.</p>']

    el.append(section(
        "סקירת הפרויקט",
        callout(
            "לרוזבלה יש בדיוק את מה שרוב החנויות המקוונות רודפות אחריו שנים: "
            "קולקציה רב-מותגית מוקפדת, חנות באחד הרחובות המהלכים בתל אביב, "
            "וקהל של 11,000 עוקבים. " + b("מה שחסר הוא דרך לקהל הזה לקנות "
            "כשהוא לא עומד ברחוב שבזי.") + " ההצעה מכסה עיצוב, בנייה והשקה של "
            "חנות Shopify עם אוטומציות קלות, כך שהתפעול היומיומי יישאר פשוט "
            "לצוות קטן — " + b("ללא דמי בנייה וללא ריטיינר") + "."),
        h3("איך נראית הצלחה"),
        bullets([
            b("חנות חיה, מותאמת קודם כול לנייד") + " — נושאת את המראה של הבוטיק.",
            b("אינסטגרם שמוכר") + " — קישורים מהביו ומהפוסטים ישירות לעמודי מוצר.",
            b("מלאי אמין") + " — מסונכרן בתהליך CSV פשוט שהצוות מריץ פעם בשבוע.",
            b("תקשורת אוטומטית") + " — הודעות הזמנה, משלוח ומעקב בוואטסאפ ובמייל.",
            b("עצמאות תפעולית") + " — הצוות מפעיל את החנות ביומיום, ואני "
            "ממשיך לתחזק ולשפר ברקע.",
        ])))

    el.append(section(
        "לוח זמנים",
        para("הבנייה מתומחרת לשבועיים עד שלושה מקצה לקצה. שבוע 3 הוא חיץ: "
             "הוא סופג עיכובי תוכן (צילומים, טקסטים, נתוני ספקים) ולא מאריך "
             "את העבודה הטכנית עצמה."),
        table(["שלב", "מה עושים", "משך"],
              [["שבוע 1 · תשתית",
                "נעילת תשובות הבירור · פתיחת חשבון Shopify Basic · בחירת ערכת "
                "עיצוב ומיתוג · דומיין ואמצעי תשלום · הזמנת בדיקה", "~5 ימים"],
               ["שבוע 2 · קטלוג ואוטומציה",
                "ייבוא מלאי מ-CSV · קולקציות לפי מותג · תהליכי Zapier להזמנות "
                "והתראות · תבניות וואטסאפ ומייל", "~5 ימים"],
               ["שבוע 3 · ליטוש והשקה",
                "בדיקות בנייד · כללי צ׳קאאוט ומשלוח · חיבור Instagram "
                "Shopping · הדרכת צוות · עלייה לאוויר", "חיץ"]],
              ["26%", "58%", "16%"]),
        para("בהנחה שתמונות ומחירי מוצרים זמינים עד סוף שבוע 1 — שאלות הבירור "
             "בסוף המסמך נועדו להקטין בדיוק את הסיכון הזה.", "note")))

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
              ["16%", "20%", "64%"], flush_last=False)))

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
               ["<i>אופציונלי</i> · WhatsApp API", "Wati / Twilio",
                ltr("$0–49"), "רק אם נפח ההודעות גדל"],
               ["<i>אופציונלי</i> · Klaviyo", "עד 250 אנשי קשר",
                ltr("$0–20"), "לקמפיינים מעבר ל-Shopify Email"]],
              ["24%", "24%", "13%", "39%"], flush_last=False),
        para("סה״כ ליבה: <b>כ-$50 לחודש</b> · כ-$99–119 עם שתי התוספות. "
             "עמלות סליקה של Shopify (אחוז לכל עסקה) נמצאות מחוץ לטבלה.",
             "note")))

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
              ["18%", "82%"], flush_last=False),
        h3("איך החלוקה נראית בפועל"),
        table(["הכנסה חודשית נטו", "החלק שלי (25%)", "נשאר לרוזבלה"],
              [["₪10,000", "₪2,500", "₪7,500"],
               ["₪25,000", "₪6,250", "₪18,750"],
               ["₪50,000", "₪12,500", "₪37,500"],
               ["₪100,000", "₪25,000", "₪75,000"]],
              ["34%", "33%", "33%"], first_bold=False),
        para("מספרים להמחשה בלבד, לא תחזית — הנפח בפועל תלוי בגודל הקטלוג, "
             "בתמחור ובשיעור ההמרה של קהל האינסטגרם.", "note"),
        h3("למה המודל הזה, בכנות"),
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
        h3("מה כלול לאורך כל התקופה"),
        para("החלק המתמשך אינו פסיבי. כל עוד השותפות רצה אני מתחזק את החנות: "
             "תיקוני אוטומציה, התאמות בעיצוב ובצ׳קאאוט, תמיכה בקטלוג ובייבוא, "
             "וסקירה חודשית של מה שממיר ומה שלא — בלי חשבונית תמיכה נפרדת."),
        para("התנאים המסחריים כאן הם מסגרת מוסכמת, ויעוגנו בהסכם משפטי מסודר "
             "לפני תחילת העבודה. מומלץ שכל צד יעביר אותו לעורך דין מטעמו.",
             "note")))

    el.append(section(
        "אסטרטגיית מלאי",
        h3("המסלול הראשי: תהליך ה-CSV"),
        para("Shopify מייבאת מוצרים מקובץ CSV באופן מובנה. אקים תבנית אב "
             "ב-Google Sheets — שורה לכל וריאנט עם מותג, מידה, צבע, מחיר, "
             "מלאי וקישור לתמונה — ממופה בדיוק לפורמט הייבוא. השגרה השבועית: "
             "לעדכן, לייצא, לייבא. עשר דקות, בלי ידע טכני, והגיליון משמש גם "
             "כספר המלאי של הבוטיק."),
        para("היכן שקיימים גיליונות ספק (PDF או Excel), אבנה ממירים קטנים "
             "בעזרת AI שהופכים אותם לשורות ייבוא נקיות."),
        h3("גיבוי: סקרייפינג מובנה, אם יידרש"),
        para("אם חלק מהמותגים לא מספקים נתוני מוצר שמישים, אוכל לבנות "
             "סקרייפרים ממוקדים לעמודי הקטלוג הפומביים שלהם ולחלץ כותרות, "
             "תיאורים, מפרטים ותמונות לאותו פורמט CSV. הערות היקף:"),
        bullets([
            "משמש רק כגיבוי, לכל מותג בנפרד, היכן שאין מקור נקי יותר.",
            "מוגבל לעמודים פומביים ולנתוני מוצר — בלי מחירי מתחרים ובלי "
            "מידע אישי.",
            "כפוף לתנאי השימוש של כל אתר; אם מותג אוסר זאת, נחזור להזנה ידנית.",
            "התוצר תמיד נבדק ידנית לפני שהוא עולה לאתר.",
        ])))

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
              ["24%", "20%", "56%"], flush_last=False),
        para("כל הודעה נכתבת פעם אחת, בשפה של רוזבלה, במהלך שבוע 2 — בעברית "
             "ובאנגלית היכן שרלוונטי. שום דבר לא נשלח בלי שהצוות אישר אותו.",
             "note")))

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


# ---------------------------------------------------------------- english
def content_en():
    el = ['<h1 class="title">Work Proposal — E-Commerce Launch</h1>',
          '<p class="subtitle">Rosabella TLV · multi-brand fashion boutique, '
          'Shabazi 31, Neve Tzedek · 11,000 Instagram followers. '
          'Design, build, launch and ongoing operation of an online store — '
          'with nothing paid up front.</p>']

    el.append(section(
        "Project overview",
        callout(
            "Rosabella has exactly what most online shops spend years "
            "chasing: a curated multi-brand collection, a storefront on one "
            "of Tel Aviv's most-walked streets, and an audience of 11,000. "
            + b("What's missing is a way for that audience to buy when they "
                "aren't standing on Shabazi Street.") + " This proposal "
            "covers the design, build and launch of a Shopify store wired to "
            "lightweight automation, so day-to-day operations stay simple "
            "for a small team — " + b("no build fee and no retainer") + "."),
        h3("What success looks like"),
        bullets([
            b("A live, mobile-first store") + " — carrying the boutique's "
            "curated look.",
            b("Instagram that sells") + " — bio and posts linking straight "
            "to shoppable product pages.",
            b("Inventory you can trust") + " — synced from a simple CSV "
            "routine the team runs weekly.",
            b("Automated communication") + " — order, shipping and "
            "follow-up messages over WhatsApp and email.",
            b("Operational independence") + " — the team runs the store "
            "day-to-day while I maintain and improve it in the background.",
        ])))

    el.append(section(
        "Timeline",
        para("The build is scoped for two to three weeks end-to-end. Week 3 "
             "is a buffer: it absorbs content delays — photography, product "
             "copy, supplier data — rather than extending the technical work."),
        table(["Phase", "What happens", "Duration"],
              [["Week 1 · Foundation",
                "Discovery answers locked · Shopify Basic account · theme "
                "selection and brand styling · domain and payments · test "
                "order", "~5 days"],
               ["Week 2 · Catalog and automation",
                "CSV inventory import · collections by brand · Zapier flows "
                "for orders and notifications · WhatsApp and email templates",
                "~5 days"],
               ["Week 3 · Polish and launch",
                "Mobile QA · checkout and shipping rules · Instagram "
                "Shopping link-up · team training · go-live", "Buffer"]],
              ["26%", "58%", "16%"]),
        para("Assumes product photos and prices are available by the end of "
             "Week 1 — the discovery questions at the end of this document "
             "exist to de-risk exactly that.", "note")))

    el.append(section(
        "Tech stack",
        para("For a single-location boutique with a curated catalog, the "
             "right stack is the smallest one that does the job — proven, "
             "replaceable, and not requiring a developer on retainer."),
        table(["Component", "Choice", "Why this one"],
              [["Storefront", "Shopify Basic",
                "Best-in-class checkout · native Instagram and Facebook "
                "integration · Hebrew and RTL-capable themes · CSV product "
                "import built in"],
               ["Automation", "Zapier",
                "Connects Shopify to WhatsApp, email and Google Sheets "
                "without code. The team can see and edit every workflow — "
                "no black boxes"],
               ["Email", "Shopify Email",
                "Included (10,000 sends/month free) · receipts, shipping "
                "updates and simple campaigns · Klaviyo is the upgrade path"],
               ["WhatsApp", "WhatsApp Business",
                "Free app to start · a Business API provider can be added "
                "later for automated messaging at scale"],
               ["Inventory bridge", "Google Sheets + CSV",
                "The team edits a familiar spreadsheet; a clean file flows "
                "into Shopify. No new software to learn"]],
              ["16%", "20%", "64%"], flush_last=False)))

    el.append(section(
        "Monthly costs",
        para("Third-party vendor costs after launch. These are Rosabella's "
             "own accounts, paid directly to each vendor — they are not "
             "routed through me and are not part of the revenue share."),
        table(["Vendor", "Plan", "Monthly", "Notes"],
              [["Shopify", "Basic (annual billing)", "$29",
                "$39 billed monthly · includes hosting, SSL and checkout"],
               ["Zapier", "Professional", "$19.99",
                "The free tier is enough during the build"],
               ["Domain", "rosabellatlv.com", "~$1.25", "≈ $15/year"],
               ["WhatsApp Business", "App", "$0", "Quick replies and labels"],
               ["<i>Optional</i> · WhatsApp API", "Wati / Twilio", "$0–49",
                "Only if message volume grows"],
               ["<i>Optional</i> · Klaviyo", "Up to 250 contacts", "$0–20",
                "For campaigns beyond Shopify Email"]],
              ["24%", "24%", "13%", "39%"], flush_last=False),
        para("Core total: <b>≈ $50/month</b> · ≈ $99–119 with both add-ons. "
             "Shopify payment processing fees (a percentage per transaction) "
             "sit outside this table.", "note")))

    el.append(section(
        "Commercial model",
        callout(
            "I am not charging for time. " + b("No hourly rate, no day rate "
            "and no build fee.") + " I design, build and launch the store at "
            "my own cost, and in exchange I take "
            + b("25% of the revenue the store generates") + ". "
            "If the site earns nothing, I earn nothing."),
        table(["Term", "Definition"],
              [["Basis",
                "Net revenue from orders placed through the Shopify store."],
               ["Baseline",
                "Zero. Rosabella has no online sales today, so every order "
                "the site takes is new revenue. In-store and walk-in sales "
                "are excluded entirely — I have no claim on the boutique's "
                "existing trade."],
               ["Deducted first",
                "VAT · shipping charged to the customer · payment processing "
                "fees · refunds, returns and chargebacks. The split applies "
                "to what actually settles."],
               ["Split", "25% to me · 75% to Rosabella."],
               ["Source of truth",
                "Shopify's own admin reports — the same dashboard we both "
                "look at. No separate accounting and nothing for Rosabella "
                "to compile."],
               ["Paid",
                "Monthly, within 14 days of month end, on the previous "
                "month's settled orders."],
               ["Term",
                "<b>Open-ended.</b> The partnership runs for as long as the "
                "store operates — a stake in an asset that was built, not a "
                "project with an end date."],
               ["Sale of the share",
                "<b>From month 12 onward I may sell or transfer my "
                "share</b> — to Rosabella herself or to a third party. "
                "Rosabella holds a right of first refusal: any outside offer "
                "is presented to her first, with 30 days to match it and buy "
                "the share herself."]],
              ["18%", "82%"], flush_last=False),
        h3("What the split looks like in practice"),
        table(["Net monthly online revenue", "My share (25%)",
               "Rosabella keeps"],
              [["₪10,000", "₪2,500", "₪7,500"],
               ["₪25,000", "₪6,250", "₪18,750"],
               ["₪50,000", "₪12,500", "₪37,500"],
               ["₪100,000", "₪25,000", "₪75,000"]],
              ["34%", "33%", "33%"], first_bold=False),
        para("Illustrative figures, not a forecast — actual volume depends "
             "on catalog size, pricing, and how hard the Instagram audience "
             "converts.", "note"),
        h3("Why this model, honestly"),
        bullets([
            b("Rosabella carries no build risk") + " — nothing is owed for "
            "the design, build or launch. If the store underperforms, the "
            "only money spent is roughly $50/month of vendor subscriptions.",
            b("My incentive matches yours") + " — I am paid only when the "
            "store sells, so I am motivated to keep improving conversion "
            "rather than hand over the keys and disappear.",
            b("The honest trade-off") + " — this is a long-term partnership, "
            "not a one-off payment. If the store performs strongly, 25% over "
            "time will come to more than a fixed build fee would have. That "
            "is the price of my carrying the risk instead of Rosabella — and "
            "it is exactly why the right of first refusal exists, so she "
            "always has a way to buy the share and close the matter.",
        ]),
        h3("What stays included for the whole term"),
        para("The ongoing share is not passive. For as long as the "
             "partnership runs I keep the store maintained: automation "
             "fixes, theme and checkout adjustments, catalog and import "
             "support, and a monthly review of what is converting and what "
             "is not — with no separate support invoice."),
        para("These commercial terms are an agreed framework and will be "
             "papered in a formal agreement before work begins. Each side "
             "should have their own lawyer review it.", "note")))

    el.append(section(
        "Inventory strategy",
        h3("Primary path: the CSV workflow"),
        para("Shopify imports products natively from CSV. I'll set up a "
             "Google Sheets master template — one row per variant with "
             "brand, size, color, price, stock and image link — mapped "
             "exactly to the import format. The weekly routine becomes: "
             "update, export, import. Ten minutes, no technical skills, and "
             "the sheet doubles as the boutique's stock ledger."),
        para("Where supplier line sheets exist (PDF or Excel), I'll build "
             "small AI-assisted converters that turn them into clean import "
             "rows."),
        h3("Fallback: structured scraping, if needed"),
        para("If some brands provide no usable product data, I can build "
             "targeted scrapers for their public catalog pages to extract "
             "titles, descriptions, specs and imagery into the same CSV "
             "format. Scope notes:"),
        bullets([
            "Used only as a fallback, per brand, where no cleaner source "
            "exists.",
            "Limited to public pages and product data — no competitor "
            "pricing and no personal information.",
            "Subject to each site's terms of use; if a brand forbids it, we "
            "fall back to manual entry for that brand.",
            "Output is always reviewed by hand before it goes live.",
        ])))

    el.append(section(
        "Customer communication",
        para("In Israel WhatsApp is the default channel and email is the "
             "formal backup. Both are wired to Shopify events through "
             "Zapier, so no customer is left without a reply — and nobody on "
             "the team has to remember to send."),
        table(["Trigger", "Channel", "Message"],
              [["Order placed", "Email",
                "Branded confirmation with order summary"],
               ["Order placed", "WhatsApp",
                "Personal thank-you with pickup or delivery options"],
               ["Shipped / ready", "WhatsApp + email",
                "Tracking link or 'ready for pickup at Shabazi 31'"],
               ["Delivered +3 days", "WhatsApp",
                "Fit check-in and a gentle review ask"],
               ["Cart abandoned", "Email",
                "Reminder with the items left behind"],
               ["New arrivals (weekly)", "Email",
                "Short lookbook-style campaign mirroring the Instagram feed"],
               ["Customer replies", "WhatsApp (human)",
                "Routed to the boutique's phone — automation opens the "
                "conversation, a person continues it"]],
              ["24%", "20%", "56%"], flush_last=False),
        para("Every message is written once, in Rosabella's voice, during "
             "Week 2 — in Hebrew and English where relevant. Nothing sends "
             "that the team hasn't approved.", "note")))

    el.append(section(
        "Next steps",
        table(["When", "Milestone", "Owner"],
              [["This week",
                "Review the proposal · 30-minute call on the discovery "
                "questions", "Both"],
               ["Within 1 week",
                "Partnership terms and scope signed · accounts opened",
                "Alan"],
               ["Early–mid March",
                "Build weeks 1–2: storefront, catalog, automations", "Alan"],
               ["Mid–late March",
                "Buffer week: QA, training, content finishing", "Both"],
               ["Late March", "Soft launch to a small list · test orders",
                "Both"],
               ["First week of April",
                "<b>Public launch, announced to the 11K Instagram "
                "audience</b>", "Both"]],
              ["22%", "62%", "16%"]),
        para("Launch is the start of the partnership, not the end of it. The "
             "Shopify account, the domain, the customer list and the catalog "
             "are Rosabella's property throughout, and remain hers if the "
             "partnership ends.")))

    el.append(section(
        "What's needed to start",
        para("Answers to these five questions turn this proposal into a "
             "signed agreement and a locked timeline:"),
        bullets([
            b("What inventory system does the boutique use today?") + " — "
            "point-of-sale, a spreadsheet, or paper. This determines how the "
            "master sheet gets its first fill and whether ongoing sync can "
            "be automated.",
            b("Do your suppliers provide line sheets?") + " — brand line "
            "sheets (PDF or Excel with products, SKUs, prices, images) skip "
            "manual data entry, and tell us whether the scraping fallback is "
            "needed at all.",
            b("What is the target launch date?") + " — this proposal assumes "
            "the first week of April. Confirming the exact date locks the "
            "timeline.",
            b("How many Instagram posts go out per week?") + " — posting "
            "volume tells us how much shoppable content exists on day one "
            "and how to pace the new-arrivals email.",
            b("What are the budget constraints?") + " — the build costs "
            "nothing up front, so the question is vendor subscriptions, the "
            "optional add-ons, and whether a long-term 25% partnership is a "
            "structure the business is comfortable with.",
        ])))

    return "\n".join(el)


# ---------------------------------------------------------------- template
def css(rtl):
    start, end = ("right", "left") if rtl else ("left", "right")
    return f"""
@page {{ size: A4; margin: 0; }}
* {{ box-sizing: border-box; }}
html, body {{ margin: 0; padding: 0; }}
body {{
  font-family: 'Liberation Sans', Arial, Helvetica, sans-serif;
  font-size: 8.4pt; line-height: 1.55; color: #1A130D;
  direction: {'rtl' if rtl else 'ltr'}; text-align: {start};
  -webkit-print-color-adjust: exact; print-color-adjust: exact;
}}

/* Running header/footer as thead/tfoot of a document-wide table.
   position:fixed was wrong here: it overlays rather than reserves, so
   continuation pages slid their first lines under the header band. A
   repeating thead both prints on every page and pushes content down. */
table.doc {{ width: 100%; border-collapse: collapse; }}
.doc > thead > tr > td, .doc > tfoot > tr > td {{ padding: 0; }}
.band {{ height: 3pt; background: #ECCF9F; }}
.head {{ height: 46pt; background: #0B0805; padding: 9pt 25pt 0;
        direction: ltr; display: flex; justify-content: space-between; }}
.headgap {{ height: 30pt; }}
.head .name {{ color: #ECCF9F; font-size: 10.5pt; letter-spacing: 3.2pt; }}
.head .tag  {{ color: #958F87; font-size: 6.6pt; letter-spacing: 2.1pt;
              margin-top: 2pt; }}
.head .who  {{ color: #A8A198; font-size: 7pt; line-height: 1.5;
              text-align: right; }}
.foot {{ padding: 6pt 25pt 10pt; border-top: 0.6pt solid #EFE7DA;
        color: #8A7F72; font-size: 6.6pt;
        direction: ltr; display: flex; justify-content: space-between; }}
.page {{ padding: 0 25pt 16pt; }}

h1.title {{ font-size: 15pt; line-height: 1.3; margin: 0 0 4pt;
           color: #1A130D; }}
p.subtitle {{ color: #6B5D4F; font-size: 8pt; margin: 0 0 16pt;
             line-height: 1.6; }}

h2.sec {{ font-size: 9.6pt; margin: 17pt 0 0; padding-bottom: 4pt;
         border-bottom: 1.2pt solid #ECCF9F; break-after: avoid; }}
h3 {{ font-size: 8.6pt; margin: 12pt 0 4pt; break-after: avoid; }}
p.body {{ margin: 8pt 0; }}
p.note {{ color: #8A7F72; font-size: 7.4pt; margin: 6pt 0 0;
         line-height: 1.55; }}

.callout {{ background: #FAF5EC; border-{start}: 3pt solid #C98F5E;
           padding: 9pt 12pt; margin: 10pt 0; line-height: 1.6;
           break-inside: avoid; }}

table.data {{ width: 100%; border-collapse: collapse; margin: 9pt 0 0;
             font-size: 7.8pt; }}
.data thead {{ display: table-header-group; }}
.data th {{ text-align: {start}; font-weight: bold; color: #6B5D4F;
           font-size: 7.4pt; padding: 0 0 5pt;
           border-bottom: 1pt solid #D9CBB8; }}
/* trailing column sits flush to the far edge, as in the reference; never
   set `direction` here — it reorders mixed runs and scrambles prose */
.data th:last-child, .data td:last-child {{ text-align: {end}; }}
.data.noflush th:last-child, .data.noflush td:last-child
  {{ text-align: {start}; }}
.data td {{ padding: 6pt 0; border-bottom: 0.6pt solid #EFE7DA;
           vertical-align: top; line-height: 1.5; }}
.data th + th, .data td + td {{ padding-{start}: 10pt; }}
.data td.lead {{ font-weight: bold; white-space: nowrap; }}
.data tr {{ break-inside: avoid; }}

ul {{ margin: 8pt 0; padding-{start}: 14pt; padding-{end}: 0; }}
li {{ margin: 0 0 5pt; line-height: 1.6; }}
li::marker {{ color: #C98F5E; }}
b {{ font-weight: bold; }}
i {{ font-style: italic; color: #8A7F72; }}
"""


def build(lang):
    rtl = lang == "he"
    body = content_he() if rtl else content_en()
    title = ("רוזבלה תל אביב — הצעת עבודה" if rtl
             else "Rosabella TLV — Work Proposal")
    foot_r = (f'<span dir="rtl">הצעת עבודה · הוכנה עבור {e(CLIENT)} · '
              f"{e(DATE)}</span>" if rtl
              else f"Work proposal · prepared for {e(CLIENT)} · {e(DATE)}")
    out = os.path.join(HERE, f"Rosabella-TLV-Proposal-{lang.upper()}.pdf")

    doc = f"""<!doctype html>
<html lang="{lang}" dir="{'rtl' if rtl else 'ltr'}"><head><meta charset="utf-8">
<title>{e(title)}</title><style>{css(rtl)}</style></head><body>
<table class="doc">
<thead><tr><td>
  <div class="band"></div>
  <div class="head">
    <div class="mark">
      <div class="name">ALAN COHEN</div>
      <div class="tag">AI CONSULTING</div>
    </div>
    <div class="who">{e(CLIENT)}<br>{e(DATE)}</div>
  </div>
  <div class="headgap"></div>
</td></tr></thead>
<tfoot><tr><td>
  <div class="foot">
    <div>Alan Cohen · AI Consulting · acohen.tlv@gmail.com</div>
    <div>{foot_r}</div>
  </div>
</td></tr></tfoot>
<tbody><tr><td class="page">
{body}
</td></tr></tbody>
</table>
</body></html>"""

    with tempfile.TemporaryDirectory() as td:
        src = os.path.join(td, f"proposal_{lang}.html")
        with open(src, "w", encoding="utf-8") as fh:
            fh.write(doc)
        subprocess.run(
            [find_chrome(), "--headless", "--disable-gpu", "--no-sandbox",
             "--no-pdf-header-footer", f"--print-to-pdf={out}", src],
            check=True, capture_output=True)
    print(f"Wrote {out}")


if __name__ == "__main__":
    build("he")
    build("en")
