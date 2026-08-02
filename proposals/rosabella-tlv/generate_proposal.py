#!/usr/bin/env python3
"""Generate the Rosabella TLV e-commerce proposal PDF.

Branding: Alan — indie AI builder & consultant. Warm editorial palette
(ink charcoal, dusty rose, sand) to echo a Neve Tzedek fashion boutique.

Usage:  python3 generate_proposal.py  ->  Rosabella-TLV-Proposal.pdf
"""

import os

from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import mm
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

# ---------------------------------------------------------------- palette
INK = colors.HexColor("#26211E")      # near-black warm charcoal
ROSE = colors.HexColor("#B85C6B")     # dusty rose (Rosabella)
ROSE_DEEP = colors.HexColor("#8F3E4D")
SAND = colors.HexColor("#F6F0E8")     # warm paper
SAND_DEEP = colors.HexColor("#EAE0D2")
SLATE = colors.HexColor("#6B625C")    # muted body gray
LINE = colors.HexColor("#D9CFC2")

PAGE_W, PAGE_H = A4
MARGIN = 20 * mm

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                   "Rosabella-TLV-Proposal.pdf")

# ---------------------------------------------------------------- styles
def st(name, **kw):
    base = dict(fontName="Helvetica", fontSize=10, leading=15,
                textColor=INK, alignment=TA_LEFT)
    base.update(kw)
    return ParagraphStyle(name, **base)

S = {
    "kicker": st("kicker", fontName="Helvetica-Bold", fontSize=9,
                 textColor=ROSE, leading=12, spaceAfter=4,
                 tracking=1),
    "h1": st("h1", fontName="Helvetica-Bold", fontSize=20, leading=24,
             spaceBefore=2, spaceAfter=10),
    "h2": st("h2", fontName="Helvetica-Bold", fontSize=12.5, leading=16,
             spaceBefore=10, spaceAfter=5),
    "body": st("body", spaceAfter=7, textColor=INK),
    "muted": st("muted", textColor=SLATE, fontSize=9.5, leading=14,
                spaceAfter=6),
    "bullet": st("bullet", leftIndent=12, bulletIndent=2, spaceAfter=4),
    "cell": st("cell", fontSize=9.5, leading=13),
    "cellb": st("cellb", fontName="Helvetica-Bold", fontSize=9.5, leading=13),
    "cellw": st("cellw", fontName="Helvetica-Bold", fontSize=9.5, leading=13,
                textColor=colors.white),
    "q": st("q", fontName="Helvetica-Bold", fontSize=10.5, leading=15,
            spaceAfter=2),
}


def kicker(text):
    return Paragraph(text.upper(), S["kicker"])


def rule():
    return HRFlowable(width="100%", thickness=0.7, color=LINE,
                      spaceBefore=2, spaceAfter=10)


def bullets(items):
    return [Paragraph(f"<bullet><font color='#B85C6B'>&#9642;</font></bullet> {t}",
                      S["bullet"]) for t in items]


def data_table(header, rows, widths, highlight_last=False):
    data = [[Paragraph(h, S["cellw"]) for h in header]]
    for r in rows:
        data.append([Paragraph(c, S["cell"]) for c in r])
    t = Table(data, colWidths=widths, repeatRows=1, hAlign="LEFT")
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


def signoff(width):
    """Closing card — brand statement + call to action, kept as one block."""
    inner = [
        Paragraph("Alan — AI Consulting", st("so_name",
                  fontName="Helvetica-Bold", fontSize=13, leading=17,
                  textColor=colors.white, spaceAfter=3)),
        Paragraph("indie AI builder &amp; consultant", st("so_tag",
                  fontSize=9.5, leading=13, textColor=ROSE,
                  spaceAfter=8)),
        Paragraph(
            "Independent builder of small, sharp tools for real businesses. "
            "One person, full stack, no hand-offs — the person you talk to "
            "is the person who builds it.",
            st("so_body", fontSize=9.5, leading=14,
               textColor=colors.HexColor("#D9D0C9"), spaceAfter=8)),
        Paragraph(
            "Next step: a 30-minute call to run through the five questions "
            "above. Answers in hand, I'll send the signed revenue-share "
            "agreement and a dated plan for the April launch.",
            st("so_cta", fontName="Helvetica-Bold", fontSize=10, leading=14,
               textColor=colors.white)),
    ]
    t = Table([[inner]], colWidths=[width], hAlign="LEFT")
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), INK),
        ("LINEBEFORE", (0, 0), (0, -1), 2.5, ROSE),
        ("TOPPADDING", (0, 0), (-1, -1), 13),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 13),
        ("LEFTPADDING", (0, 0), (-1, -1), 14),
        ("RIGHTPADDING", (0, 0), (-1, -1), 14),
    ]))
    return t


# ---------------------------------------------------------------- pages
def draw_cover(canv, doc):
    canv.saveState()
    canv.setFillColor(SAND)
    canv.rect(0, 0, PAGE_W, PAGE_H, stroke=0, fill=1)
    # rose side band
    canv.setFillColor(ROSE)
    canv.rect(0, 0, 7 * mm, PAGE_H, stroke=0, fill=1)
    # header mark
    canv.setFillColor(INK)
    canv.setFont("Helvetica-Bold", 11)
    canv.drawString(MARGIN, PAGE_H - 22 * mm, "ALAN")
    canv.setFillColor(ROSE_DEEP)
    canv.setFont("Helvetica", 11)
    canv.drawString(MARGIN + 14 * mm, PAGE_H - 22 * mm,
                    "/  indie AI builder & consultant")
    canv.setStrokeColor(LINE)
    canv.setLineWidth(0.7)
    canv.line(MARGIN, PAGE_H - 26 * mm, PAGE_W - MARGIN, PAGE_H - 26 * mm)

    # title block
    y = PAGE_H - 90 * mm
    canv.setFillColor(ROSE_DEEP)
    canv.setFont("Helvetica-Bold", 10)
    canv.drawString(MARGIN, y + 34 * mm, "E-COMMERCE LAUNCH PROPOSAL")
    canv.setFillColor(INK)
    canv.setFont("Helvetica-Bold", 34)
    canv.drawString(MARGIN, y + 20 * mm, "Rosabella TLV")
    canv.setFont("Helvetica", 13)
    canv.setFillColor(SLATE)
    canv.drawString(MARGIN, y + 10 * mm,
                    "From boutique floor to online storefront — lean, "
                    "automated, on-brand.")

    # client card
    canv.setFillColor(colors.white)
    canv.roundRect(MARGIN, y - 46 * mm, PAGE_W - 2 * MARGIN, 40 * mm,
                   3 * mm, stroke=0, fill=1)
    canv.setFillColor(SLATE)
    canv.setFont("Helvetica-Bold", 8.5)
    cx = MARGIN + 8 * mm
    rows = [
        ("PREPARED FOR", "Rosabella TLV — multi-brand fashion boutique"),
        ("LOCATION", "Shabazi 31, Neve Tzedek, Tel Aviv"),
        ("AUDIENCE", "11,000 Instagram followers"),
        ("PREPARED BY", "Alan — AI Consulting  ·  Target launch: April"),
    ]
    ry = y - 12 * mm
    for label, val in rows:
        canv.setFillColor(ROSE_DEEP)
        canv.setFont("Helvetica-Bold", 8.5)
        canv.drawString(cx, ry, label)
        canv.setFillColor(INK)
        canv.setFont("Helvetica", 10.5)
        canv.drawString(cx + 34 * mm, ry, val)
        ry -= 8 * mm

    # footer
    canv.setFillColor(SLATE)
    canv.setFont("Helvetica", 9)
    canv.drawString(MARGIN, 18 * mm,
                    "Small tools, sharp automations, honest scope. "
                    "Built by one person who ships.")
    canv.restoreState()


def draw_page(canv, doc):
    canv.saveState()
    # top hairline + mark
    canv.setFillColor(INK)
    canv.setFont("Helvetica-Bold", 8.5)
    canv.drawString(MARGIN, PAGE_H - 12 * mm, "ALAN / AI CONSULTING")
    canv.setFillColor(SLATE)
    canv.setFont("Helvetica", 8.5)
    canv.drawRightString(PAGE_W - MARGIN, PAGE_H - 12 * mm,
                         "Rosabella TLV — E-Commerce Launch Proposal")
    canv.setStrokeColor(LINE)
    canv.setLineWidth(0.6)
    canv.line(MARGIN, PAGE_H - 14.5 * mm, PAGE_W - MARGIN, PAGE_H - 14.5 * mm)
    # footer
    canv.line(MARGIN, 15 * mm, PAGE_W - MARGIN, 15 * mm)
    canv.setFillColor(SLATE)
    canv.setFont("Helvetica", 8.5)
    canv.drawString(MARGIN, 10.5 * mm, "Confidential — prepared for Rosabella TLV")
    canv.setFillColor(ROSE_DEEP)
    canv.drawRightString(PAGE_W - MARGIN, 10.5 * mm, f"{canv.getPageNumber()}")
    canv.restoreState()


# ---------------------------------------------------------------- content
def build():
    doc = BaseDocTemplate(OUT, pagesize=A4,
                          leftMargin=MARGIN, rightMargin=MARGIN,
                          topMargin=24 * mm, bottomMargin=22 * mm,
                          title="Rosabella TLV — E-Commerce Launch Proposal",
                          author="Alan — AI Consulting")
    frame = Frame(MARGIN, 22 * mm, PAGE_W - 2 * MARGIN, PAGE_H - 46 * mm,
                  id="body")
    cover_frame = Frame(MARGIN, 22 * mm, PAGE_W - 2 * MARGIN,
                        PAGE_H - 46 * mm, id="cover")
    doc.addPageTemplates([
        PageTemplate(id="Cover", frames=[cover_frame], onPage=draw_cover),
        PageTemplate(id="Body", frames=[frame], onPage=draw_page),
    ])

    W = PAGE_W - 2 * MARGIN
    el = [NextPageTemplate("Body"), Spacer(1, 1), PageBreak()]

    # ---- 1. project overview
    el += [
        kicker("01 · Project overview"),
        Paragraph("A boutique-grade online store, without the agency overhead",
                  S["h1"]),
        rule(),
        Paragraph(
            "Rosabella TLV has what most online shops spend years chasing: a "
            "curated multi-brand collection, a storefront in one of Tel Aviv's "
            "most-walked streets, and an engaged audience of 11,000 Instagram "
            "followers. What's missing is a way for that audience to buy when "
            "they aren't standing on Shabazi Street.", S["body"]),
        Paragraph(
            "This proposal covers the design, build, and launch of a Shopify "
            "store for Rosabella TLV, wired together with lightweight "
            "automation so day-to-day operations stay manageable for a small "
            "team. The goal is simple: turn Instagram attention into orders, "
            "keep inventory honest between the floor and the site, and do it "
            "with no money down — no build fee, no bloated retainers, no "
            "six-month roadmaps. I get paid out of what the store "
            "earns, or not at all.", S["body"]),
        Paragraph("What success looks like", S["h2"]),
    ] + bullets([
        "A live, mobile-first Shopify store carrying the boutique's curated look.",
        "Instagram bio and posts linking to shoppable product pages.",
        "Inventory synced from a simple CSV workflow the team can run weekly.",
        "Order, shipping, and follow-up messages sent automatically via "
        "WhatsApp and email.",
        "A store the team can run independently after a single handover session.",
    ])

    # ---- 2. timeline
    el += [
        Spacer(1, 8),
        KeepTogether([
            kicker("02 · Timeline estimate"),
            Paragraph("Two to three weeks of focused build", S["h1"]),
            rule(),
            Paragraph(
                "The build is scoped for <b>2–3 weeks</b> end-to-end. Week 3 "
                "is a buffer: it absorbs content delays (photography, product "
                "copy, supplier data) rather than extending the technical "
                "work.", S["body"]),
            data_table(
                ["Phase", "Focus", "Deliverables"],
                [
                    ["<b>Week 1</b><br/>Foundation",
                     "Discovery answers locked, Shopify Basic account, theme "
                     "selection & brand styling, domain + payments (credit "
                     "cards / Bit as available via provider)",
                     "Styled storefront skeleton, payment test order"],
                    ["<b>Week 2</b><br/>Catalog & automation",
                     "CSV inventory import, collections by brand, Zapier "
                     "flows for orders & notifications, WhatsApp + email "
                     "templates",
                     "Populated catalog, working automations end-to-end"],
                    ["<b>Week 3</b><br/>Polish & launch (buffer)",
                     "QA on mobile, checkout & shipping rules, Instagram "
                     "Shopping link-up, team training, go-live",
                     "Launch checklist signed off, store live"],
                ],
                [W * 0.20, W * 0.48, W * 0.32]),
        ]),
        Paragraph(
            "Assumes product photos and prices are available by end of Week 1 "
            "— the discovery questions on the final page de-risk exactly "
            "this.", S["muted"]),
    ]

    # ---- 3. tech stack
    el += [
        Spacer(1, 8),
        KeepTogether([
            kicker("03 · Tech stack recommendation"),
            Paragraph("Shopify Basic + Zapier: small stack, big leverage",
                      S["h1"]),
            rule(),
            Paragraph(
                "For a single-location boutique with a curated catalog, the "
                "right stack is the smallest one that does the job. I'm "
                "recommending <b>Shopify Basic</b> as the storefront and "
                "<b>Zapier</b> as the automation glue — both proven, both "
                "replaceable, neither requiring a developer on retainer.",
                S["body"]),
        ]),
        data_table(
            ["Component", "Choice", "Why this one"],
                [
                    ["Storefront",
                     "<b>Shopify Basic</b>",
                     "Best-in-class checkout, native Instagram/Facebook "
                     "Shopping integration, Hebrew/RTL-capable themes, CSV "
                     "product import built in. Basic tier covers everything "
                     "a single boutique needs."],
                    ["Automation",
                     "<b>Zapier</b>",
                     "Connects Shopify to WhatsApp, email, and Google Sheets "
                     "without code. The team can see and edit every workflow "
                     "— no black boxes."],
                    ["Email",
                     "Shopify Email",
                     "Included with Shopify (10,000 sends/month free). "
                     "Handles receipts, shipping updates, and simple "
                     "campaigns. Klaviyo is the upgrade path if campaigns "
                     "grow."],
                    ["WhatsApp",
                     "WhatsApp Business",
                     "Free app to start; a Business-API provider (e.g. Wati "
                     "or Twilio) can be added later for fully automated "
                     "messages at scale."],
                    ["Inventory bridge",
                     "Google Sheets + CSV",
                     "The team edits a familiar spreadsheet; a clean CSV "
                     "flows into Shopify. No new software to learn."],
                ],
                [W * 0.18, W * 0.22, W * 0.60]),
    ]

    # ---- 4. costs
    el += [
        PageBreak(),
        KeepTogether([
            kicker("04 · Monthly cost breakdown"),
            Paragraph("Running costs, itemized", S["h1"]),
            rule(),
            Paragraph(
                "Third-party vendor costs after launch. Prices are current "
                "list prices in USD and worth re-verifying at signing; "
                "monthly-billing rates are slightly higher.", S["body"]),
        ]),
        data_table(
            ["Vendor / Service", "Plan", "Monthly (USD)", "Notes"],
                [
                    ["Shopify", "Basic (annual billing)", "$29",
                     "$39 if billed monthly. Includes hosting, SSL, "
                     "checkout, Shopify Email free tier."],
                    ["Zapier", "Professional (annual billing)", "$19.99",
                     "Covers multi-step Zaps for order + messaging "
                     "automations. Free tier is enough during the build."],
                    ["Domain", "rosabellatlv.com (or .co.il)", "~$1.25",
                     "≈ $15/year, billed annually."],
                    ["WhatsApp Business app", "—", "$0",
                     "Free tier to start; quick-replies + labels."],
                    ["<i>Optional:</i> WhatsApp API provider",
                     "Wati / Twilio, if full automation is wanted",
                     "$0–49", "Only if message volume outgrows the free "
                     "Business app."],
                    ["<i>Optional:</i> Klaviyo email", "Free tier to 250 "
                     "contacts", "$0–20", "Upgrade path for campaigns and "
                     "flows beyond Shopify Email."],
                    ["<b>Core total</b>", "", "<b>≈ $50 / month</b>",
                     "<b>≈ $99–119 / month with both optional add-ons</b>"],
                ],
            [W * 0.24, W * 0.28, W * 0.16, W * 0.32],
            highlight_last=True),
        Paragraph(
            "These are Rosabella's own accounts, paid directly to each "
            "vendor — they are not routed through me and not part of the "
            "revenue share described in the next section. Shopify payment "
            "processing fees (a percentage per transaction, varying by "
            "provider and region) sit outside this table.", S["muted"]),
    ]

    # ---- 5. commercial model
    el += [
        PageBreak(),
        KeepTogether([
            kicker("05 · Commercial model"),
            Paragraph("No build fee — I take 25% of what the store earns",
                      S["h1"]),
            rule(),
            Paragraph(
                "I am not charging for time. There is no hourly rate, no day "
                "rate, and no fixed build fee. I design, build, and launch "
                "the store at my own cost, and in exchange I take "
                "<b>25% of the revenue the store generates</b>. If the site "
                "earns nothing, I earn nothing.", S["body"]),
            Paragraph("How the 25% is calculated", S["h2"]),
        ]),
        data_table(
            ["Term", "Definition"],
                [
                    ["Basis", "Net revenue from orders placed through the "
                     "Shopify store."],
                    ["Baseline", "Zero. Rosabella has no online sales today, "
                     "so every order the site takes is new revenue. In-store "
                     "and walk-in sales are excluded entirely — I have no "
                     "claim on the boutique's existing trade."],
                    ["Deducted first", "VAT, shipping charged to the "
                     "customer, payment processing fees, and any refunds, "
                     "returns, or chargebacks. The split applies to what "
                     "actually settles."],
                    ["My share", "25% of that net figure. Rosabella keeps 75%."],
                    ["Source of truth", "Shopify's own admin reports — the "
                     "same dashboard we both look at. No separate accounting "
                     "and nothing for Rosabella to compile."],
                    ["Paid", "Monthly, within 14 days of month end, on the "
                     "previous month's settled orders."],
                    ["Term", "12 months from public launch, renewable only "
                     "by mutual agreement."],
                    ["Early exit", "From month 6 onward Rosabella can end "
                     "the share by paying 3× the trailing three-month "
                     "average. This caps the total exposure at any point."],
                ],
            [W * 0.22, W * 0.78]),
        Spacer(1, 6),
        KeepTogether([
            Paragraph("What the split looks like in practice", S["h2"]),
            data_table(
                ["Net monthly online revenue", "My 25%", "Rosabella keeps"],
                    [
                        ["NIS 10,000", "NIS 2,500", "NIS 7,500"],
                        ["NIS 25,000", "NIS 6,250", "NIS 18,750"],
                        ["NIS 50,000", "NIS 12,500", "NIS 37,500"],
                        ["NIS 100,000", "NIS 25,000", "NIS 75,000"],
                    ],
                [W * 0.40, W * 0.28, W * 0.32]),
            Paragraph(
                "Illustrative figures, not a forecast — actual volume "
                "depends on catalog size, pricing, and how hard the "
                "Instagram audience converts.", S["muted"]),
        ]),
        Spacer(1, 6),
        KeepTogether([
            Paragraph("Why this model, honestly", S["h2"]),
        ] + bullets([
            "<b>Rosabella carries no build risk.</b> Nothing is owed for the "
            "design, build, or launch. If the store underperforms, the only "
            "money spent is roughly $50/month of vendor subscriptions.",
            "<b>My incentive matches yours.</b> I am paid only when the "
            "store sells, so I am motivated to keep it converting rather "
            "than hand over the keys and disappear.",
            "<b>The honest trade-off:</b> if the store performs strongly, "
            "25% across twelve months will come to more than a fixed build "
            "fee would have. That is the price of my carrying the build "
            "risk instead of Rosabella — and the early-exit clause exists "
            "so there is always a way to cap it.",
        ])),
        Spacer(1, 6),
        KeepTogether([
            Paragraph("What stays included for the whole term", S["h2"]),
            Paragraph(
                "The ongoing share is not passive. For as long as it runs I "
                "keep the store maintained: automation fixes, theme and "
                "checkout adjustments, catalog and import support, and a "
                "monthly review of what is converting and what is not. No "
                "separate support invoice.", S["body"]),
        ]),
    ]

    # ---- 6. inventory
    el += [
        Spacer(1, 8),
        KeepTogether([
            kicker("06 · Inventory strategy"),
            Paragraph("CSV-first, with a scraping fallback", S["h1"]),
            rule(),
            Paragraph("Primary path: the CSV workflow", S["h2"]),
            Paragraph(
                "Shopify imports products natively from CSV. I'll set up a "
                "Google Sheets master template — one row per product variant "
                "with brand, size, color, price, stock, and image link — "
                "mapped exactly to Shopify's import format. The weekly "
                "routine becomes: update the sheet, export, import. Ten "
                "minutes, no technical skills, and the sheet doubles as the "
                "boutique's stock ledger.", S["body"]),
        ]),
        Paragraph(
            "Where supplier line sheets exist (PDF or Excel), I'll build "
            "small AI-assisted converters that turn them into clean import "
            "rows — this is exactly the kind of unglamorous automation an "
            "indie AI builder is for.", S["body"]),
        Paragraph("Fallback: structured scraping, if needed", S["h2"]),
        Paragraph(
            "If some brands provide no usable product data — no line sheets, "
            "no spreadsheets — I can build targeted scrapers for those "
            "brands' public catalog pages to extract titles, descriptions, "
            "specs, and imagery into the same CSV format. Scope notes:",
            S["body"]),
    ] + bullets([
        "Used only as a fallback, per brand, where no cleaner source exists.",
        "Respects each site's terms of service; product descriptions get "
        "rewritten in Rosabella's voice rather than copied verbatim.",
        "Scrapers are delivered as simple re-runnable scripts, so refreshing "
        "a brand's catalog next season is a one-command job.",
    ])

    # ---- 7. communication
    el += [
        Spacer(1, 8),
        KeepTogether([
            kicker("07 · Customer communication workflow"),
            Paragraph("WhatsApp + email, automated via Zapier", S["h1"]),
            rule(),
            Paragraph(
                "Israeli customers live on WhatsApp; email carries the "
                "transactional record. The store will use both, each where "
                "it's strongest, with Zapier moving data between Shopify and "
                "the messaging layer.", S["body"]),
        ]),
        data_table(
            ["Trigger (Shopify)", "Channel", "Automated message"],
                [
                    ["Order placed", "Email", "Branded confirmation with "
                     "order summary (Shopify native)."],
                    ["Order placed", "WhatsApp", "Personal thank-you + "
                     "pickup/delivery options, via Zapier."],
                    ["Order shipped / ready", "WhatsApp + email",
                     "Tracking link or 'ready for pickup at Shabazi 31'."],
                    ["Delivered +3 days", "WhatsApp", "Fit check-in and "
                     "gentle review ask."],
                    ["Cart abandoned", "Email", "Reminder with the items "
                     "left behind (Shopify native)."],
                    ["New arrivals (weekly)", "Email", "Short lookbook-style "
                     "campaign mirroring the Instagram feed."],
                    ["Customer replies on WhatsApp", "WhatsApp (human)",
                     "Routed to the boutique's phone — automation opens the "
                     "conversation, a person continues it."],
                ],
            [W * 0.26, W * 0.20, W * 0.54]),
        Paragraph(
            "Every automated message is written once, in Rosabella's tone, "
            "during Week 2 — Hebrew and English versions where relevant. "
            "Nothing sends that the team hasn't approved.", S["muted"]),
    ]

    # ---- 8. next steps
    el += [
        Spacer(1, 8),
        KeepTogether([
            kicker("08 · Next steps"),
            Paragraph("Path to an April launch", S["h1"]),
            rule(),
            Paragraph(
                "Working backwards from a first-week-of-April go-live, with "
                "the 2–3 week build placed to leave breathing room:",
                S["body"]),
        ]),
        data_table(
            ["When", "Milestone", "Owner"],
                [
                    ["This week", "Rosabella reviews this proposal; "
                     "30-minute call to walk through the discovery "
                     "questions (next page).", "Both"],
                    ["Within 1 week", "Answers in hand — revenue-share terms "
                     "and scope signed. Shopify + domain accounts opened.",
                     "Alan"],
                    ["Early–mid March", "Build Weeks 1–2: storefront, "
                     "catalog import, automations.", "Alan"],
                    ["Mid–late March", "Week 3 buffer: QA, training "
                     "session, content finishing.", "Both"],
                    ["Last week of March", "Soft launch to a small list; "
                     "test orders end-to-end.", "Both"],
                    ["<b>First week of April</b>",
                     "<b>Public launch, announced to the 11K Instagram "
                     "audience.</b>", "<b>Both</b>"],
                ],
            [W * 0.22, W * 0.62, W * 0.16]),
        Paragraph(
            "After launch I stay available for a light support window — "
            "fixes and tweaks as real orders come in — before handing the "
            "keys over fully.", S["body"]),
    ]

    # ---- 9. discovery questions
    q_items = [
        ("1. What inventory system does the boutique use today?",
         "Point-of-sale software, a spreadsheet, or paper — this determines "
         "how the CSV master sheet gets its first fill and whether ongoing "
         "sync can be automated."),
        ("2. Do your suppliers provide line sheets?",
         "Brand line sheets (PDF/Excel with products, SKUs, prices, images) "
         "let us skip manual data entry — and tell us whether the scraping "
         "fallback is needed at all."),
        ("3. What is the target launch date?",
         "This proposal assumes the first week of April — confirming the "
         "exact date (and any events tied to it) locks the timeline."),
        ("4. How many Instagram posts go out per week today?",
         "Current posting volume tells us how much shoppable content exists "
         "on day one and how to pace the new-arrivals email that mirrors "
         "the feed."),
        ("5. What are the budget constraints?",
         "The build itself costs nothing up front, so this is about the "
         "roughly $50/month of vendor subscriptions and whether the "
         "optional add-ons (WhatsApp API, Klaviyo) are affordable now or "
         "later — and whether a 25% revenue share is a shape the business "
         "is comfortable with at all."),
    ]
    q_flow = []
    for q, why in q_items:
        q_flow.append(KeepTogether([
            Paragraph(q, S["q"]),
            Paragraph(why, S["muted"]),
            Spacer(1, 3),
        ]))
    el += [
        PageBreak(),
        KeepTogether([
            kicker("09 · Before we start"),
            Paragraph("Five discovery questions for Rosabella", S["h1"]),
            rule(),
            Paragraph(
                "Answers to these five questions turn this proposal into a "
                "signed agreement and a locked timeline:", S["body"]),
        ]),
    ] + q_flow + [
        Spacer(1, 12),
        signoff(W),
    ]

    doc.build(el)
    print(f"Wrote {OUT}")


if __name__ == "__main__":
    build()
