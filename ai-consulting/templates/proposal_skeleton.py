#!/usr/bin/env python3
"""<Client name> — <document type>.

Copy to clients/<slug>/proposal.py and replace the content. Content only:
anything to do with appearance belongs in ai-consulting/house_format.py.

Usage:  python3 proposal.py
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))))

from house_format import (  # noqa: E402
    b, bullets, callout, h3, ltr, note, para, render, section, subtitle,
    table, title,
)

HERE = os.path.dirname(os.path.abspath(__file__))
CLIENT = "<Client name>"
DATE = "<DD.MM.YYYY>"
SLUG = "<Client-Slug>"


def content_he():
    el = [title("<כותרת המסמך>"),
          subtitle("<שורת תקציר: מי הלקוח, מה ההצעה מכסה>")]

    el.append(section(
        "<שם הפרק>",
        # a callout carries the single most important claim on the page
        callout("<המשפט המרכזי, עם " + b("הדגשה") + " היכן שצריך>"),
        para("<פסקה רגילה>"),
        h3("<כותרת משנה>"),
        bullets([
            b("<לid מודגש>") + " — <הסבר>",
            "<שורה רגילה>",
        ])))

    el.append(section(
        "<פרק עם טבלה>",
        table(["<עמודה>", "<עמודה>", "<משך>"],
              [["<ערך>", "<תיאור>", "<קצר>"]],
              ["30%", "54%", "16%"]),
        # flush_last=False when the trailing column holds prose, not a
        # short value — otherwise the paragraph comes out ragged
        note("<הערת שוליים>")))

    return "\n".join(el)


def content_en():
    el = [title("<Document title>"),
          subtitle("<One-line summary: who the client is, what this covers>")]

    el.append(section(
        "<Section name>",
        callout("<The single most important claim, with "
                + b("emphasis") + " where it earns it>"),
        para("<A normal paragraph>"),
        h3("<Sub-heading>"),
        bullets([
            b("<Bold lead-in>") + " — <explanation>",
            "<A plain line>",
        ])))

    el.append(section(
        "<Section with a table>",
        table(["<Column>", "<Column>", "<Duration>"],
              [["<Value>", "<Description>", "<Short>"]],
              ["30%", "54%", "16%"]),
        note("<Footnote>")))

    return "\n".join(el)


if __name__ == "__main__":
    for lang, body, doc_title in (
        ("he", content_he(), f"{CLIENT} — <כותרת>"),
        ("en", content_en(), f"{CLIENT} — <Title>"),
    ):
        render(body, os.path.join(HERE, f"{SLUG}-Proposal-{lang.upper()}.pdf"),
               lang=lang, client=CLIENT, date=DATE, doc_title=doc_title)
