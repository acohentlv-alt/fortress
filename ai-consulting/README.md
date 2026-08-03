# Alan Cohen · AI Consulting

Client-facing documents for the consulting business — websites, agents and
systems. Proposals, work plans and quotes all live here and all share one
house format.

```
ai-consulting/
├── house_format.py                  the format — colors, type, layout, render()
├── templates/
│   └── proposal_skeleton.py         copy this to start a new client
└── clients/
    └── rosabella-tlv/
        ├── proposal.py              content only
        ├── Rosabella-TLV-Proposal-HE.pdf
        └── Rosabella-TLV-Proposal-EN.pdf
```

## The one rule

**Content lives in the client folder. Appearance lives in `house_format.py`.**

A client file should contain sentences and table rows — nothing else. If you
find yourself writing a color, a font size or a margin inside a client file,
it belongs in `house_format.py` instead. That is the whole point of the split:
change a token once and every document the business has ever produced follows.

## Starting a new client

```bash
mkdir -p clients/<client-slug>
cp templates/proposal_skeleton.py clients/<client-slug>/proposal.py
# edit CLIENT, DATE and the content functions, then:
cd clients/<client-slug> && python3 proposal.py
```

Both language editions are produced from the same content functions. Drop
`content_en` (and its entry in the `__main__` loop) if a client only needs
Hebrew.

## Where the format came from

It was reverse-engineered from the Neve Tzedek Properties work plan. That PDF
turned out to be a Chrome print-to-PDF of an HTML page — Skia/PDF producer,
Arial only — so documents here are built as HTML and printed with headless
Chromium rather than composed in a PDF library. Two benefits: the output
matches the original exactly, and Hebrew bidi is handled natively by the
browser instead of by hand.

Every design token in `house_format.py` was sampled from a render of that
reference rather than eyeballed:

| Token | Value | Used for |
|---|---|---|
| `GOLD` | `#ECCF9F` | top band, section rules |
| `BRONZE` | `#C98F5E` | callout accent, list markers |
| `INK_BAND` | `#0B0805` | header band |
| `INK_TEXT` | `#1A130D` | headings, body |
| `CREAM` | `#FAF5EC` | callout fill |
| `RULE_HEAD` | `#D9CBB8` | rule under a table header row |
| `RULE_HAIR` | `#EFE7DA` | rule between table rows |
| margin / band / header | 25pt / 3pt / 46pt | page geometry |

Font is Liberation Sans, which is metric-compatible with Arial and carries
full Hebrew coverage plus the shekel sign.

## RTL traps worth knowing

Three things bit during the build and are now handled in `house_format.py`.
They will bite again if the format is ever reimplemented elsewhere:

- **Never set `direction: ltr` on a cell that may hold Hebrew prose.** It
  reorders mixed runs and scrambles the sentence. To pin a column to the far
  edge, change `text-align` only.
- **Wrap bare Latin/numeric runs in `ltr()`.** A range like `$0-49` renders as
  `49-$0` inside Hebrew text otherwise.
- **A running header must reserve space, not overlay it.** `position: fixed`
  looks right on page 1 and slides continuation pages under the band. The
  header and footer are the `thead`/`tfoot` of a document-wide table for
  exactly this reason.

## Building

Requires headless Chromium (already present in the dev container at
`/opt/pw-browsers/`) and the Liberation fonts. No Python packages beyond the
standard library.
