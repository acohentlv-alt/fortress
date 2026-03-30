"""Bug report endpoint — saves to DB + emails Alan."""

import asyncio
import base64
import html
import json
import logging
import smtplib
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email import encoders

from fastapi import APIRouter, File, Form, Request, UploadFile
from fastapi.responses import JSONResponse

from fortress.api.db import execute
from fortress.api.routes.activity import log_activity
from fortress.config.settings import settings

logger = logging.getLogger("fortress.bug_report")

router = APIRouter(prefix="/api/bug-report", tags=["bug-report"])

MAX_FILE_SIZE = 5 * 1024 * 1024


def _send_bug_email_sync(description: str, ctx: dict, screenshot_bytes: bytes | None, screenshot_name: str | None):
    smtp_user = getattr(settings, "smtp_user", None) or ""
    smtp_pass = getattr(settings, "smtp_password", None) or ""
    notify_to = getattr(settings, "contact_notify_email", None) or smtp_user

    if not smtp_user or not smtp_pass:
        logger.warning("SMTP not configured — skipping bug report email")
        return

    username = ctx.get("username", "inconnu")
    role = ctx.get("role", "")
    workspace_id = ctx.get("workspace_id") or "(admin)"
    page_url = ctx.get("page_url", "")
    timestamp = ctx.get("timestamp", "")
    user_agent = ctx.get("user_agent", "")
    screen = ctx.get("screen", "")
    errors = ctx.get("console_errors", [])

    safe_desc = html.escape(description)

    msg = MIMEMultipart("mixed")
    msg["From"] = smtp_user
    msg["To"] = notify_to
    msg["Subject"] = f"Bug Report — {username}"

    error_html = ""
    if errors:
        error_html = "<h4 style='margin:12px 0 4px'>Erreurs console</h4><ul style='font-size:12px;color:#c0392b'>" + "".join(
            f"<li><code>{html.escape(str(e.get('time','')))}</code> — {html.escape(str(e.get('message','')))}</li>" for e in errors
        ) + "</ul>"

    body_html = f"""
    <div style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto;padding:20px">
        <div style="background:#e74c3c;color:white;padding:16px 24px;border-radius:8px 8px 0 0">
            <h2 style="margin:0;font-size:18px">Bug Report — {html.escape(username)}</h2>
        </div>
        <div style="background:#f8f9fa;padding:24px;border:1px solid #e9ecef;border-top:none;border-radius:0 0 8px 8px">
            <table style="width:100%;border-collapse:collapse;font-size:14px">
                <tr><td style="padding:6px 0;color:#666;width:110px"><strong>Utilisateur</strong></td><td>{html.escape(username)} ({html.escape(role)})</td></tr>
                <tr><td style="padding:6px 0;color:#666"><strong>Workspace</strong></td><td>{html.escape(str(workspace_id))}</td></tr>
                <tr><td style="padding:6px 0;color:#666"><strong>Page</strong></td><td>{html.escape(page_url)}</td></tr>
                <tr><td style="padding:6px 0;color:#666"><strong>Date</strong></td><td>{html.escape(timestamp)}</td></tr>
                <tr><td style="padding:6px 0;color:#666"><strong>Écran</strong></td><td>{html.escape(screen)}</td></tr>
            </table>
            <hr style="border:none;border-top:1px solid #e9ecef;margin:16px 0">
            <h4 style="margin:0 0 8px">Description</h4>
            <div style="white-space:pre-wrap;font-size:14px;line-height:1.6;color:#333;background:white;padding:12px;border-radius:4px;border:1px solid #e9ecef">{safe_desc}</div>
            {error_html}
            {"<p style='font-size:12px;color:#999;margin-top:16px'>Capture d'écran jointe</p>" if screenshot_bytes else ""}
        </div>
    </div>
    """

    text_body = f"Bug report de {username}\n\nPage: {page_url}\nDate: {timestamp}\n\n{description}"

    alt = MIMEMultipart("alternative")
    alt.attach(MIMEText(text_body, "plain"))
    alt.attach(MIMEText(body_html, "html"))
    msg.attach(alt)

    if screenshot_bytes and screenshot_name:
        part = MIMEBase("application", "octet-stream")
        part.set_payload(screenshot_bytes)
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", f'attachment; filename="{screenshot_name}"')
        msg.attach(part)

    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(smtp_user, smtp_pass)
            server.send_message(msg)
        logger.info("Bug report email sent to %s", notify_to)
    except Exception as e:
        logger.error("Failed to send bug report email: %s", e)


@router.post("")
async def submit_bug_report(
    request: Request,
    description: str = Form(...),
    context: str = Form("{}"),
    screenshot: UploadFile | None = File(None),
):
    user = getattr(request.state, "user", None)
    if not user:
        return JSONResponse(status_code=401, content={"error": "Authentification requise."})

    description = description.strip()
    if not description or len(description) < 5:
        return JSONResponse(status_code=422, content={"error": "Description trop courte (minimum 5 caractères)."})
    if len(description) > 5000:
        return JSONResponse(status_code=422, content={"error": "Description trop longue (maximum 5000 caractères)."})

    try:
        ctx = json.loads(context)
    except (json.JSONDecodeError, TypeError):
        ctx = {}

    screenshot_bytes = None
    screenshot_name = None
    screenshot_b64 = None
    if screenshot and screenshot.filename:
        ct = (screenshot.content_type or "").lower()
        if not ct.startswith("image/"):
            return JSONResponse(status_code=422, content={"error": "Le fichier doit être une image."})
        screenshot_bytes = await screenshot.read()
        if len(screenshot_bytes) > MAX_FILE_SIZE:
            return JSONResponse(status_code=422, content={"error": "Le fichier dépasse 5 Mo."})
        screenshot_name = screenshot.filename
        screenshot_b64 = base64.b64encode(screenshot_bytes).decode("ascii")

    await execute("""
        INSERT INTO bug_reports (username, role, workspace_id, description, context, screenshot_name, screenshot_data, page_url)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        user.username,
        user.role,
        user.workspace_id,
        description,
        context,
        screenshot_name,
        screenshot_b64,
        ctx.get("page_url") or "",
    ))

    asyncio.get_event_loop().run_in_executor(None, _send_bug_email_sync, description, ctx, screenshot_bytes, screenshot_name)

    try:
        await log_activity(
            user.id,
            user.username,
            "bug_report",
            "bug",
            ctx.get("page_url", ""),
            f"Bug report: {description[:100]}"
        )
    except Exception:
        pass

    return {"ok": True, "message": "Rapport envoyé. Merci !"}
