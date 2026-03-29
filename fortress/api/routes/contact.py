"""
Contact form — public endpoint for landing page submissions.

Stores requests in `contact_requests` table AND forwards via email.
Admin also sees them in the activity feed.
"""

import asyncio
import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from fortress.api.db import fetch_one, execute
from fortress.config.settings import settings

logger = logging.getLogger("fortress.contact")

router = APIRouter(prefix="/api/contact", tags=["contact"])


class ContactRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=200)
    email: str = Field(..., min_length=3, max_length=200)
    company: str = Field("", max_length=200)
    message: str = Field(..., min_length=5, max_length=2000)


def _send_email_sync(req: ContactRequest):
    """Send notification email via Gmail SMTP (runs in thread pool)."""
    smtp_user = getattr(settings, "smtp_user", None) or ""
    smtp_pass = getattr(settings, "smtp_password", None) or ""
    notify_to = getattr(settings, "contact_notify_email", None) or smtp_user

    if not smtp_user or not smtp_pass:
        logger.warning("SMTP not configured — skipping email notification")
        return False

    msg = MIMEMultipart("alternative")
    msg["From"] = smtp_user
    msg["To"] = notify_to
    msg["Subject"] = f"🏰 Fortress — Nouvelle demande de {req.name}"
    msg["Reply-To"] = req.email

    # Plain text version
    text = f"""Nouvelle demande de contact sur Fortress

Nom: {req.name}
Email: {req.email}
Entreprise: {req.company or '(non renseigné)'}

Message:
{req.message}

---
Répondre directement à cet email pour contacter {req.name}.
"""

    # HTML version
    html = f"""
    <div style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto;padding:20px">
        <div style="background:#6c5ce7;color:white;padding:16px 24px;border-radius:8px 8px 0 0">
            <h2 style="margin:0;font-size:18px">🏰 Fortress — Nouvelle demande de contact</h2>
        </div>
        <div style="background:#f8f9fa;padding:24px;border:1px solid #e9ecef;border-top:none;border-radius:0 0 8px 8px">
            <table style="width:100%;border-collapse:collapse;font-size:14px">
                <tr>
                    <td style="padding:8px 0;color:#666;width:100px"><strong>Nom</strong></td>
                    <td style="padding:8px 0">{req.name}</td>
                </tr>
                <tr>
                    <td style="padding:8px 0;color:#666"><strong>Email</strong></td>
                    <td style="padding:8px 0"><a href="mailto:{req.email}" style="color:#6c5ce7">{req.email}</a></td>
                </tr>
                <tr>
                    <td style="padding:8px 0;color:#666"><strong>Entreprise</strong></td>
                    <td style="padding:8px 0">{req.company or '<em style="color:#999">Non renseigné</em>'}</td>
                </tr>
            </table>
            <hr style="border:none;border-top:1px solid #e9ecef;margin:16px 0">
            <div style="white-space:pre-wrap;font-size:14px;line-height:1.6;color:#333">{req.message}</div>
            <hr style="border:none;border-top:1px solid #e9ecef;margin:16px 0">
            <p style="font-size:12px;color:#999;margin:0">
                Répondre directement à cet email pour contacter {req.name}.
            </p>
        </div>
    </div>
    """

    msg.attach(MIMEText(text, "plain"))
    msg.attach(MIMEText(html, "html"))

    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(smtp_user, smtp_pass)
            server.send_message(msg)
        logger.info("✅ Contact email sent to %s", notify_to)
        return True
    except Exception as e:
        logger.error("❌ Failed to send contact email: %s", e)
        return False


@router.post("")
async def submit_contact(req: ContactRequest):
    """Public endpoint — no auth. Stores submission + sends email."""

    # Basic email format check
    if "@" not in req.email or "." not in req.email.split("@")[-1]:
        raise HTTPException(status_code=422, detail="Adresse email invalide")

    # Rate limit: max 5 submissions per email per day
    recent = await fetch_one("""
        SELECT COUNT(*) AS cnt FROM contact_requests
        WHERE email = %s AND created_at >= NOW() - INTERVAL '1 day'
    """, (req.email,))

    if recent and (recent.get("cnt", 0) or 0) >= 5:
        raise HTTPException(
            status_code=429,
            detail="Trop de demandes. Veuillez réessayer demain."
        )

    # 1. Store in database
    await execute("""
        INSERT INTO contact_requests (name, email, company, message)
        VALUES (%s, %s, %s, %s)
    """, (req.name.strip(), req.email.strip(), req.company.strip(), req.message.strip()))

    # 2. Send email notification (in background thread to not block response)
    loop = asyncio.get_event_loop()
    loop.run_in_executor(None, _send_email_sync, req)

    # 3. Log in activity_log for admin visibility
    try:
        await execute("""
            INSERT INTO activity_log (username, action, target_type, target_id, details)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            req.name.strip(),
            "contact_request",
            "contact",
            req.email.strip(),
            f"Demande de contact: {req.name.strip()} ({req.email.strip()}) — {req.message[:100]}"
        ))
    except Exception:
        pass  # Don't fail the submission if activity logging fails

    return {"ok": True, "message": "Votre demande a bien été envoyée. Nous vous répondrons rapidement."}


@router.get("/list")
async def list_contacts_requests(request: Request):
    """Admin-only list of contact form submissions."""
    user = getattr(request.state, "user", None)
    if not user or not user.is_admin:
        return JSONResponse(status_code=403, content={"error": "Accès refusé."})

    rows = await fetch_one("""
        SELECT
            json_agg(
                json_build_object(
                    'id', id,
                    'name', name,
                    'email', email,
                    'company', company,
                    'message', message,
                    'created_at', created_at
                ) ORDER BY created_at DESC
            ) AS requests
        FROM contact_requests
    """)
    return {"requests": (rows or {}).get("requests") or []}
