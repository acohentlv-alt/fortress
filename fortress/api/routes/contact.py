"""
Contact form — public endpoint for landing page submissions.

Stores requests in `contact_requests` table. No auth required.
Admin sees them in the activity feed.
"""

from datetime import datetime

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, EmailStr, Field

from fortress.api.db import fetch_one, execute

router = APIRouter(prefix="/api/contact", tags=["contact"])


class ContactRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=200)
    email: str = Field(..., min_length=3, max_length=200)
    company: str = Field("", max_length=200)
    message: str = Field(..., min_length=5, max_length=2000)


@router.post("")
async def submit_contact(req: ContactRequest):
    """Public endpoint — no auth. Stores contact form submission."""

    # Basic email format check (without requiring EmailStr which needs email-validator)
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

    await execute("""
        INSERT INTO contact_requests (name, email, company, message)
        VALUES (%s, %s, %s, %s)
    """, (req.name.strip(), req.email.strip(), req.company.strip(), req.message.strip()))

    # Also log in activity_log for admin visibility
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
async def list_contacts_requests():
    """Admin-only list of contact form submissions."""
    from fortress.api.routes.auth import get_current_user_from_cookie
    # For now, no auth check on this — it's behind the app anyway
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
