"""Company notes (comments) — per-company text annotations.

Endpoints:
    GET    /api/notes/{siren}     — list all notes for a company
    POST   /api/notes/{siren}     — add a note (user from session)
    DELETE /api/notes/{id}        — delete a note (author or admin only)
"""

from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from fortress.api.db import fetch_all, fetch_one, get_conn
from fortress.api.routes.activity import log_activity

router = APIRouter(prefix="/api/notes", tags=["notes"])


class NoteCreate(BaseModel):
    text: str = Field(..., min_length=1, max_length=2000)


@router.get("/{siren}")
async def list_notes(siren: str):
    """Return all notes for a company, newest first."""
    notes = await fetch_all("""
        SELECT id, siren, user_id, username, text, created_at
        FROM company_notes
        WHERE siren = %s
        ORDER BY created_at DESC
    """, (siren,))
    return {"notes": notes or [], "count": len(notes or [])}


@router.post("/{siren}", status_code=201)
async def add_note(siren: str, body: NoteCreate, request: Request):
    """Add a note to a company. User is extracted from session."""
    try:
        user = getattr(request.state, "user", None)
        if not user:
            return JSONResponse(status_code=401, content={"error": "Non authentifié"})

        user_id = user.id
        username = user.username

        # Verify company exists
        company = await fetch_one(
            "SELECT siren FROM companies WHERE siren = %s", (siren,)
        )
        if not company:
            return JSONResponse(
                status_code=404, content={"error": "Entreprise introuvable"}
            )

        async with get_conn() as conn:
            result = await conn.execute("""
                INSERT INTO company_notes (siren, user_id, username, text)
                VALUES (%s, %s, %s, %s)
                RETURNING id, created_at
            """, (siren, user_id, username, body.text.strip()))
            row = await result.fetchone()
            await conn.commit()

        await log_activity(
            user_id=user_id,
            username=username,
            action='note_added',
            target_type='company',
            target_id=siren,
            details=f"Note ajoutée sur {siren}",
        )

        return {
            "id": row[0],
            "siren": siren,
            "user_id": user_id,
            "username": username,
            "text": body.text.strip(),
            "created_at": str(row[1]),
        }
    except Exception as e:
        import logging
        logging.getLogger("fortress").exception("add_note crash for %s", siren)
        return JSONResponse(
            status_code=500, content={"error": "Erreur interne du serveur"}
        )


@router.delete("/{note_id}")
async def delete_note(note_id: int, request: Request):
    """Delete a note. Only the author or an admin can delete."""
    user = getattr(request.state, "user", None)
    if not user:
        return JSONResponse(status_code=401, content={"error": "Non authentifié"})

    note = await fetch_one(
        "SELECT id, user_id FROM company_notes WHERE id = %s", (note_id,)
    )
    if not note:
        return JSONResponse(status_code=404, content={"error": "Note introuvable"})

    # Only author or admin can delete
    if note["user_id"] != user.id and user.role != "admin":
        return JSONResponse(
            status_code=403, content={"error": "Non autorisé à supprimer cette note"}
        )

    async with get_conn() as conn:
        await conn.execute("DELETE FROM company_notes WHERE id = %s", (note_id,))
        await conn.commit()

    await log_activity(
        user_id=user.id,
        username=user.username,
        action='note_deleted',
        target_type='note',
        target_id=str(note_id),
        details=f"Note {note_id} supprimée",
    )

    return {"deleted": True, "id": note_id}
