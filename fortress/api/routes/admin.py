"""Admin API routes — user management.

Admin-only endpoints for listing, creating, and updating users.
All users share the same database (companies, contacts, etc.).
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from fortress.api.auth import decode_session_token, hash_password
from fortress.api.db import fetch_all, get_conn

logger = logging.getLogger("fortress.api.admin")

router = APIRouter(prefix="/api/admin", tags=["admin"])

_COOKIE_NAME = "fortress_session"


def _get_admin(request: Request):
    """Verify the request comes from an admin. Returns SessionUser or None."""
    token = request.cookies.get(_COOKIE_NAME)
    if not token:
        return None
    user = decode_session_token(token)
    if not user or user.role != "admin":
        return None
    return user


@router.get("/users")
async def list_users(request: Request):
    """List all users. Admin only."""
    admin = _get_admin(request)
    if not admin:
        return JSONResponse(status_code=403, content={"error": "Admin requis."})

    rows = await fetch_all(
        "SELECT id, username, role, display_name, created_at, last_login FROM users ORDER BY id"
    )
    return {"users": rows, "count": len(rows)}


@router.post("/users")
async def create_user(request: Request):
    """Create a new user. Admin only.

    Body: {"username": "...", "password": "...", "role": "user", "display_name": "..."}
    """
    admin = _get_admin(request)
    if not admin:
        return JSONResponse(status_code=403, content={"error": "Admin requis."})

    try:
        body = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"error": "Corps invalide."})

    username = (body.get("username") or "").strip()
    password = body.get("password") or ""
    role = (body.get("role") or "user").strip().lower()
    display_name = (body.get("display_name") or username).strip()

    if not username or not password:
        return JSONResponse(status_code=400, content={"error": "username et password requis."})
    if role not in ("admin", "user"):
        return JSONResponse(status_code=400, content={"error": "role doit etre 'admin' ou 'user'."})
    if len(password) < 4:
        return JSONResponse(status_code=400, content={"error": "Mot de passe trop court (min 4)."})

    pw_hash = hash_password(password)

    async with get_conn() as conn:
        # Check if exists
        cur = await conn.execute("SELECT id FROM users WHERE username = %s", (username,))
        if await cur.fetchone():
            return JSONResponse(status_code=409, content={"error": f"'{username}' existe deja."})

        await conn.execute(
            "INSERT INTO users (username, password_hash, role, display_name) VALUES (%s, %s, %s, %s)",
            (username, pw_hash, role, display_name),
        )
        await conn.commit()

    logger.info("admin.user_created", extra={"username": username, "role": role, "by": admin.username})
    return {"created": True, "username": username, "role": role, "display_name": display_name}


@router.patch("/users/{user_id}")
async def update_user(user_id: int, request: Request):
    """Update a user's display_name, role, or password. Admin only.

    Body: {"display_name": "...", "role": "...", "password": "..."}
    Only provided fields are updated.
    """
    admin = _get_admin(request)
    if not admin:
        return JSONResponse(status_code=403, content={"error": "Admin requis."})

    try:
        body = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"error": "Corps invalide."})

    updates = []
    params = []

    if "display_name" in body:
        updates.append("display_name = %s")
        params.append(body["display_name"])

    if "role" in body and body["role"] in ("admin", "user"):
        updates.append("role = %s")
        params.append(body["role"])

    if "password" in body and len(body["password"]) >= 4:
        updates.append("password_hash = %s")
        params.append(hash_password(body["password"]))

    if "username" in body and body["username"].strip():
        updates.append("username = %s")
        params.append(body["username"].strip())

    if not updates:
        return JSONResponse(status_code=400, content={"error": "Rien a modifier."})

    params.append(user_id)
    async with get_conn() as conn:
        cur = await conn.execute(
            f"UPDATE users SET {', '.join(updates)} WHERE id = %s RETURNING id, username, role, display_name",
            tuple(params),
        )
        row = await cur.fetchone()
        if not row:
            return JSONResponse(status_code=404, content={"error": "Utilisateur introuvable."})
        await conn.commit()

    return {"updated": True, "user": {"id": row[0], "username": row[1], "role": row[2], "display_name": row[3]}}
