"""Auth API routes — login, logout, current user.

Session is stored as a signed cookie (fortress_session).
The cookie is HttpOnly (JS can't read it) and sent automatically on every request.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, Request, Response
from fastapi.responses import JSONResponse

from fortress.api.auth import (
    create_session_token,
    decode_session_token,
    get_user_by_username,
    update_last_login,
    verify_password,
)
from fortress.api.db import get_conn
from fortress.config.settings import settings

logger = logging.getLogger("fortress.api.auth")

router = APIRouter(prefix="/api/auth", tags=["auth"])

# Cookie settings
_COOKIE_NAME = "fortress_session"
_COOKIE_MAX_AGE = 86400  # 24 hours


@router.post("/login")
async def login(request: Request):
    """Authenticate with username + password. Sets a session cookie.

    Body: {"username": "...", "password": "..."}
    Returns: {"status": "ok", "user": {id, username, role, display_name}}
    """
    try:
        body = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"error": "Corps de requête invalide."})

    username = (body.get("username") or "").strip()
    password = body.get("password") or ""

    if not username or not password:
        return JSONResponse(
            status_code=400,
            content={"error": "Nom d'utilisateur et mot de passe requis."},
        )

    # Look up user in database
    try:
        async with get_conn() as conn:
            user = await get_user_by_username(conn, username)
    except RuntimeError:
        return JSONResponse(
            status_code=503,
            content={"error": "Base de données hors ligne."},
        )

    if not user or not verify_password(password, user["password_hash"]):
        logger.warning("auth.login_failed", extra={"username": username})
        return JSONResponse(
            status_code=401,
            content={"error": "Identifiants incorrects."},
        )

    # Create session token and set cookie
    token = create_session_token(user["id"], user["username"], user["role"], user.get("workspace_id"))

    response = JSONResponse(content={
        "status": "ok",
        "user": {
            "id": user["id"],
            "username": user["username"],
            "role": user["role"],
            "display_name": user["display_name"] or user["username"],
            "workspace_id": user.get("workspace_id"),
            "workspace_name": user.get("workspace_name"),
        },
    })
    response.set_cookie(
        key=_COOKIE_NAME,
        value=token,
        max_age=_COOKIE_MAX_AGE,
        httponly=True,
        samesite="lax",
        secure=settings.secure_cookies,  # True when FRONTEND_URL is https://
    )

    # Update last_login
    try:
        async with get_conn() as conn:
            await update_last_login(conn, user["id"])
    except Exception:
        pass  # Non-critical

    logger.info("auth.login_ok", extra={"username": username, "role": user["role"]})
    return response


@router.post("/logout")
async def logout():
    """Clear the session cookie."""
    response = JSONResponse(content={"status": "ok"})
    response.delete_cookie(_COOKIE_NAME)
    return response


@router.get("/me")
async def get_current_user(request: Request):
    """Return the currently logged-in user, or 401 if not authenticated.

    The frontend calls this on page load to check if the session is still valid.
    """
    token = request.cookies.get(_COOKIE_NAME)
    if not token:
        return JSONResponse(status_code=401, content={"error": "Non authentifié."})

    user = decode_session_token(token)
    if not user:
        response = JSONResponse(status_code=401, content={"error": "Session expirée."})
        response.delete_cookie(_COOKIE_NAME)
        return response

    return {
        "user": {
            "id": user.id,
            "username": user.username,
            "role": user.role,
            "workspace_id": user.workspace_id,
        },
    }


@router.get("/check")
async def auth_check():
    """Returns whether auth is enabled (always True now)."""
    return {"auth_required": True}
