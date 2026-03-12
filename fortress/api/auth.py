"""Fortress authentication — session-based auth with bcrypt password hashing.

Provides:
  - Password hashing / verification (bcrypt)
  - Session token management (itsdangerous TimedSerializer)
  - FastAPI dependencies: get_current_user, require_admin

Session flow:
  1. POST /api/auth/login → validates credentials → returns session cookie
  2. All /api/* requests → middleware reads cookie → attaches user to request.state
  3. POST /api/auth/logout → clears cookie

Session tokens are signed with SESSION_SECRET (from settings) and expire after 24h.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone

import bcrypt
from itsdangerous import TimedSerializer, BadSignature, SignatureExpired

from fortress.config.settings import settings

logger = logging.getLogger("fortress.auth")

# Session token lifetime: 24 hours
_SESSION_MAX_AGE = 86400

# Lazy-init serializer (needs settings.session_secret)
_serializer: TimedSerializer | None = None


def _get_serializer() -> TimedSerializer:
    global _serializer
    if _serializer is None:
        _serializer = TimedSerializer(settings.session_secret)
    return _serializer


# ---------------------------------------------------------------------------
# Password helpers
# ---------------------------------------------------------------------------

def hash_password(password: str) -> str:
    """Hash a password with bcrypt. Returns the hash as a UTF-8 string."""
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def verify_password(password: str, password_hash: str) -> bool:
    """Check a password against a bcrypt hash."""
    try:
        return bcrypt.checkpw(password.encode("utf-8"), password_hash.encode("utf-8"))
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Session tokens
# ---------------------------------------------------------------------------

def create_session_token(user_id: int, username: str, role: str) -> str:
    """Create a signed session token containing user identity."""
    s = _get_serializer()
    return s.dumps({"uid": user_id, "usr": username, "role": role})


@dataclass
class SessionUser:
    """Decoded user from a valid session token."""
    id: int
    username: str
    role: str  # 'admin' | 'user'

    @property
    def is_admin(self) -> bool:
        return self.role == "admin"


def decode_session_token(token: str) -> SessionUser | None:
    """Decode and validate a session token. Returns None if invalid/expired."""
    s = _get_serializer()
    try:
        data = s.loads(token, max_age=_SESSION_MAX_AGE)
        return SessionUser(
            id=data["uid"],
            username=data["usr"],
            role=data["role"],
        )
    except (BadSignature, SignatureExpired, KeyError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Database helpers (used by routes and setup script)
# ---------------------------------------------------------------------------

async def get_user_by_username(conn, username: str) -> dict | None:
    """Fetch a user row by username. Returns dict or None."""
    import psycopg.rows
    cur = await conn.execute(
        "SELECT id, username, password_hash, role, display_name FROM users WHERE username = %s",
        (username,),
    )
    row = await cur.fetchone()
    if not row:
        return None
    return {
        "id": row[0],
        "username": row[1],
        "password_hash": row[2],
        "role": row[3],
        "display_name": row[4],
    }


async def update_last_login(conn, user_id: int) -> None:
    """Update last_login timestamp for a user."""
    await conn.execute(
        "UPDATE users SET last_login = NOW() WHERE id = %s",
        (user_id,),
    )
    await conn.commit()
