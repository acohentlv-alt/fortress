"""Admin API routes — user management.

Admin-only endpoints for listing, creating, updating, and deactivating users.
All users share the same database (companies, contacts, etc.).
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, Query, Request
from fastapi.responses import JSONResponse

from fortress.api.auth import decode_session_token, hash_password
from fortress.api.db import fetch_all, fetch_one, get_conn

logger = logging.getLogger("fortress.api.admin")

router = APIRouter(prefix="/api/admin", tags=["admin"])

_COOKIE_NAME = "fortress_session"
_VALID_ROLES = ("admin", "head", "user")


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
    """List all active users with workspace info. Admin only."""
    admin = _get_admin(request)
    if not admin:
        return JSONResponse(status_code=403, content={"error": "Admin requis."})

    rows = await fetch_all(
        """SELECT u.id, u.username, u.role, u.display_name, u.created_at, u.last_login,
                  u.workspace_id, COALESCE(w.name, '') AS workspace_name, u.active
           FROM users u
           LEFT JOIN workspaces w ON w.id = u.workspace_id
           WHERE u.active = TRUE OR u.active IS NULL
           ORDER BY u.id"""
    )
    users = []
    for r in (rows or []):
        users.append({
            "id": r["id"],
            "username": r["username"],
            "role": r["role"],
            "display_name": r["display_name"],
            "created_at": r["created_at"].isoformat() if r.get("created_at") else None,
            "last_login": r["last_login"].isoformat() if r.get("last_login") else None,
            "workspace_id": r["workspace_id"],
            "workspace_name": r["workspace_name"],
            "active": r["active"],
        })
    return {"users": users, "count": len(users)}


@router.post("/users")
async def create_user(request: Request):
    """Create a new user. Admin only.

    Body: {"username": "...", "password": "...", "role": "user", "display_name": "...", "workspace_id": 1}
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
    workspace_id = body.get("workspace_id") or None
    if workspace_id is not None:
        try:
            workspace_id = int(workspace_id)
        except (ValueError, TypeError):
            workspace_id = None

    if not username or not password:
        return JSONResponse(status_code=400, content={"error": "Identifiant et mot de passe requis."})
    if role not in _VALID_ROLES:
        return JSONResponse(status_code=400, content={"error": "Rôle invalide. Les valeurs acceptées sont : admin, head, user."})
    if len(password) < 4:
        return JSONResponse(status_code=400, content={"error": "Mot de passe trop court (minimum 4 caractères)."})

    # Workspace validation per role
    if role == "head" and not workspace_id:
        return JSONResponse(status_code=400, content={"error": "Un responsable doit être associé à un espace de travail."})
    if role == "admin":
        workspace_id = None

    pw_hash = hash_password(password)

    async with get_conn() as conn:
        # Check if username already exists
        cur = await conn.execute("SELECT id FROM users WHERE username = %s", (username,))
        if await cur.fetchone():
            return JSONResponse(status_code=409, content={"error": f"L'identifiant '{username}' existe déjà."})

        await conn.execute(
            "INSERT INTO users (username, password_hash, role, display_name, workspace_id) VALUES (%s, %s, %s, %s, %s)",
            (username, pw_hash, role, display_name, workspace_id),
        )
        await conn.commit()

    logger.info("admin.user_created", extra={"username": username, "role": role, "by": admin.username})
    return {"created": True, "username": username, "role": role, "display_name": display_name}


@router.patch("/users/{user_id}")
async def update_user(user_id: int, request: Request):
    """Update a user's display_name, role, password, username, or workspace. Admin only.

    Body: {"display_name": "...", "role": "...", "password": "...", "username": "...", "workspace_id": 1}
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

    # Fetch current user state for context-aware validation
    async with get_conn() as conn:
        cur = await conn.execute(
            "SELECT id, username, role, workspace_id FROM users WHERE id = %s AND (active = TRUE OR active IS NULL)",
            (user_id,)
        )
        current = await cur.fetchone()

    if not current:
        return JSONResponse(status_code=404, content={"error": "Utilisateur introuvable."})

    current_role = current[2]
    current_workspace_id = current[3]

    # Determine final role (may be changing)
    new_role = None
    if "role" in body:
        new_role = (body["role"] or "").strip().lower()
        if new_role not in _VALID_ROLES:
            return JSONResponse(status_code=400, content={"error": "Rôle invalide. Les valeurs acceptées sont : admin, head, user."})
        updates.append("role = %s")
        params.append(new_role)

    final_role = new_role if new_role is not None else current_role

    # Workspace update
    if "workspace_id" in body:
        new_workspace_id = body["workspace_id"]
        if new_workspace_id is not None:
            try:
                new_workspace_id = int(new_workspace_id)
            except (ValueError, TypeError):
                new_workspace_id = None
        updates.append("workspace_id = %s")
        params.append(new_workspace_id)
        resolved_workspace_id = new_workspace_id
    else:
        resolved_workspace_id = current_workspace_id

    # Role-based workspace enforcement
    if final_role == "head" and not resolved_workspace_id:
        return JSONResponse(status_code=400, content={"error": "Un responsable doit être associé à un espace de travail."})
    if final_role == "admin":
        # Force workspace_id = NULL for admin users — remove any workspace update and replace with NULL
        ws_indices = [i for i, u in enumerate(updates) if "workspace_id" in u]
        if ws_indices:
            # Update the existing param in-place (updates and params always appended together)
            params[ws_indices[0]] = None
        else:
            updates.append("workspace_id = %s")
            params.append(None)

    if "display_name" in body:
        updates.append("display_name = %s")
        params.append(body["display_name"])

    if "password" in body and body["password"]:
        if len(body["password"]) < 4:
            return JSONResponse(status_code=400, content={"error": "Mot de passe trop court (minimum 4 caractères)."})
        updates.append("password_hash = %s")
        params.append(hash_password(body["password"]))

    if "username" in body and body["username"].strip():
        new_username = body["username"].strip()
        # Check uniqueness for the new username (ignore current user's own row)
        async with get_conn() as conn:
            cur = await conn.execute(
                "SELECT id FROM users WHERE username = %s AND id != %s",
                (new_username, user_id)
            )
            if await cur.fetchone():
                return JSONResponse(status_code=409, content={"error": f"L'identifiant '{new_username}' est déjà utilisé."})
        updates.append("username = %s")
        params.append(new_username)

    if not updates:
        return JSONResponse(status_code=400, content={"error": "Aucune modification à effectuer."})

    params.append(user_id)
    async with get_conn() as conn:
        cur = await conn.execute(
            f"UPDATE users SET {', '.join(updates)} WHERE id = %s AND (active = TRUE OR active IS NULL) RETURNING id, username, role, display_name",
            tuple(params),
        )
        row = await cur.fetchone()
        if not row:
            return JSONResponse(status_code=404, content={"error": "Utilisateur introuvable."})
        await conn.commit()

    return {"updated": True, "user": {"id": row[0], "username": row[1], "role": row[2], "display_name": row[3]}}


@router.delete("/users/{user_id}")
async def deactivate_user(user_id: int, request: Request):
    """Deactivate a user (soft delete). Admin only. Cannot deactivate yourself."""
    admin = _get_admin(request)
    if not admin:
        return JSONResponse(status_code=403, content={"error": "Admin requis."})

    if user_id == admin.id:
        return JSONResponse(status_code=400, content={"error": "Impossible de désactiver votre propre compte."})

    async with get_conn() as conn:
        cur = await conn.execute(
            "UPDATE users SET active = FALSE WHERE id = %s AND (active = TRUE OR active IS NULL) RETURNING username",
            (user_id,)
        )
        row = await cur.fetchone()
        if not row:
            return JSONResponse(status_code=404, content={"error": "Utilisateur introuvable ou déjà désactivé."})
        await conn.commit()

    return {"deactivated": True, "username": row[0]}


@router.get("/workspaces")
async def list_workspaces(request: Request):
    """List all workspaces with user count and head info. Admin only."""
    admin = _get_admin(request)
    if not admin:
        return JSONResponse(status_code=403, content={"error": "Admin requis."})

    rows = await fetch_all(
        """SELECT w.id, w.name, w.created_at,
                  COUNT(u.id) FILTER (WHERE u.active = TRUE OR u.active IS NULL) AS user_count,
                  (SELECT username FROM users WHERE workspace_id = w.id AND role = 'head' AND (active = TRUE OR active IS NULL) LIMIT 1) AS head_username
           FROM workspaces w
           LEFT JOIN users u ON u.workspace_id = w.id
           GROUP BY w.id, w.name, w.created_at
           ORDER BY w.name"""
    )
    return {"workspaces": [
        {
            "id": r["id"],
            "name": r["name"],
            "created_at": r["created_at"].isoformat() if r.get("created_at") else None,
            "user_count": r["user_count"] or 0,
            "head_username": r["head_username"],
        }
        for r in (rows or [])
    ]}


@router.post("/workspaces")
async def create_workspace(request: Request):
    """Create a new workspace. Admin only."""
    admin = _get_admin(request)
    if not admin:
        return JSONResponse(status_code=403, content={"error": "Admin requis."})

    try:
        body = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"error": "Corps invalide."})

    name = (body.get("name") or "").strip()
    if not name:
        return JSONResponse(status_code=400, content={"error": "Le nom de l'espace de travail est requis."})
    if len(name) > 100:
        return JSONResponse(status_code=400, content={"error": "Le nom ne peut pas dépasser 100 caractères."})

    async with get_conn() as conn:
        try:
            cur = await conn.execute(
                "INSERT INTO workspaces (name) VALUES (%s) RETURNING id, name, created_at",
                (name,)
            )
            row = await cur.fetchone()
            await conn.commit()
        except Exception as e:
            if "unique" in str(e).lower() or "duplicate" in str(e).lower():
                return JSONResponse(status_code=409, content={"error": f"L'espace de travail '{name}' existe déjà."})
            raise

    logger.info("admin.workspace_created", extra={"name": name, "by": admin.username})
    return {
        "created": True,
        "workspace": {
            "id": row[0],
            "name": row[1],
            "created_at": row[2].isoformat() if row[2] else None,
        }
    }


@router.patch("/workspaces/{workspace_id}")
async def update_workspace(workspace_id: int, request: Request):
    """Rename a workspace. Admin only."""
    admin = _get_admin(request)
    if not admin:
        return JSONResponse(status_code=403, content={"error": "Admin requis."})

    try:
        body = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"error": "Corps invalide."})

    name = (body.get("name") or "").strip()
    if not name:
        return JSONResponse(status_code=400, content={"error": "Le nom de l'espace de travail est requis."})
    if len(name) > 100:
        return JSONResponse(status_code=400, content={"error": "Le nom ne peut pas dépasser 100 caractères."})

    async with get_conn() as conn:
        # Check workspace exists
        cur = await conn.execute("SELECT id FROM workspaces WHERE id = %s", (workspace_id,))
        if not await cur.fetchone():
            return JSONResponse(status_code=404, content={"error": "Espace de travail introuvable."})

        try:
            cur = await conn.execute(
                "UPDATE workspaces SET name = %s WHERE id = %s RETURNING id, name, created_at",
                (name, workspace_id)
            )
            row = await cur.fetchone()
            await conn.commit()
        except Exception as e:
            if "unique" in str(e).lower() or "duplicate" in str(e).lower():
                return JSONResponse(status_code=409, content={"error": f"L'espace de travail '{name}' existe déjà."})
            raise

    logger.info("admin.workspace_updated", extra={"workspace_id": workspace_id, "name": name, "by": admin.username})
    return {
        "updated": True,
        "workspace": {
            "id": row[0],
            "name": row[1],
            "created_at": row[2].isoformat() if row[2] else None,
        }
    }


@router.delete("/workspaces/{workspace_id}")
async def delete_workspace(workspace_id: int, request: Request):
    """Delete a workspace. Admin only. Refuses if last workspace, has users, or has data."""
    admin = _get_admin(request)
    if not admin:
        return JSONResponse(status_code=403, content={"error": "Admin requis."})

    async with get_conn() as conn:
        # Check it's not the last workspace
        cur = await conn.execute("SELECT COUNT(*) FROM workspaces")
        row = await cur.fetchone()
        if row and row[0] <= 1:
            return JSONResponse(status_code=400, content={"error": "Impossible de supprimer le dernier espace de travail."})

        # Check workspace exists and get name
        cur = await conn.execute("SELECT name FROM workspaces WHERE id = %s", (workspace_id,))
        ws_row = await cur.fetchone()
        if not ws_row:
            return JSONResponse(status_code=404, content={"error": "Espace de travail introuvable."})
        ws_name = ws_row[0]

        # Check for active users
        cur = await conn.execute(
            "SELECT COUNT(*) FROM users WHERE workspace_id = %s AND (active = TRUE OR active IS NULL)",
            (workspace_id,)
        )
        row = await cur.fetchone()
        user_count = row[0] if row else 0
        if user_count > 0:
            return JSONResponse(status_code=400, content={"error": f"Impossible de supprimer : {user_count} utilisateur(s) encore assigné(s)."})

        # Check for data in batch_data
        cur = await conn.execute(
            "SELECT COUNT(*) FROM batch_data WHERE workspace_id = %s",
            (workspace_id,)
        )
        row = await cur.fetchone()
        if row and row[0] > 0:
            return JSONResponse(status_code=400, content={"error": "Impossible de supprimer : cet espace contient encore des données (batches, entreprises)."})

        # Safe to delete
        await conn.execute("DELETE FROM workspaces WHERE id = %s", (workspace_id,))
        await conn.commit()

    logger.info("admin.workspace_deleted", extra={"workspace_id": workspace_id, "name": ws_name, "by": admin.username})
    return {"deleted": True, "name": ws_name}


@router.get("/system-log")
async def get_system_log(
    request: Request,
    level: str = Query("all", description="all, error, warning"),
    period: str = Query("week", description="day, week, month, all"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    """System error log — admin only."""
    admin = _get_admin(request)
    if not admin:
        return JSONResponse(status_code=403, content={"error": "Admin requis."})

    interval_map = {"day": "1 day", "week": "7 days", "month": "30 days", "all": "10 years"}
    interval = interval_map.get(period, "7 days")

    level_filter = ""
    if level == "error":
        level_filter = "AND level IN ('ERROR', 'CRITICAL')"
    elif level == "warning":
        level_filter = "AND level = 'WARNING'"

    rows = await fetch_all(f"""
        SELECT id, level, source, message, traceback, path, created_at
        FROM system_log
        WHERE created_at >= NOW() - %s::interval
        {level_filter}
        ORDER BY created_at DESC
        LIMIT %s OFFSET %s
    """, (interval, limit, offset))

    count_row = await fetch_one(f"""
        SELECT COUNT(*) AS total FROM system_log
        WHERE created_at >= NOW() - %s::interval
        {level_filter}
    """, (interval,))
    total = (count_row or {}).get("total", 0)

    return {"entries": rows or [], "total": total, "period": period}


@router.delete("/system-log")
async def clear_system_log(request: Request):
    """Clear old system log entries. Admin only."""
    admin = _get_admin(request)
    if not admin:
        return JSONResponse(status_code=403, content={"error": "Admin requis."})

    async with get_conn() as conn:
        await conn.execute("DELETE FROM system_log WHERE created_at < NOW() - INTERVAL '7 days'")
        await conn.commit()

    return {"cleared": True}


@router.get("/deploy-status")
async def deploy_status(request: Request):
    """Check if it's safe to deploy.

    Returns running/queued job count so admin knows if any batches
    would be interrupted by a deploy.
    """
    admin = _get_admin(request)
    if not admin:
        return JSONResponse(status_code=403, content={"error": "Admin requis."})

    active_jobs = await fetch_all(
        """SELECT batch_id, batch_name, status, companies_scraped, batch_size, updated_at
           FROM batch_data
           WHERE status IN ('in_progress', 'queued')
           ORDER BY updated_at DESC"""
    )

    return {
        "running_jobs": len(active_jobs),
        "safe_to_deploy": len(active_jobs) == 0,
        "active_jobs": active_jobs,
        "message": "✅ Aucun batch en cours — déploiement sûr." if len(active_jobs) == 0
                   else f"⚠️ {len(active_jobs)} batch(s) en cours — attendez avant de déployer.",
    }
