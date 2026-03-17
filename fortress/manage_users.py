"""Manage users — non-interactive CLI for bulk user operations.

Usage:
    python -m fortress.manage_users

All users share the same database (companies, contacts, etc.).
"""

import asyncio
import sys

import psycopg

from fortress.api.auth import hash_password
from fortress.config.settings import settings


async def list_users():
    """Print all users."""
    conn = await psycopg.AsyncConnection.connect(settings.db_url)
    try:
        cur = await conn.execute(
            "SELECT id, username, role, display_name, created_at, last_login FROM users ORDER BY id"
        )
        rows = await cur.fetchall()
        print(f"\n  {len(rows)} utilisateur(s) dans la base:\n")
        print(f"  {'ID':>4}  {'Username':<20}  {'Role':<8}  {'Display Name':<25}  {'Last Login'}")
        print("  " + "-" * 88)
        for r in rows:
            last = str(r[5])[:16] if r[5] else "jamais"
            print(f"  {r[0]:>4}  {r[1]:<20}  {r[2]:<8}  {r[3] or '-':<25}  {last}")
        print()
    finally:
        await conn.close()


async def create_user(username, password, role, display_name=""):
    """Create a user. Returns True if created, False if exists."""
    password_hash = hash_password(password)
    conn = await psycopg.AsyncConnection.connect(settings.db_url)
    try:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id              SERIAL          PRIMARY KEY,
                username        VARCHAR(50)     NOT NULL UNIQUE,
                password_hash   TEXT            NOT NULL,
                role            VARCHAR(20)     NOT NULL DEFAULT 'user',
                display_name    TEXT,
                created_at      TIMESTAMP       NOT NULL DEFAULT NOW(),
                last_login      TIMESTAMP
            )
        """)
        await conn.commit()
        try:
            await conn.execute(
                "INSERT INTO users (username, password_hash, role, display_name) VALUES (%s, %s, %s, %s)",
                (username, password_hash, role, display_name or username),
            )
            await conn.commit()
            return True
        except psycopg.errors.UniqueViolation:
            await conn.rollback()
            return False
    finally:
        await conn.close()


async def update_display_name(username, new_display_name):
    """Update display_name for a user."""
    conn = await psycopg.AsyncConnection.connect(settings.db_url)
    try:
        cur = await conn.execute(
            "UPDATE users SET display_name = %s WHERE username = %s RETURNING id",
            (new_display_name, username),
        )
        row = await cur.fetchone()
        await conn.commit()
        return row is not None
    finally:
        await conn.close()


async def run_all():
    """Execute the user management tasks."""
    print("\n  Fortress - Gestion des utilisateurs")
    db_host = settings.db_url.split("@")[-1] if "@" in settings.db_url else "local"
    print(f"  DB: {db_host}\n")

    # 1. List current users BEFORE changes
    print("=== AVANT modifications ===")
    await list_users()

    # 2. Update olivier's display name to "Olivier Haddad"
    ok = await update_display_name("olivier", "Olivier Haddad")
    if ok:
        print("  OK: olivier -> display_name = 'Olivier Haddad'")
    else:
        print("  WARN: utilisateur 'olivier' introuvable")

    # 3. Create jonathan (password: 3579C)
    created = await create_user("jonathan", "3579C", "user", "Jonathan")
    if created:
        print("  OK: Compte cree: jonathan (user)")
    else:
        print("  WARN: 'jonathan' existe deja")

    # 4. Create oliviercohen (password: 4680C)
    created = await create_user("oliviercohen", "4680C", "user", "Olivier Cohen")
    if created:
        print("  OK: Compte cree: oliviercohen (user) - display: Olivier Cohen")
    else:
        print("  WARN: 'oliviercohen' existe deja")

    # 5. List current users AFTER changes
    print("\n=== APRES modifications ===")
    await list_users()

    print("  INFO: Tous les utilisateurs partagent la meme base de donnees.")
    print("  Chaque utilisateur voit les memes entreprises, contacts et resultats.\n")


if __name__ == "__main__":
    asyncio.run(run_all())
