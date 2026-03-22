"""Setup users — interactive CLI to create Fortress user accounts.

Usage:
    python -m fortress.setup_users

Creates the users table if not exists, then prompts for username, password, role.
Can be run multiple times safely (uses INSERT ... ON CONFLICT DO NOTHING).
"""

import asyncio
import getpass
import sys

import psycopg

from fortress.api.auth import hash_password
from fortress.config.settings import settings


async def create_user(username: str, password: str, role: str, display_name: str = "") -> bool:
    """Insert a user into the database. Returns True if created, False if exists."""
    password_hash = hash_password(password)

    conn = await psycopg.AsyncConnection.connect(settings.db_url)
    try:
        # Ensure table exists
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id              SERIAL          PRIMARY KEY,
                username        VARCHAR(50)     NOT NULL UNIQUE,
                password_hash   TEXT            NOT NULL,
                role            VARCHAR(20)     NOT NULL DEFAULT 'user',
                display_name    TEXT,
                created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
                last_login      TIMESTAMPTZ
            )
        """)
        await conn.commit()

        # Insert user
        try:
            await conn.execute(
                """INSERT INTO users (username, password_hash, role, display_name)
                   VALUES (%s, %s, %s, %s)""",
                (username, password_hash, role, display_name or username),
            )
            await conn.commit()
            return True
        except psycopg.errors.UniqueViolation:
            await conn.rollback()
            return False
    finally:
        await conn.close()


def main():
    """Interactive user creation."""
    print("\n🏰 Fortress — Création de compte utilisateur\n")
    print(f"   Base de données: {settings.db_url.split('@')[-1]}\n")

    while True:
        username = input("Nom d'utilisateur: ").strip()
        if not username:
            print("  ❌ Le nom ne peut pas être vide.\n")
            continue

        password = getpass.getpass("Mot de passe: ")
        if len(password) < 4:
            print("  ❌ Mot de passe trop court (minimum 4 caractères).\n")
            continue

        password_confirm = getpass.getpass("Confirmer le mot de passe: ")
        if password != password_confirm:
            print("  ❌ Les mots de passe ne correspondent pas.\n")
            continue

        role = input("Rôle (admin/user) [user]: ").strip().lower() or "user"
        if role not in ("admin", "user"):
            print("  ❌ Rôle invalide. Choisissez 'admin' ou 'user'.\n")
            continue

        display_name = input(f"Nom affiché [{username}]: ").strip() or username

        created = asyncio.run(create_user(username, password, role, display_name))
        if created:
            emoji = "👑" if role == "admin" else "👤"
            print(f"\n  ✅ {emoji} Compte créé: {username} ({role})\n")
        else:
            print(f"\n  ⚠️  L'utilisateur '{username}' existe déjà.\n")

        another = input("Créer un autre compte ? (o/n) [n]: ").strip().lower()
        if another != "o":
            break

    print("\n🏰 Terminé. Démarrez l'API avec: python3 -m fortress.api.main\n")


if __name__ == "__main__":
    main()
