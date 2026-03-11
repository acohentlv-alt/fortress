"""Database utilities for Fortress.

CLI tools (sirene ingesters) use get_pool/init_db from connection.py.
The API server uses fortress.api.db instead.
"""

from fortress.database.connection import close_pool, get_pool, init_db

__all__ = ["get_pool", "close_pool", "init_db"]
