"""Shared workspace access gate.

Used by notes.py, companies.py, and any other route that needs to check
whether a given SIREN is accessible to a user's workspace.

For MAPS entities: check companies.workspace_id directly.
For real SIRENs: check via batch_tags → batch_data.workspace_id.
"""

from __future__ import annotations

from fortress.api.db import fetch_one


async def siren_in_workspace(siren: str, workspace_id) -> bool:
    """Return True if the SIREN belongs to the given workspace.

    For MAPS entities, check companies.workspace_id directly.
    For real SIRENs, check via batch_tags → batch_data.workspace_id.
    """
    if siren.startswith("MAPS"):
        row = await fetch_one(
            "SELECT workspace_id FROM companies WHERE siren = %s", (siren,)
        )
        return row is not None and row.get("workspace_id") == workspace_id
    else:
        row = await fetch_one("""
            SELECT 1 FROM batch_tags bt
            JOIN batch_data bd ON bd.batch_id = bt.batch_id
            WHERE bt.siren = %s AND bd.workspace_id = %s
            LIMIT 1
        """, (siren, workspace_id))
        return row is not None
