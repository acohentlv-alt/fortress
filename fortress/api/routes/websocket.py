from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Request, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Dict, List, Any
import json
import logging

logger = logging.getLogger(__name__)
router = APIRouter()

_COOKIE_NAME = "fortress_session"


class ConnectionManager:
    def __init__(self):
        # Maps job_id to a list of connected WebSockets (existing per-job channel)
        self.active_connections: Dict[str, List[WebSocket]] = {}
        # Maps workspace_id (str) to a list of connected WebSockets.
        # Special key "__all__" receives all workspace broadcasts (admin).
        self.workspace_connections: Dict[str, List[WebSocket]] = {}

    # ── Per-job connections (existing) ──────────────────────────────────
    async def connect(self, websocket: WebSocket, job_id: str):
        await websocket.accept()
        if job_id not in self.active_connections:
            self.active_connections[job_id] = []
        self.active_connections[job_id].append(websocket)
        logger.info(f"WS Client connected to job {job_id}")

    def disconnect(self, websocket: WebSocket, job_id: str):
        if job_id in self.active_connections:
            if websocket in self.active_connections[job_id]:
                self.active_connections[job_id].remove(websocket)
            if not self.active_connections[job_id]:
                del self.active_connections[job_id]
        logger.info(f"WS Client disconnected from job {job_id}")

    async def broadcast(self, job_id: str, message: dict):
        if job_id in self.active_connections:
            to_remove = []
            for connection in self.active_connections[job_id]:
                try:
                    await connection.send_text(json.dumps(message))
                except Exception as e:
                    logger.error(f"Error sending WS message to client for job {job_id}: {e}")
                    to_remove.append(connection)

            for conn in to_remove:
                self.disconnect(conn, job_id)

    # ── Per-workspace connections (new) ──────────────────────────────────
    async def connect_workspace(self, websocket: WebSocket, workspace_key: str):
        await websocket.accept()
        if workspace_key not in self.workspace_connections:
            self.workspace_connections[workspace_key] = []
        self.workspace_connections[workspace_key].append(websocket)
        logger.info(f"WS Client connected to workspace {workspace_key}")

    def disconnect_workspace(self, websocket: WebSocket, workspace_key: str):
        if workspace_key in self.workspace_connections:
            if websocket in self.workspace_connections[workspace_key]:
                self.workspace_connections[workspace_key].remove(websocket)
            if not self.workspace_connections[workspace_key]:
                del self.workspace_connections[workspace_key]
        logger.info(f"WS Client disconnected from workspace {workspace_key}")

    async def broadcast_workspace(self, workspace_id: Any, message: dict):
        """Broadcast to all subscribers of a workspace, and also to __all__ (admin)."""
        payload = json.dumps(message)
        targets = [str(workspace_id), "__all__"]
        for key in targets:
            if key not in self.workspace_connections:
                continue
            to_remove = []
            for ws in self.workspace_connections[key]:
                try:
                    await ws.send_text(payload)
                except Exception as e:
                    logger.error(f"Error broadcasting to workspace {key}: {e}")
                    to_remove.append(ws)
            for ws in to_remove:
                self.disconnect_workspace(ws, key)


manager = ConnectionManager()


def _verify_session_cookie(websocket: WebSocket):
    """Verify fortress_session cookie from WebSocket handshake.
    Returns SessionUser or None."""
    from fortress.api.auth import decode_session_token
    token = websocket.cookies.get(_COOKIE_NAME)
    if not token:
        return None
    return decode_session_token(token)


@router.websocket("/ws/enrich/{job_id}")
async def websocket_endpoint(websocket: WebSocket, job_id: str):
    await manager.connect(websocket, job_id)
    try:
        while True:
            # We don't expect messages from the client in this flow
            # but we need to keep the connection alive
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket, job_id)


@router.websocket("/ws/workspace/{workspace_id}")
async def workspace_websocket_endpoint(websocket: WebSocket, workspace_id: str):
    """Workspace-scoped WebSocket for batch completion notifications.

    workspace_id: numeric workspace ID for regular users,
                  or "all" for admin (receives all workspace broadcasts).

    Auth: validates fortress_session cookie before accepting connection.
    Admin (role='admin') may connect with workspace_id="all".
    Regular users must match their own workspace_id.
    """
    user = _verify_session_cookie(websocket)
    if not user:
        await websocket.close(code=4403)
        return

    # Admin can subscribe to "all"
    if workspace_id == "all":
        if user.role != "admin":
            await websocket.close(code=4403)
            return
        workspace_key = "__all__"
    else:
        # Non-admin: enforce workspace match
        if user.role != "admin":
            try:
                if user.workspace_id != int(workspace_id):
                    await websocket.close(code=4403)
                    return
            except (ValueError, TypeError):
                await websocket.close(code=4403)
                return
        workspace_key = str(workspace_id)

    await manager.connect_workspace(websocket, workspace_key)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect_workspace(websocket, workspace_key)


class NotifyBatchCompleteBody(BaseModel):
    workspace_id: int
    batch_id: str
    batch_name: str
    count: int


@router.post("/api/internal/notify-batch-complete")
async def notify_batch_complete(body: NotifyBatchCompleteBody, request: Request):
    """Internal endpoint: pipeline calls this when a batch completes.
    Only accepts connections from localhost.
    """
    client_host = request.client.host if request.client else ""
    if client_host not in ("127.0.0.1", "localhost", "::1"):
        raise HTTPException(status_code=403, detail="Internal endpoint")

    await manager.broadcast_workspace(
        body.workspace_id,
        {
            "type": "batch_complete",
            "batch_id": body.batch_id,
            "batch_name": body.batch_name,
            "count": body.count,
        },
    )
    return {"ok": True}
