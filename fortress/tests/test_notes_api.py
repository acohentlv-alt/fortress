import pytest
from unittest.mock import patch, MagicMock, AsyncMock
from httpx import AsyncClient, ASGITransport
from fortress.api.main import app

@pytest.mark.asyncio
async def test_add_note_no_auth():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.post("/api/notes/123456789", json={"text": "Hello"})
    assert response.status_code == 401

@pytest.mark.asyncio
async def test_get_notes_empty_or_not_found(mock_auth, mock_db_global):
    # Implementation currently returns 200 even if company doesn't exist
    mock_db_global["fetch_all"].return_value = []
    with patch("fortress.api.auth.decode_session_token", return_value=MagicMock(id=1, username="test", role="admin")):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test", cookies={"fortress_session": "fake"}) as ac:
            response = await ac.get("/api/notes/999999999")
        assert response.status_code == 200
        assert response.json()["count"] == 0

@pytest.mark.asyncio
async def test_add_note_success(mock_auth, mock_db_global):
    mock_auth.return_value = MagicMock(id=1, username="test", role="admin")
    mock_db_global["fetch_one"].return_value = {"siren": "123456789"} # Company exists
    mock_db_global["conn"].execute.return_value = AsyncMock()
    
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test", cookies={"fortress_session": "fake"}) as ac:
        response = await ac.post("/api/notes/123456789", json={"text": "Hello world"})
    
    assert response.status_code == 201
    assert response.json()["text"] == "Hello world"

@pytest.mark.asyncio
async def test_add_note_company_not_found(mock_auth, mock_db_global):
    mock_auth.return_value = MagicMock(id=1, username="test", role="admin")
    mock_db_global["fetch_one"].return_value = None # Company does NOT exist
    
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test", cookies={"fortress_session": "fake"}) as ac:
        response = await ac.post("/api/notes/999999999", json={"text": "Hello"})
    
    assert response.status_code == 404
