import pytest
from unittest.mock import patch, MagicMock, AsyncMock
from httpx import AsyncClient, ASGITransport
from fortress.api.main import app

@pytest.mark.asyncio
async def test_get_activity_no_auth():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/api/activity")
    assert response.status_code == 401

@pytest.mark.asyncio
async def test_get_activity_non_admin(mock_auth):
    mock_auth.return_value = MagicMock(id=2, username="user", role="user")
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test", cookies={"fortress_session": "fake"}) as ac:
        response = await ac.get("/api/activity")
    assert response.status_code == 403

@pytest.mark.asyncio
async def test_get_activity_admin_ok(mock_auth):
    mock_auth.return_value = MagicMock(id=1, username="admin", role="admin")
    with patch("fortress.api.routes.activity.fetch_all", AsyncMock(return_value=[])):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test", cookies={"fortress_session": "fake"}) as ac:
            response = await ac.get("/api/activity")
        assert response.status_code == 200
        assert response.json()["entries"] == []
