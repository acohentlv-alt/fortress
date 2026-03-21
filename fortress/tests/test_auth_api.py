import pytest
from httpx import AsyncClient, ASGITransport
from fortress.api.main import app
from fortress.api.auth import SessionUser

@pytest.mark.asyncio
async def test_session_user_dataclass():
    user = SessionUser(id=1, username="testadmin", role="admin")
    assert user.id == 1
    assert user.username == "testadmin"
    assert user.role == "admin"

@pytest.mark.asyncio
async def test_auth_me_no_session():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/api/auth/me")
    assert response.status_code == 401

@pytest.mark.asyncio
async def test_login_invalid_credentials():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.post("/api/auth/login", json={"username": "wrong", "password": "wrong"})
    assert response.status_code == 401
