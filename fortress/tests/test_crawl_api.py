import pytest
from unittest.mock import patch, MagicMock, AsyncMock
from httpx import AsyncClient, ASGITransport
from fortress.api.main import app

@pytest.mark.asyncio
async def test_crawl_website_auth_required():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.post("/api/companies/123456789/crawl-website")
    assert response.status_code == 401

@pytest.mark.asyncio
async def test_crawl_website_success(mock_auth, mock_db_global):
    mock_auth.return_value = MagicMock(id=1, username="admin", role="admin")
    # First fetch_one: company check
    # Second fetch_one: website lookup
    mock_db_global["fetch_one"].side_effect = [
        {"siren": "123456789", "denomination": "Test Co", "departement": "75"},
        {"website": "http://realbusiness.fr"}
    ]
    
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test", cookies={"fortress_session": "fake"}) as ac:
        response = await ac.post("/api/companies/123456789/crawl-website")
    
    print(f"DEBUG CRAWL: {response.status_code} {response.text}")
    assert response.status_code == 200
    data = response.json()
    assert "message" in data
    assert "Enrichissement terminé" in data["message"]
    assert data["extracted"]["email"] == "contact@realbusiness.fr"
