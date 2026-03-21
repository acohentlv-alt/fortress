import pytest
from unittest.mock import patch, MagicMock, AsyncMock

@pytest.fixture(autouse=True)
def mock_db_global():
    # Global mocks for all DB interactions
    m_fetch_one = AsyncMock(return_value=None)
    m_fetch_all = AsyncMock(return_value=[])
    m_execute = AsyncMock()
    m_conn = AsyncMock()
    
    # Psycopg 3: conn.cursor() is sync, returns an async context manager
    m_cursor = AsyncMock()
    m_cursor_acm = MagicMock()
    m_cursor_acm.__aenter__ = AsyncMock(return_value=m_cursor)
    m_cursor_acm.__aexit__ = AsyncMock(return_value=None)
    m_conn.cursor = MagicMock(return_value=m_cursor_acm)
    
    # Mock connection context manager
    m_cm = MagicMock()
    m_cm.__aenter__ = AsyncMock(return_value=m_conn)
    m_cm.__aexit__ = AsyncMock(return_value=None)
    
    # Mock Crawler internals (source modules)
    m_curl_resp = MagicMock()
    m_curl_resp.status_code = 200
    m_curl_resp.text = "<html><body>contact@realbusiness.fr</body></html>"
    
    m_curl_client = AsyncMock()
    m_curl_client.get = AsyncMock(return_value=m_curl_resp)
    m_curl_client.__aenter__ = AsyncMock(return_value=m_curl_client)
    m_curl_client.__aexit__ = AsyncMock(return_value=None)
    
    m_curl_class = MagicMock(return_value=m_curl_client)

    modules_to_patch = [
        "fortress.api.db",
        "fortress.api.routes.auth",
        "fortress.api.routes.notes",
        "fortress.api.routes.activity",
        "fortress.api.routes.companies",
        "fortress.api.routes.batch",
        "fortress.api.routes.admin",
    ]
    
    stack = []
    try:
        # Patch source modules for crawler and enricher
        stack.append(patch("fortress.module_c.curl_client.CurlClient", m_curl_class))
        stack.append(patch("fortress.module_b.contact_parser.extract_emails", MagicMock(return_value=["contact@realbusiness.fr"])))
        stack.append(patch("fortress.module_b.contact_parser.extract_phones", MagicMock(return_value=[])))
        stack.append(patch("fortress.module_b.contact_parser.extract_social_links", MagicMock(return_value={})))
        stack.append(patch("fortress.module_d.enricher._best_email", MagicMock(return_value="contact@realbusiness.fr")))
        stack.append(patch("fortress.module_d.enricher._best_phone", MagicMock(return_value=None)))
        
        for p in stack:
            p.start()

        for mod in modules_to_patch:
            # Patch fetch_one, fetch_all, execute, get_conn if they exist in the module
            for func_name, mock_obj in [
                ("fetch_one", m_fetch_one),
                ("fetch_all", m_fetch_all),
                ("execute", m_execute),
                ("get_conn", m_cm),
            ]:
                try:
                    p = patch(f"{mod}.{func_name}", mock_obj)
                    stack.append(p)
                    p.start()
                except AttributeError:
                    pass

        for mod in modules_to_patch:
            # Patch fetch_one, fetch_all, execute, get_conn if they exist in the module
            for func_name, mock_obj in [
                ("fetch_one", m_fetch_one),
                ("fetch_all", m_fetch_all),
                ("execute", m_execute),
                ("get_conn", m_cm),
            ]:
                try:
                    p = patch(f"{mod}.{func_name}", mock_obj)
                    stack.append(p)
                    p.start()
                except AttributeError:
                    pass
        
        # Additional lifespan mocks
        p1 = patch("fortress.api.db.pool_status", return_value={"connected": True})
        p2 = patch("fortress.api.db.init_pool", AsyncMock())
        p3 = patch("fortress.api.db.close_pool", AsyncMock())
        for p in [p1, p2, p3]:
            stack.append(p)
            p.start()
            
        yield {
            "fetch_one": m_fetch_one,
            "fetch_all": m_fetch_all,
            "conn": m_conn,
            "cursor": m_cursor
        }
    finally:
        for p in reversed(stack):
            try: p.stop()
            except: pass

@pytest.fixture
def mock_auth():
    # Patch decode_session_token in main.py (middleware)
    with patch("fortress.api.main.decode_session_token") as mock_decode:
        yield mock_decode

@pytest.fixture(scope="session")
def anyio_backend():
    return "asyncio"
