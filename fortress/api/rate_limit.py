"""Rate limiting — shared module for login endpoint protection.

Extracted from main.py to avoid circular imports with auth.py.
Both main.py and auth.py import from here.
"""

from slowapi import Limiter
from slowapi.util import get_remote_address


def _render_proxy_key(request):
    """Rate limit key: use X-Forwarded-For on Render (behind proxy), fall back to client host."""
    fwd = request.headers.get("x-forwarded-for", "")
    if fwd:
        return fwd.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


limiter = Limiter(key_func=_render_proxy_key)
