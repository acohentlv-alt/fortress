# Frontend Agent — Login Page Prompt

## Context

The backend auth system has been built. The current login page (screenshot attached) shows the OLD system — a single "Clé API" field. This must be replaced with a **username + password** login form.

## What exists (backend-ready)

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/auth/login` | POST | Body: `{"username": "...", "password": "..."}` → Returns `{"status": "ok", "user": {id, username, role, display_name}}` + sets `fortress_session` HttpOnly cookie |
| `/api/auth/logout` | POST | Clears the session cookie |
| `/api/auth/me` | GET | Returns current user if session is valid, or 401 if not |
| `/api/auth/check` | GET | Returns `{"auth_required": true}` (always true now) |

**Session cookie:** `fortress_session` — HttpOnly, 24h expiry, set automatically by the browser. No JS token management needed.

## What to build

### 1. Replace login.js

Delete the current API key form. Build instead:

```
┌───────────────────────────────────────┐
│            🏰                         │
│         Fortress                      │
│                                       │
│   ┌─────────────────────────────┐     │
│   │ Nom d'utilisateur           │     │
│   └─────────────────────────────┘     │
│   ┌─────────────────────────────┐     │
│   │ Mot de passe            👁️  │     │
│   └─────────────────────────────┘     │
│                                       │
│   [ Se connecter                ]     │
│                                       │
│   ❌ Identifiants incorrects.         │
│      (only visible on error)          │
└───────────────────────────────────────┘
```

**Requirements:**
- Username field (text input, autofocus)
- Password field with show/hide toggle (👁️ icon)
- "Se connecter" button (loading spinner while checking)
- Error message below button (French: "Identifiants incorrects")
- Enter key submits the form
- On success: redirect to `#/` (dashboard)
- No "Sign up" link — accounts are created by the admin via CLI

### 2. Update app.js auth guard

The router should check if the user is logged in before rendering any page:
1. On page load, call `GET /api/auth/me`
2. If 401 → redirect to login page
3. If 200 → proceed, store user info in memory for the session

### 3. Add logout button

In the sidebar or header, add a "Déconnexion" button:
- Calls `POST /api/auth/logout`
- Redirects to login page
- Clears any stored user state

### 4. Update api.js

- Remove old `X-API-Key` header logic
- No new headers needed — the session cookie is sent automatically by `fetch` when using `credentials: 'same-origin'` (default) or `credentials: 'include'`
- Add `loginUser(username, password)`, `logoutUser()`, `getCurrentUser()` functions
- Update `extractApiError()` to handle 401 by redirecting to login

### 5. Display logged-in user

Show the user's display name in the sidebar or header:
- `"👑 Administrateur"` for admin
- `"👤 Opérateur"` for user

## Test accounts

| Username | Password | Role | Display Name |
|----------|----------|------|-------------|
| `admin` | `Fortress2026!` | admin | Administrateur |
| `operateur` | `Fortress2026` | user | Opérateur |
| `invite` | `FortressView` | user | Invité |

## Hard rules

1. Vanilla JS + CSS only — no frameworks
2. All text in French
3. Dark mode design system (existing tokens)
4. Inter font (`var(--font-family)`)
5. Use `registerCleanup()` for any intervals
6. `showToast()` for success/error feedback
7. No `localStorage` for auth tokens — the cookie handles it

## Testing

1. Start API: `python3 -m fortress.api.main` (port 8080)
2. Open `http://localhost:8080` — should redirect to login
3. Try wrong credentials → "Identifiants incorrects"
4. Login as admin → dashboard loads, "👑 Administrateur" shown
5. Refresh page → still logged in (cookie persists)
6. Click "Déconnexion" → back to login
7. Try accessing `#/search` without login → redirected to login
