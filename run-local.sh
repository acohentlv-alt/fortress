#!/bin/bash
# Launch Fortress locally against local Postgres (free, no cloud). Then open http://localhost:8080
cd "$(dirname "$0")"
export DATABASE_URL="postgresql://alancohen@localhost:5432/fortress2"
export FRONTEND_URL="http://localhost:8080"
exec python3 -m uvicorn fortress.api.main:app --host 127.0.0.1 --port 8080
