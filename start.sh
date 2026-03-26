#!/bin/bash
cd "$(dirname "$0")"
python3 -m uvicorn fortress.api.main:app --port 8080 --reload
