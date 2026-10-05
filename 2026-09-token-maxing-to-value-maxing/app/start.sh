#!/bin/bash
set -e
cd backend
python -m uvicorn main:app --host 0.0.0.0 --port 8000
