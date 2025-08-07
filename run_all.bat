@echo off
REM Start Celery worker
start cmd /k "uv run celery -A celery_tasks worker --pool=solo --loglevel=info"

REM Start Flower monitoring tool
start cmd /k "uv run celery -A celery_tasks flower --port=5555  --basic-auth=liron:123"
