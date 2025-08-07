@echo off
REM Batch file to start the Celery worker
uv run celery -A celery_tasks worker --loglevel=info
