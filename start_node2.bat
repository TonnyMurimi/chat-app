@echo off
cd /d "%~dp0server"
set NODE_ID=node2
set PEERS=ws://localhost:8001/peer
python -m uvicorn main:app --host 0.0.0.0 --port 8002
