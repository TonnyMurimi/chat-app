# Distributed Chat Application

A real-time chat system that runs on two (or more) cooperating server nodes.
Built with Python, FastAPI and WebSockets (WebSockets run over TCP).

## Features
- Multiple clients, multiple server nodes, messages relayed between nodes
- Automatic client failover (the browser switches node when its server dies)
- Peer links auto-reconnect when a dead node comes back
- Message persistence (per-node log file; last 50 messages replayed on join)
- Extras: usernames, private messages, live online-users list across nodes, timestamps, /status endpoint

## Requirements
Python 3.9+ and: `pip install fastapi uvicorn websockets`

## Run (Windows)
1. Open two Command Prompt windows in the chat-app folder.
2. Window 1: `start_node1.bat` (serves http://localhost:8001)
3. Window 2: `start_node2.bat` (serves http://localhost:8002)
4. Open http://localhost:8001 in one browser tab and http://localhost:8002 in another.
5. Enter different names and chat.

## Using the chat
- Type a message and press Enter to send to everyone.
- Private message: `/msg alice hello`, or click a name in "Online now".
- The header shows which node you are connected to.

## Demonstrating failover
1. Join from a tab on node1 and another on node2, and exchange messages.
2. Stop node1 (Ctrl+C in its window). Its tab shows "Connection lost" and reconnects to node2 automatically.
3. Start node1 again: the nodes reconnect to each other within about 2 seconds.

## Load test
With both nodes running: `python tests\load_test.py 20 10`
(20 clients, 10 messages each; reports deliveries and throughput).

## Project layout
    chat-app/
      server/main.py          server node (client endpoint /ws, peer endpoint /peer)
      server/requirements.txt
      static/index.html       browser UI with failover logic
      tests/load_test.py      load simulation
      start_node1.bat, start_node2.bat

## Architecture summary
Full-mesh peer relay: each node keeps an outbound WebSocket to every other node
and forwards each message from its own clients. Messages received from a peer
are delivered locally only (no loops) and de-duplicated by message id.
Clients hold a list of node addresses and rotate to the next one on disconnect.
