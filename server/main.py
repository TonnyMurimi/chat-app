"""
Distributed Chat - server node
Every node is the same program. Each node is told (via environment variables):
  NODE_ID : a name for this node, e.g. node1
  PEERS   : comma-separated peer URLs, e.g. ws://localhost:8002/peer

How the distribution works

* Clients connect to ANY node at  /ws?user=<name>
* Each node opens an OUTBOUND websocket to every peer's /peer endpoint and
  forwards every message it receives from its own clients (full mesh).
* A node that receives a message on /peer delivers it to its own local
  clients only (it does NOT forward it again, so there are no loops).
* Message ids are remembered, so a duplicate is never delivered twice.
* If a peer dies, the outbound link retries every 2 seconds until it is back.
* Messages are appended to chat_<NODE_ID>.log (persistence). The last 50
  public messages are replayed to every client that connects.
"""
import asyncio
import json
import os
import time
import uuid
from collections import deque
from contextlib import asynccontextmanager
from pathlib import Path

import websockets
from fastapi import FastAPI, WebSocket
from fastapi.responses import FileResponse

# configuration
NODE_ID = os.environ.get("NODE_ID", "node1")
PEERS = [p.strip() for p in os.environ.get("PEERS", "").split(",") if p.strip()]
BASE = Path(__file__).parent
LOG_FILE = BASE / f"chat_{NODE_ID}.log"
INDEX_FILE = BASE.parent / "static" / "index.html"
HISTORY_SIZE = 50

#node state
local_clients: dict[str, WebSocket] = {}     # username -> websocket (this node)
remote_users: dict[str, list[str]] = {}      # peer node id -> its usernames
peer_links: dict[str, object] = {}           # peer url -> live outbound socket
seen_ids: deque = deque(maxlen=2000)         # message ids already handled
history: deque = deque(maxlen=HISTORY_SIZE)  # recent public messages


# persistence
def log_message(msg: dict) -> None:
    """Append one message to this node's log file (one JSON object per line)."""
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(msg) + "\n")


def load_history() -> None:
    """On startup, reload recent public messages from the log file."""
    if not LOG_FILE.exists():
        return
    with open(LOG_FILE, encoding="utf-8") as f:
        for line in f:
            try:
                m = json.loads(line)
            except ValueError:
                continue
            if m.get("type") == "chat" and not m.get("to"):
                history.append(m)
                seen_ids.append(m["id"])


def record(msg: dict) -> None:
    """Remember a message: dedupe id, log it, keep public ones as history."""
    seen_ids.append(msg["id"])
    log_message(msg)
    if not msg.get("to"):
        history.append(msg)


# helpers
def all_users() -> list[str]:
    """Everyone online across ALL nodes."""
    users = set(local_clients)
    for names in remote_users.values():
        users.update(names)
    return sorted(users)


async def send_json(ws: WebSocket, data: dict) -> None:
    try:
        await ws.send_text(json.dumps(data))
    except Exception:
        pass


async def broadcast_local(data: dict) -> None:
    for ws in list(local_clients.values()):
        await send_json(ws, data)


async def send_to_peers(data: dict) -> None:
    text = json.dumps(data)
    for url, link in list(peer_links.items()):
        try:
            await link.send(text)
        except Exception as e:
            print(f"[{NODE_ID}] could not send to {url}: {e}")


async def announce_presence() -> None:
    """Tell peers who is on this node, and refresh our own clients' user list."""
    await send_to_peers({"type": "presence", "node": NODE_ID,
                         "users": sorted(local_clients)})
    await broadcast_local({"type": "users", "users": all_users()})


async def deliver(msg: dict) -> None:
    """Hand a message to the right LOCAL clients."""
    target = msg.get("to")
    if target:  # private message: only recipient and sender
        for name in {target, msg["user"]}:
            ws = local_clients.get(name)
            if ws:
                await send_json(ws, msg)
    else:       # public message: everyone on this node
        await broadcast_local(msg)


# peer links
async def peer_loop(url: str) -> None:
    """Keep an outbound connection to one peer alive forever (auto-reconnect)."""
    was_up = False
    while True:
        try:
            async with websockets.connect(url) as link:
                peer_links[url] = link
                was_up = True
                print(f"[{NODE_ID}] connected to peer {url}")
                await link.send(json.dumps({"type": "hello", "node": NODE_ID}))
                await link.send(json.dumps({"type": "presence", "node": NODE_ID,
                                            "users": sorted(local_clients)}))
                async for _ in link:   # we never expect data; this detects a close
                    pass
        except Exception:
            pass
        finally:
            peer_links.pop(url, None)
        if was_up:
            print(f"[{NODE_ID}] LOST peer {url} - retrying every 2s")
            was_up = False
        await asyncio.sleep(2)


@asynccontextmanager
async def lifespan(app: FastAPI):
    load_history()
    print(f"[{NODE_ID}] starting. peers = {PEERS or 'none'}")
    tasks = [asyncio.create_task(peer_loop(u)) for u in PEERS]
    yield
    for t in tasks:
        t.cancel()


app = FastAPI(lifespan=lifespan)


@app.get("/")
async def index():
    return FileResponse(INDEX_FILE)


@app.get("/status")
async def status():
    """Handy for demos and the report: shows the state of this node."""
    return {"node": NODE_ID, "local_users": sorted(local_clients),
            "remote_users": remote_users, "peers_connected": list(peer_links)}


#  peer endpoint
@app.websocket("/peer")
async def peer_endpoint(ws: WebSocket):
    """Other server nodes send us their clients' messages here."""
    await ws.accept()
    peer_node = None
    try:
        while True:
            data = json.loads(await ws.receive_text())
            kind = data.get("type")
            if kind == "hello":
                peer_node = data["node"]
            elif kind == "presence":
                peer_node = data["node"]
                remote_users[peer_node] = data["users"]
                await broadcast_local({"type": "users", "users": all_users()})
            elif kind == "chat" and data["id"] not in seen_ids:
                record(data)
                await deliver(data)
    except Exception:
        pass   # peer disconnected
    finally:
        if peer_node and remote_users.pop(peer_node, None) is not None:
            print(f"[{NODE_ID}] peer {peer_node} went away")
            await broadcast_local({"type": "users", "users": all_users()})


#  client endpoint
@app.websocket("/ws")
async def client_endpoint(ws: WebSocket, user: str = ""):
    """Browser clients connect here."""
    await ws.accept()
    user = user.strip()[:20]
    if not user or user in all_users():
        await send_json(ws, {"type": "error",
                             "text": "That username is empty or already in use."})
        await ws.close(code=4000)
        return

    local_clients[user] = ws
    print(f"[{NODE_ID}] {user} joined")
    await send_json(ws, {"type": "welcome", "node": NODE_ID,
                         "history": list(history), "users": all_users()})
    await announce_presence()

    try:
        while True:
            data = json.loads(await ws.receive_text())
            text = str(data.get("text", "")).strip()[:500]
            target = data.get("to") or None
            if not text:
                continue
            if target and target not in all_users():
                await send_json(ws, {"type": "error",
                                     "text": f"User '{target}' is not online."})
                continue
            msg = {"type": "chat", "id": uuid.uuid4().hex, "user": user,
                   "text": text, "to": target, "ts": time.time(), "node": NODE_ID}
            record(msg)
            await deliver(msg)          # to our own clients
            await send_to_peers(msg)    # to every other node
    except Exception:
        pass   # client closed the tab / lost connection
    finally:
        if local_clients.get(user) is ws:
            del local_clients[user]
            print(f"[{NODE_ID}] {user} left")
            await announce_presence()
