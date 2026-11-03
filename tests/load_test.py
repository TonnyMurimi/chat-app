"""
Load test: opens N clients (split across both nodes), each sends M messages
at the same time, then checks that every message reached every client.

Usage:  python load_test.py 20 10      (20 clients, 10 messages each)
"""
import asyncio
import json
import sys
import time

import websockets

NODES = ["ws://localhost:8001/ws", "ws://localhost:8002/ws"]


async def client(i, msgs, stats, start):
    url = f"{NODES[i % len(NODES)]}?user=load{i}"
    async with websockets.connect(url) as ws:
        await ws.recv()                      # welcome message
        stats["connected"] += 1

        async def reader():
            async for raw in ws:
                if json.loads(raw).get("type") == "chat":
                    stats["received"] += 1

        task = asyncio.create_task(reader())
        await start.wait()                   # everyone sends at the same moment
        for k in range(msgs):
            await ws.send(json.dumps({"text": f"msg {k} from load{i}"}))
            stats["sent"] += 1
            await asyncio.sleep(0.01)
        await asyncio.sleep(2)               # let the last deliveries arrive
        task.cancel()


async def main():
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 20
    m = int(sys.argv[2]) if len(sys.argv) > 2 else 10
    stats = {"connected": 0, "sent": 0, "received": 0}
    start = asyncio.Event()
    tasks = [asyncio.create_task(client(i, m, stats, start)) for i in range(n)]

    for _ in range(100):
        if stats["connected"] >= n:
            break
        await asyncio.sleep(0.1)
    print(f"Connected clients : {stats['connected']}/{n}")

    expected = n * m * n                     # every message goes to every client
    t0 = time.time()
    start.set()
    while stats["received"] < expected and time.time() - t0 < 20:
        await asyncio.sleep(0.05)
    elapsed = time.time() - t0

    print(f"Messages sent     : {stats['sent']}")
    print(f"Deliveries        : {stats['received']} / {expected} expected")
    print(f"Time taken        : {elapsed:.2f} s")
    print(f"Throughput        : {stats['received'] / elapsed:.0f} deliveries/s")
    await asyncio.gather(*tasks, return_exceptions=True)


asyncio.run(main())
