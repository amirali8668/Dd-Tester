import os
import asyncio
import aiohttp
import time
from flask import Flask, render_template
from flask_socketio import SocketIO, emit
from fake_useragent import UserAgent

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="eventlet")

active_test = False

@app.route("/")
def index():
    return render_template("index.html")

def run_async_test(url, target_reqs, concurrency, num_agents):
    global active_test
    active_test = True

    completed = 0
    status_200 = 0
    status_429 = 0
    status_5xx = 0
    failed_ignored = 0

    ua = UserAgent()
    agents = [ua.random for _ in range(num_agents)]

    async def worker():
        nonlocal completed, status_200, status_429, status_5xx, failed_ignored
        semaphore = asyncio.Semaphore(concurrency)
        connector = aiohttp.TCPConnector(limit=concurrency, ssl=False)

        async with aiohttp.ClientSession(connector=connector) as session:
            start_time = time.time()

            async def send_req(agent_id):
                nonlocal completed, status_200, status_429, status_5xx, failed_ignored
                headers = {"User-Agent": agents[agent_id % len(agents)]}

                while completed < target_reqs and active_test:
                    async with semaphore:
                        try:
                            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=4)) as resp:
                                completed += 1
                                st = resp.status
                                if st == 200: status_200 += 1
                                elif st in [429, 403]: status_429 += 1
                                elif st in [500, 502, 503, 504]: status_5xx += 1

                                if completed % 500 == 0:
                                    elapsed = time.time() - start_time
                                    socketio.emit("update", {
                                        "completed": completed,
                                        "target": target_reqs,
                                        "s200": status_200,
                                        "s429": status_429,
                                        "s5xx": status_5xx,
                                        "ignored": failed_ignored,
                                        "rps": round(completed / elapsed, 2) if elapsed > 0 else 0
                                    })
                        except Exception:
                            failed_ignored += 1

            tasks = [asyncio.create_task(send_req(i)) for i in range(num_agents)]
            await asyncio.gather(*tasks, return_exceptions=True)

            elapsed = time.time() - start_time
            socketio.emit("finished", {
                "completed": completed,
                "s200": status_200,
                "s429": status_429,
                "s5xx": status_5xx,
                "ignored": failed_ignored,
                "time": round(elapsed, 2),
                "rps": round(completed / elapsed, 2) if elapsed > 0 else 0
            })

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(worker())
    active_test = False

@socketio.on("start_test")
def handle_start(data):
    global active_test
    if not active_test:
        url = data.get("url")
        target = int(data.get("target", 100000))
        concurrency = int(data.get("concurrency", 5000))
        agents = int(data.get("agents", 100))
        socketio.start_background_task(run_async_test, url, target, concurrency, agents)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    socketio.run(app, host="0.0.0.0", port=port)
