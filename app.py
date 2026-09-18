import os
import asyncio
import time
from urllib.parse import urlparse

import aiohttp
from flask import Flask, jsonify, render_template
from flask_socketio import SocketIO
from fake_useragent import UserAgent

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")

# Safety limits for authorized testing only.
MAX_TARGET_REQUESTS = int(os.getenv("MAX_TARGET_REQUESTS", "10000"))
MAX_CONCURRENCY = int(os.getenv("MAX_CONCURRENCY", "50"))
MAX_AGENTS = int(os.getenv("MAX_AGENTS", "20"))
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "4"))

# Optional comma-separated host allowlist.
# Example:
# ALLOWED_HOSTS=example.com,www.example.com
_allowed_hosts = {
    h.strip().lower()
    for h in os.getenv("ALLOWED_HOSTS", "").split(",")
    if h.strip()
}

active_test = False


def is_allowed_url(url: str) -> bool:
    try:
        parsed = urlparse(url)
        if parsed.scheme not in ("http", "https") or not parsed.hostname:
            return False

        # Require an explicit allowlist in production.
        if not _allowed_hosts:
            return False

        host = parsed.hostname.lower()
        return host in _allowed_hosts
    except Exception:
        return False


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/health")
def health():
    return jsonify({"status": "ok", "active_test": active_test})


def run_async_test(url, target_reqs, concurrency, num_agents):
    global active_test
    active_test = True

    completed = 0
    status_200 = 0
    status_429 = 0
    status_5xx = 0
    failed_ignored = 0

    try:
        ua = UserAgent()
        agents = [ua.random for _ in range(num_agents)]
    except Exception:
        agents = ["authorized-test-client/1.0"]

    async def worker():
        nonlocal completed, status_200, status_429, status_5xx, failed_ignored

        semaphore = asyncio.Semaphore(concurrency)
        connector = aiohttp.TCPConnector(
            limit=concurrency,
            limit_per_host=concurrency,
            ssl=False,
        )

        timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)

        async with aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
        ) as session:
            start_time = time.time()

            async def send_req(agent_id):
                nonlocal completed, status_200, status_429, status_5xx, failed_ignored

                headers = {
                    "User-Agent": agents[agent_id % len(agents)],
                    "X-Authorized-Test": "true",
                }

                while completed < target_reqs and active_test:
                    async with semaphore:
                        try:
                            async with session.get(
                                url,
                                headers=headers,
                            ) as resp:
                                completed += 1
                                st = resp.status

                                if st == 200:
                                    status_200 += 1
                                elif st in (403, 429):
                                    status_429 += 1
                                elif st in (500, 502, 503, 504):
                                    status_5xx += 1

                                if completed % 100 == 0:
                                    elapsed = time.time() - start_time
                                    socketio.emit("update", {
                                        "completed": completed,
                                        "target": target_reqs,
                                        "s200": status_200,
                                        "s429": status_429,
                                        "s5xx": status_5xx,
                                        "ignored": failed_ignored,
                                        "rps": round(
                                            completed / elapsed, 2
                                        ) if elapsed > 0 else 0,
                                    })

                        except Exception:
                            failed_ignored += 1

            tasks = [
                asyncio.create_task(send_req(i))
                for i in range(num_agents)
            ]
            await asyncio.gather(*tasks, return_exceptions=True)

            elapsed = time.time() - start_time
            socketio.emit("finished", {
                "completed": completed,
                "s200": status_200,
                "s429": status_429,
                "s5xx": status_5xx,
                "ignored": failed_ignored,
                "time": round(elapsed, 2),
                "rps": round(completed / elapsed, 2) if elapsed > 0 else 0,
            })

    loop = asyncio.new_event_loop()
    try:
        asyncio.set_event_loop(loop)
        loop.run_until_complete(worker())
    finally:
        loop.close()
        active_test = False


@socketio.on("start_test")
def handle_start(data):
    global active_test

    if active_test:
        socketio.emit("error", {"message": "A test is already running."})
        return

    url = str(data.get("url", "")).strip()

    if not is_allowed_url(url):
        socketio.emit(
            "error",
            {"message": "URL is not in ALLOWED_HOSTS."},
        )
        return

    try:
        target = int(data.get("target", 1000))
        concurrency = int(data.get("concurrency", 10))
        agents = int(data.get("agents", 5))
    except (TypeError, ValueError):
        socketio.emit("error", {"message": "Invalid numeric parameters."})
        return

    target = max(1, min(target, MAX_TARGET_REQUESTS))
    concurrency = max(1, min(concurrency, MAX_CONCURRENCY))
    agents = max(1, min(agents, MAX_AGENTS))

    socketio.start_background_task(
        run_async_test,
        url,
        target,
        concurrency,
        agents,
    )


@socketio.on("stop_test")
def handle_stop():
    global active_test
    active_test = False


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5000"))
    socketio.run(app, host="0.0.0.0", port=port)
