import os
import asyncio
import random
import time
from urllib.parse import urlparse

import aiohttp
from flask import Flask, Response, jsonify
from flask_socketio import SocketIO, emit
from fake_useragent import UserAgent


# =========================================================
# APP CONFIG
# =========================================================

app = Flask(__name__)

socketio = SocketIO(
    app,
    cors_allowed_origins="*",
    async_mode="threading",
)

MAX_TARGET_REQUESTS = int(
    os.getenv("MAX_TARGET_REQUESTS", "100000")
)

MAX_CONCURRENCY = int(
    os.getenv("MAX_CONCURRENCY", "50")
)

MAX_AGENTS = int(
    os.getenv("MAX_AGENTS", "20")
)

REQUEST_TIMEOUT = float(
    os.getenv("REQUEST_TIMEOUT", "4")
)


# =========================================================
# GLOBAL STATE
# =========================================================

active_test = False
stop_requested = False


# =========================================================
# USER AGENTS
# =========================================================

try:
    ua = UserAgent()

    USER_AGENTS = [
        ua.random
        for _ in range(MAX_AGENTS)
    ]

except Exception:
    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 Chrome/140.0 Safari/537.36",

        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 Chrome/140.0 Safari/537.36",

        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/605.1.15 Version/18.0 Safari/605.1.15",
    ]


# =========================================================
# HTML PANEL
# =========================================================

HTML = r"""
<!DOCTYPE html>
<html lang="en">

<head>
    <meta charset="UTF-8">

    <meta
        name="viewport"
        content="width=device-width, initial-scale=1.0"
    >

    <title>HTTP Test Panel</title>

    <script
        src="https://cdn.socket.io/4.7.5/socket.io.min.js">
    </script>

    <style>

        * {
            box-sizing: border-box;
        }

        body {
            margin: 0;
            background: #0b0f17;
            color: #e8edf5;
            font-family:
                Arial,
                Helvetica,
                sans-serif;
        }

        .container {
            width: min(1100px, 94%);
            margin: 30px auto;
        }

        .header {
            margin-bottom: 20px;
        }

        .header h1 {
            margin: 0 0 7px;
            font-size: 28px;
        }

        .header p {
            margin: 0;
            color: #8994a5;
        }

        .card {
            background: #111827;
            border: 1px solid #202b3d;
            border-radius: 14px;
            padding: 20px;
            margin-bottom: 18px;
            box-shadow:
                0 10px 30px rgba(0, 0, 0, .18);
        }

        label {
            display: block;
            margin-bottom: 7px;
            color: #aeb8c7;
            font-size: 14px;
        }

        input {
            width: 100%;
            background: #0b1220;
            color: white;
            border: 1px solid #2b3850;
            border-radius: 9px;
            padding: 12px;
            outline: none;
            font-size: 14px;
        }

        input:focus {
            border-color: #667eea;
        }

        .grid {
            display: grid;
            grid-template-columns:
                repeat(3, 1fr);
            gap: 14px;
            margin-top: 14px;
        }

        .buttons {
            display: flex;
            gap: 10px;
            margin-top: 18px;
        }

        button {
            border: 0;
            border-radius: 9px;
            padding: 12px 20px;
            cursor: pointer;
            color: white;
            font-weight: bold;
        }

        #startBtn {
            background: #2563eb;
        }

        #stopBtn {
            background: #dc2626;
        }

        button:disabled {
            opacity: .5;
            cursor: not-allowed;
        }

        .stats {
            display: grid;
            grid-template-columns:
                repeat(5, 1fr);
            gap: 12px;
        }

        .stat {
            background: #0b1220;
            border: 1px solid #202b3d;
            border-radius: 10px;
            padding: 15px;
        }

        .stat-title {
            color: #8d99aa;
            font-size: 13px;
            margin-bottom: 8px;
        }

        .stat-value {
            font-size: 23px;
            font-weight: bold;
        }

        .log-title {
            margin-bottom: 10px;
            font-weight: bold;
        }

        #log {
            height: 360px;
            overflow-y: auto;
            background: #070b12;
            border: 1px solid #202b3d;
            border-radius: 9px;
            padding: 12px;
            font-family:
                Consolas,
                Monaco,
                monospace;
            font-size: 12px;
            white-space: pre-wrap;
        }

        .log-line {
            margin-bottom: 5px;
            color: #b8c2d1;
        }

        .status {
            display: inline-block;
            margin-top: 12px;
            padding: 6px 10px;
            border-radius: 20px;
            background: #172033;
            color: #aeb8c7;
            font-size: 13px;
        }

        @media (max-width: 800px) {

            .grid {
                grid-template-columns: 1fr;
            }

            .stats {
                grid-template-columns:
                    repeat(2, 1fr);
            }

        }

    </style>
</head>


<body>

<div class="container">

    <div class="header">

        <h1>HTTP Test Panel</h1>

        <p>
            Live HTTP request testing dashboard
        </p>

        <div id="status"
             class="status">
            Disconnected
        </div>

    </div>


    <div class="card">

        <label>
            Target URL
        </label>

        <input
            id="url"
            type="text"
            placeholder="https://example.com"
        >


        <div class="grid">

            <div>

                <label>
                    Requests
                </label>

                <input
                    id="requests"
                    type="number"
                    value="100000"
                    min="1"
                    max="100000"
                >

            </div>


            <div>

                <label>
                    Concurrency
                </label>

                <input
                    id="concurrency"
                    type="number"
                    value="50"
                    min="1"
                    max="50"
                >

            </div>


            <div>

                <label>
                    User Agents
                </label>

                <input
                    id="agents"
                    type="number"
                    value="20"
                    min="1"
                    max="20"
                >

            </div>

        </div>


        <div class="buttons">

            <button
                id="startBtn"
                onclick="startTest()">
                START
            </button>

            <button
                id="stopBtn"
                onclick="stopTest()"
                disabled>
                STOP
            </button>

        </div>

    </div>


    <div class="card">

        <div class="stats">

            <div class="stat">

                <div class="stat-title">
                    Completed
                </div>

                <div
                    id="completed"
                    class="stat-value">
                    0
                </div>

            </div>


            <div class="stat">

                <div class="stat-title">
                    HTTP 200
                </div>

                <div
                    id="success"
                    class="stat-value">
                    0
                </div>

            </div>


            <div class="stat">

                <div class="stat-title">
                    403 / 429
                </div>

                <div
                    id="blocked"
                    class="stat-value">
                    0
                </div>

            </div>


            <div class="stat">

                <div class="stat-title">
                    5xx
                </div>

                <div
                    id="serverErrors"
                    class="stat-value">
                    0
                </div>

            </div>


            <div class="stat">

                <div class="stat-title">
                    RPS
                </div>

                <div
                    id="rps"
                    class="stat-value">
                    0
                </div>

            </div>

        </div>

    </div>


    <div class="card">

        <div class="log-title">
            Live Logs
        </div>

        <div id="log"></div>

    </div>

</div>


<script>

const socket = io();

const statusElement =
    document.getElementById("status");

const startButton =
    document.getElementById("startBtn");

const stopButton =
    document.getElementById("stopBtn");


function addLog(message) {

    const log =
        document.getElementById("log");

    const now =
        new Date().toLocaleTimeString();

    const line =
        document.createElement("div");

    line.className = "log-line";

    line.textContent =
        "[" + now + "] " + message;

    log.appendChild(line);

    log.scrollTop =
        log.scrollHeight;

}


function setRunning(running) {

    startButton.disabled = running;
    stopButton.disabled = !running;

}


function resetStats() {

    document.getElementById("completed")
        .textContent = "0";

    document.getElementById("success")
        .textContent = "0";

    document.getElementById("blocked")
        .textContent = "0";

    document.getElementById("serverErrors")
        .textContent = "0";

    document.getElementById("rps")
        .textContent = "0";

}


function startTest() {

    const url =
        document.getElementById("url")
        .value.trim();

    const requests =
        Number(
            document.getElementById("requests")
            .value
        );

    const concurrency =
        Number(
            document.getElementById("concurrency")
            .value
        );

    const agents =
        Number(
            document.getElementById("agents")
            .value
        );


    if (!url) {

        addLog("ERROR: Please enter a URL.");

        return;

    }


    resetStats();

    document.getElementById("log")
        .innerHTML = "";

    setRunning(true);

    addLog("Starting test...");

    socket.emit(
        "start_test",
        {
            url: url,
            target_reqs: requests,
            concurrency: concurrency,
            num_agents: agents
        }
    );

}


function stopTest() {

    socket.emit("stop_test");

    addLog("Stop requested...");

}


socket.on("connect", function() {

    statusElement.textContent =
        "Connected";

    addLog("Connected to server.");

});


socket.on("disconnect", function() {

    statusElement.textContent =
        "Disconnected";

    addLog("Disconnected from server.");

});


socket.on("update", function(data) {

    if (data.completed !== undefined) {

        document.getElementById("completed")
            .textContent =
            data.completed.toLocaleString();

    }

    if (data.success !== undefined) {

        document.getElementById("success")
            .textContent =
            data.success.toLocaleString();

    }

    if (data.blocked !== undefined) {

        document.getElementById("blocked")
            .textContent =
            data.blocked.toLocaleString();

    }

    if (data.server_errors !== undefined) {

        document.getElementById("serverErrors")
            .textContent =
            data.server_errors.toLocaleString();

    }

    if (data.rps !== undefined) {

        document.getElementById("rps")
            .textContent =
            Number(data.rps).toFixed(2);

    }

    if (data.message) {

        addLog(data.message);

    }

});


socket.on("finished", function(data) {

    setRunning(false);

    addLog(
        "Finished. " +
        "Completed: " +
        (data.completed || 0).toLocaleString()
    );

});


socket.on("error", function(data) {

    setRunning(false);

    addLog(
        "ERROR: " +
        (data.message || "Unknown error")
    );

});


</script>

</body>

</html>
"""


# =========================================================
# ROUTES
# =========================================================

@app.route("/")
def index():

    return Response(
        HTML,
        mimetype="text/html"
    )


@app.route("/health")
def health():

    return jsonify({
        "status": "ok",
        "active_test": active_test
    })


# =========================================================
# URL VALIDATION
# =========================================================

def validate_url(url):

    try:

        parsed = urlparse(url)

        if parsed.scheme not in (
            "http",
            "https"
        ):
            return False

        if not parsed.hostname:
            return False

        return True

    except Exception:

        return False


# =========================================================
# ASYNC TEST
# =========================================================

async def run_async_test(
    url,
    target_reqs,
    concurrency,
    num_agents
):

    global active_test
    global stop_requested

    completed = 0
    success = 0
    blocked = 0
    server_errors = 0

    started_at = time.monotonic()

    semaphore = asyncio.Semaphore(
        concurrency
    )

    timeout = aiohttp.ClientTimeout(
        total=REQUEST_TIMEOUT
    )


    connector = aiohttp.TCPConnector(
        limit=concurrency,
        limit_per_host=concurrency,
        ssl=False
    )


    async with aiohttp.ClientSession(
        timeout=timeout,
        connector=connector
    ) as session:


        async def one_request():

            nonlocal completed
            nonlocal success
            nonlocal blocked
            nonlocal server_errors

            global stop_requested

            if stop_requested:
                return


            async with semaphore:

                try:

                    headers = {
                        "User-Agent":
                            random.choice(
                                USER_AGENTS[:max(
                                    1,
                                    min(
                                        num_agents,
                                        len(USER_AGENTS)
                                    )
                                )]
                            ),
                        "Accept":
                            "*/*",
                        "Connection":
                            "keep-alive"
                    }


                    async with session.get(
                        url,
                        headers=headers,
                        allow_redirects=True
                    ) as response:

                        status = response.status

                        # Read response body so the
                        # connection can be reused.
                        await response.read()


                        if status == 200:

                            success += 1

                        elif status in (
                            403,
                            429
                        ):

                            blocked += 1

                        elif 500 <= status <= 599:

                            server_errors += 1


                except Exception:

                    server_errors += 1


                finally:

                    completed += 1


        while (
            completed < target_reqs
            and not stop_requested
        ):

            remaining = target_reqs - completed

            batch_size = min(
                concurrency,
                remaining
            )


            tasks = [
                asyncio.create_task(
                    one_request()
                )
                for _ in range(batch_size)
            ]


            await asyncio.gather(
                *tasks,
                return_exceptions=True
            )


            elapsed = (
                time.monotonic()
                - started_at
            )


            rps = (
                completed / elapsed
                if elapsed > 0
                else 0
            )


            socketio.emit(
                "update",
                {
                    "completed": completed,
                    "success": success,
                    "blocked": blocked,
                    "server_errors":
                        server_errors,
                    "rps": rps,
                    "message":
                        f"Progress: "
                        f"{completed:,}/"
                        f"{target_reqs:,} | "
                        f"RPS: {rps:.2f}"
                }
            )


    elapsed = (
        time.monotonic()
        - started_at
    )


    rps = (
        completed / elapsed
        if elapsed > 0
        else 0
    )


    active_test = False


    socketio.emit(
        "finished",
        {
            "completed": completed,
            "success": success,
            "blocked": blocked,
            "server_errors":
                server_errors,
            "rps": rps,
            "stopped":
                stop_requested
        }
    )


    stop_requested = False


# =========================================================
# SOCKET EVENTS
# =========================================================

@socketio.on("start_test")
def start_test(data):

    global active_test
    global stop_requested


    if active_test:

        emit(
            "error",
            {
                "message":
                    "A test is already running."
            }
        )

        return


    data = data or {}


    url = str(
        data.get("url", "")
    ).strip()


    if not validate_url(url):

        emit(
            "error",
            {
                "message":
                    "Invalid URL. Use http:// or https://."
            }
        )

        return


    try:

        target_reqs = int(
            data.get(
                "target_reqs",
                MAX_TARGET_REQUESTS
            )
        )

        concurrency = int(
            data.get(
                "concurrency",
                MAX_CONCURRENCY
            )
        )

        num_agents = int(
            data.get(
                "num_agents",
                MAX_AGENTS
            )
        )

    except (
        ValueError,
        TypeError
    ):

        emit(
            "error",
            {
                "message":
                    "Invalid numeric values."
            }
        )

        return


    target_reqs = max(
        1,
        min(
            target_reqs,
            MAX_TARGET_REQUESTS
        )
    )


    concurrency = max(
        1,
        min(
            concurrency,
            MAX_CONCURRENCY
        )
    )


    num_agents = max(
        1,
        min(
            num_agents,
            MAX_AGENTS
        )
    )


    active_test = True
    stop_requested = False


    emit(
        "update",
        {
            "message":
                f"Test started: {url}"
        }
    )


    socketio.start_background_task(
        run_test_wrapper,
        url,
        target_reqs,
        concurrency,
        num_agents
    )


@socketio.on("stop_test")
def stop_test():

    global stop_requested

    if active_test:

        stop_requested = True

        emit(
            "update",
            {
                "message":
                    "Stopping test..."
            }
        )

    else:

        emit(
            "update",
            {
                "message":
                    "No active test."
            }
        )


# =========================================================
# BACKGROUND WRAPPER
# =========================================================

def run_test_wrapper(
    url,
    target_reqs,
    concurrency,
    num_agents
):

    global active_test

    try:

        asyncio.run(
            run_async_test(
                url,
                target_reqs,
                concurrency,
                num_agents
            )
        )

    except Exception as e:

        active_test = False

        socketio.emit(
            "error",
            {
                "message":
                    f"Test error: {e}"
            }
        )


# =========================================================
# MAIN
# =========================================================

if __name__ == "__main__":

    port = int(
        os.getenv(
            "PORT",
            "5000"
        )
    )


    print(
        f"Starting server on port {port}"
    )


    socketio.run(
        app,
        host="0.0.0.0",
        port=port,
        allow_unsafe_werkzeug=True
    )
