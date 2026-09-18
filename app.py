import os
import asyncio
import time
from urllib.parse import urlparse

import aiohttp
from flask import Flask, jsonify, Response
from flask_socketio import SocketIO
from fake_useragent import UserAgent

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")

MAX_TARGET_REQUESTS = int(os.getenv("MAX_TARGET_REQUESTS", "100000"))
MAX_CONCURRENCY = int(os.getenv("MAX_CONCURRENCY", "50"))
MAX_AGENTS = int(os.getenv("MAX_AGENTS", "20"))
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "4"))

ALLOWED_HOSTS = {
    x.strip().lower()
    for x in os.getenv("ALLOWED_HOSTS", "").split(",")
    if x.strip()
}

active_test = False
stop_requested = False


def is_allowed_url(url):
    try:
        p = urlparse(url)
        return (
            p.scheme in ("http", "https")
            and bool(p.hostname)
            and bool(ALLOWED_HOSTS)
            and p.hostname.lower() in ALLOWED_HOSTS
        )
    except Exception:
        return False


HTML = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>HTTP Test Panel</title>
<style>
body{margin:0;background:#0b1020;color:#e8ecf7;font-family:Arial,sans-serif}
.wrap{max-width:1000px;margin:25px auto;padding:15px}
.card{background:#121a2d;border:1px solid #293653;border-radius:14px;padding:18px;margin-bottom:15px}
h1{margin:0 0 7px}.muted{color:#91a0bc;font-size:13px}
.grid{display:grid;grid-template-columns:2fr 1fr 1fr 1fr;gap:10px}
label{display:block;color:#aebbd2;font-size:12px;margin-bottom:6px}
input{width:100%;padding:11px;border-radius:9px;border:1px solid #34425f;background:#0c1324;color:white}
button{padding:11px 18px;border:0;border-radius:9px;cursor:pointer;font-weight:bold;margin-top:12px;margin-right:7px}
.start{background:#36d399}.stop{background:#ff6b6b}
.stats{display:grid;grid-template-columns:repeat(5,1fr);gap:9px}
.stat{background:#0c1324;border-radius:9px;padding:12px}
.stat b{display:block;font-size:20px;margin-top:5px}
.log{height:360px;overflow:auto;background:#070b14;border-radius:9px;padding:12px;font:12px monospace;white-space:pre-wrap}
.ok{color:#54e39b}.bad{color:#ff8585}
@media(max-width:700px){.grid,.stats{grid-template-columns:1fr 1fr}.grid>:first-child{grid-column:1/-1}}
</style>
</head>
<body>
<div class="wrap">
<div class="card">
<h1>HTTP Test Panel</h1>
<div class="muted">Only domains listed in ALLOWED_HOSTS can be tested.</div>
</div>

<div class="card">
<div class="grid">
<div><label>URL</label><input id="url" placeholder="https://your-domain.com/"></div>
<div><label>Requests</label><input id="target" type="number" value="1000" min="1" max="100000"></div>
<div><label>Concurrency</label><input id="concurrency" type="number" value="10" min="1" max="50"></div>
<div><label>Agents</label><input id="agents" type="number" value="5" min="1" max="20"></div>
</div>
<button class="start" onclick="startTest()">Start</button>
<button class="stop" onclick="stopTest()">Stop</button>
</div>

<div class="card">
<div class="stats">
<div class="stat">Completed<b id="completed">0</b></div>
<div class="stat">200<b id="s200">0</b></div>
<div class="stat">403/429<b id="s429">0</b></div>
<div class="stat">5xx<b id="s5xx">0</b></div>
<div class="stat">RPS<b id="rps">0</b></div>
</div>
</div>

<div class="card">
<div class="muted">Live request log</div>
<div id="log" class="log"></div>
</div>
</div>

<script src="https://cdn.socket.io/4.7.5/socket.io.min.js"></script>
<script>
const socket=io(), box=document.getElementById("log");
function log(s,c=""){let x=document.createElement("div");x.className=c;x.textContent="["+new Date().toLocaleTimeString()+"] "+s;box.appendChild(x);while(box.children.length>300)box.removeChild(box.firstChild);box.scrollTop=box.scrollHeight}
function setv(id,v){document.getElementById(id).textContent=v}
function startTest(){
 let d={url:document.getElementById("url").value.trim(),target:+document.getElementById("target").value,concurrency:+document.getElementById("concurrency").value,agents:+document.getElementById("agents").value};
 log("Starting: "+d.url);socket.emit("start_test",d)
}
function stopTest(){socket.emit("stop_test");log("Stop requested.")}
socket.on("connect",()=>log("Connected.","ok"));
socket.on("disconnect",()=>log("Disconnected.","bad"));
socket.on("error",d=>log("ERROR: "+d.message,"bad"));
socket.on("update",d=>{
 setv("completed",d.completed+" / "+d.target);setv("s200",d.s200);setv("s429",d.s429);setv("s5xx",d.s5xx);setv("rps",d.rps);
 log("Progress "+d.completed+"/"+d.target+" | 200="+d.s200+" | 403/429="+d.s429+" | 5xx="+d.s5xx+" | RPS="+d.rps)
});
socket.on("finished",d=>{
 setv("completed",d.completed);setv("s200",d.s200);setv("s429",d.s429);setv("s5xx",d.s5xx);setv("rps",d.rps);
 log("Finished: "+d.completed+" requests, "+d.time+"s, RPS="+d.rps,"ok")
});
</script>
</body>
</html>"""


@app.route("/")
def index():
    return Response(HTML, mimetype="text/html")


@app.route("/health")
def health():
    return jsonify({"status": "ok", "active_test": active_test})


def run_async_test(url, target, concurrency, agents_count):
    global active_test, stop_requested
    active_test = True
    stop_requested = False
    completed = s200 = s429 = s5xx = failed = 0

    try:
        try:
            ua = UserAgent()
            agents = [ua.random for _ in range(agents_count)]
        except Exception:
            agents = ["authorized-test-client/1.0"]

        async def worker():
            nonlocal completed, s200, s429, s5xx, failed
            sem = asyncio.Semaphore(concurrency)
            connector = aiohttp.TCPConnector(
                limit=concurrency, limit_per_host=concurrency, ssl=False
            )
            timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)

            async with aiohttp.ClientSession(
                connector=connector, timeout=timeout
            ) as session:
                started = time.time()

                async def one_worker(i):
                    nonlocal completed, s200, s429, s5xx, failed
                    headers = {
                        "User-Agent": agents[i % len(agents)],
                        "X-Authorized-Test": "true"
                    }
                    while completed < target and not stop_requested:
                        async with sem:
                            if completed >= target or stop_requested:
                                break
                            try:
                                async with session.get(url, headers=headers) as r:
                                    completed += 1
                                    if r.status == 200:
                                        s200 += 1
                                    elif r.status in (403, 429):
                                        s429 += 1
                                    elif r.status in (500, 502, 503, 504):
                                        s5xx += 1
                                    if completed % 100 == 0 or completed == target:
                                        elapsed = time.time() - started
                                        socketio.emit("update", {
                                            "completed": completed,
                                            "target": target,
                                            "s200": s200,
                                            "s429": s429,
                                            "s5xx": s5xx,
                                            "ignored": failed,
                                            "rps": round(completed / elapsed, 2) if elapsed else 0
                                        })
                            except Exception:
                                failed += 1

                await asyncio.gather(
                    *(asyncio.create_task(one_worker(i)) for i in range(agents_count)),
                    return_exceptions=True
                )

                elapsed = time.time() - started
                socketio.emit("finished", {
                    "completed": completed, "s200": s200, "s429": s429,
                    "s5xx": s5xx, "ignored": failed,
                    "time": round(elapsed, 2),
                    "rps": round(completed / elapsed, 2) if elapsed else 0
                })

        loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(loop)
            loop.run_until_complete(worker())
        finally:
            loop.close()
    finally:
        active_test = False
        stop_requested = False


@socketio.on("start_test")
def handle_start(data):
    global active_test
    if active_test:
        socketio.emit("error", {"message": "A test is already running."})
        return

    url = str(data.get("url", "")).strip()
    if not is_allowed_url(url):
        socketio.emit("error", {
            "message": "URL is not in ALLOWED_HOSTS."
        })
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
        run_async_test, url, target, concurrency, agents
    )


@socketio.on("stop_test")
def handle_stop():
    global stop_requested
    stop_requested = True


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5000"))
    socketio.run(
        app, host="0.0.0.0", port=port,
        allow_unsafe_werkzeug=True
    )
