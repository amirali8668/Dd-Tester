import asyncio
import aiohttp
import time
import csv
from fake_useragent import UserAgent

# Configuration
TARGET_SUCCESSFUL_RESPONSES = 100000
NUM_AGENTS = 100
CONCURRENCY_LIMIT = 5000  # 5K concurrent capacity

# Counters
completed_responses = 0
success_200 = 0
rate_limited_429 = 0
worker_errors_5xx = 0
network_failures_ignored = 0
stop_event = asyncio.Event()
lock = asyncio.Lock()

ua = UserAgent()
DEVICE_AGENTS = [ua.random for _ in range(NUM_AGENTS)]

async def agent_task(agent_id, session, semaphore, url):
    global completed_responses, success_200, rate_limited_429, worker_errors_5xx, network_failures_ignored

    headers = {
        "User-Agent": DEVICE_AGENTS[agent_id % len(DEVICE_AGENTS)],
        "Accept": "*/*",
        "Connection": "keep-alive"
    }

    while not stop_event.is_set():
        async with lock:
            if completed_responses >= TARGET_SUCCESSFUL_RESPONSES:
                stop_event.set()
                break

        async with semaphore:
            if stop_event.is_set():
                break

            try:
                async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=3)) as response:
                    status = response.status

                    async with lock:
                        # فقط درخواست‌هایی که پاسخ از سرور دریافت کرده‌اند شمارش می‌شوند
                        completed_responses += 1
                        current_req = completed_responses

                        if status == 200:
                            success_200 += 1
                        elif status in [429, 403]:
                            rate_limited_429 += 1
                        elif status in [500, 502, 503, 504]:
                            worker_errors_5xx += 1

                        if current_req % 1000 == 0:
                            print(f"[PROGRESS] {current_req}/{TARGET_SUCCESSFUL_RESPONSES} valid responses received...")

                        if current_req >= TARGET_SUCCESSFUL_RESPONSES:
                            stop_event.set()
                            break

            except Exception:
                # درخواست‌های فیل شده به خاطر قطعی نت/تایم‌آوت نادیده گرفته می‌شوند
                async with lock:
                    network_failures_ignored += 1

async def main():
    print("=== MOBILE STABLE 5K BLASTER (EXCLUDING NETWORK FAILS) ===")
    url = input("Enter Worker URL: ").strip()
    if not url.startswith("http"):
        url = "https://" + url

    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
    connector = aiohttp.TCPConnector(limit=CONCURRENCY_LIMIT, ssl=False, ttl_dns_cache=300)

    start_time = time.time()

    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [asyncio.create_task(agent_task(i, session, semaphore, url)) for i in range(NUM_AGENTS)]
        try:
            await asyncio.gather(*tasks, return_exceptions=True)
        except KeyboardInterrupt:
            stop_event.set()

    elapsed_time = time.time() - start_time

    print("\n=== FINAL RESULTS ===")
    print(f"Valid Server Responses: {completed_responses}")
    print(f"Ignored Network Failures: {network_failures_ignored}")
    print(f"HTTP 200 OK: {success_200}")
    print(f"Cloudflare Rate Limit (429/403): {rate_limited_429}")
    print(f"Worker Errors (5xx): {worker_errors_5xx}")
    print(f"Elapsed Time: {elapsed_time:.2f} seconds")
    if elapsed_time > 0:
        print(f"Effective Speed: {completed_responses / elapsed_time:.2f} req/sec")

if __name__ == "__main__":
    asyncio.run(main())
