#!/usr/bin/env python3
"""Шаг 2: где узлы начинают отказывать.

Прошлый замер показал потери: на 8 параллельных запросов узел отдал только
3000 блоков из 8000. Значит параллелить бесконечно нельзя — надо знать предел.
Меряем: сколько запросов проходит и какая выходит скорость при 1,2,4,8 потоках
к одному узлу, с фиксацией ПРИЧИН отказа.
"""
import json
import time
import urllib.request
import concurrent.futures as cf
import collections

TIMEOUT = 25
BATCH = 1000
NODES = [
    "http://node.monerodevs.org:18089",
    "http://opennode.xmr-tw.org:18089",
    "http://xmr-node.cakewallet.com:18081",
    "http://nodes.hashvault.pro:18081",
]


def rpc(node, method, params=None):
    body = json.dumps({"jsonrpc": "2.0", "id": "0", "method": method,
                       "params": params or {}}).encode()
    req = urllib.request.Request(node + "/json_rpc", data=body,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
        return json.load(r)


def fetch(node, start, count):
    t0 = time.time()
    try:
        r = rpc(node, "get_block_headers_range",
                {"start_height": start, "end_height": start + count - 1})
        if "error" in r:
            return 0, time.time() - t0, "rpc:" + str(r["error"].get("message", "?"))[:28]
        return len(r.get("result", {}).get("headers", [])), time.time() - t0, "ok"
    except Exception as e:
        return 0, time.time() - t0, type(e).__name__


def run(node, threads, base):
    t0 = time.time()
    with cf.ThreadPoolExecutor(max_workers=threads) as ex:
        futs = [ex.submit(fetch, node, base + i * BATCH, BATCH) for i in range(threads)]
        res = [f.result() for f in futs]
    dt = time.time() - t0
    got = sum(r[0] for r in res)
    reasons = collections.Counter(r[2] for r in res)
    return got, dt, reasons


if __name__ == "__main__":
    h = rpc(NODES[0], "get_info")["result"]["height"]
    base = h - 60000
    node = NODES[0]
    print(f"узел {node}, пачка {BATCH} блоков\n")
    print(f"{'потоков':>8} {'запрошено':>10} {'получено':>9} {'время':>7} {'блоков/с':>9}  причины отказов")
    for th in (1, 2, 4, 8):
        got, dt, reasons = run(node, th, base)
        want = th * BATCH
        bad = {k: v for k, v in reasons.items() if k != "ok"}
        rate = got / dt if dt else 0
        print(f"{th:>8} {want:>10} {got:>9} {dt:>6.2f}с {rate:>9.0f}  {bad if bad else '-'}")
        base += th * BATCH + 5000

    print("\n=== то же, но по разным узлам (по 1 потоку на узел) ===")
    print(f"{'узлов':>8} {'запрошено':>10} {'получено':>9} {'время':>7} {'блоков/с':>9}  причины")
    for k in (1, 2, 4):
        use = NODES[:k]
        t0 = time.time()
        with cf.ThreadPoolExecutor(max_workers=k) as ex:
            futs = [ex.submit(fetch, use[i], base + i * BATCH, BATCH) for i in range(k)]
            res = [f.result() for f in futs]
        dt = time.time() - t0
        got = sum(r[0] for r in res)
        reasons = collections.Counter(r[2] for r in res)
        bad = {kk: v for kk, v in reasons.items() if kk != "ok"}
        print(f"{k:>8} {k*BATCH:>10} {got:>9} {dt:>6.2f}с {got/dt if dt else 0:>9.0f}  {bad if bad else '-'}")
        base += k * BATCH + 5000
