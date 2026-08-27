#!/usr/bin/env python3
"""Шаг 0 задачи Monero #196: есть ли вообще выигрыш от нескольких узлов.

Меряем на публичных узлах:
  - живой ли узел, какая высота цепи
  - задержка ответа
  - реальная скорость отдачи блоков (последовательно с одного узла)

Без этого писать многопоточную загрузку бессмысленно: если узкое место не в
сети, параллельность ничего не даст. Тот же порядок, что и в эмуляторе -
сначала измерить, потом чинить.
"""
import json
import time
import urllib.request
import concurrent.futures as cf

NODES = [
    "http://node.moneroworld.com:18089",
    "http://node.supportxmr.com:18081",
    "http://opennode.xmr-tw.org:18089",
    "http://node.community.rino.io:18081",
    "http://xmr-node.cakewallet.com:18081",
    "http://node.monerodevs.org:18089",
    "http://nodes.hashvault.pro:18081",
    "http://singapore.node.xmr.pm:18089",
]

TIMEOUT = 12


def rpc(node, method, params=None):
    body = json.dumps({
        "jsonrpc": "2.0", "id": "0", "method": method,
        "params": params or {},
    }).encode()
    req = urllib.request.Request(
        node + "/json_rpc", data=body,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
        return json.load(r)


def check(node):
    t0 = time.time()
    try:
        info = rpc(node, "get_info")
        dt = time.time() - t0
        res = info.get("result", {})
        return {
            "node": node,
            "ok": True,
            "latency_ms": round(dt * 1000),
            "height": res.get("height"),
            "busy": res.get("busy_syncing"),
            "restricted": res.get("restricted"),
        }
    except Exception as e:
        return {"node": node, "ok": False, "err": type(e).__name__}


def fetch_blocks(node, start, count):
    """Тянем count блоков подряд через get_block (JSON), меряем время и объём."""
    total_bytes = 0
    t0 = time.time()
    got = 0
    for h in range(start, start + count):
        try:
            r = rpc(node, "get_block", {"height": h})
            blob = r.get("result", {}).get("blob", "")
            total_bytes += len(blob) // 2
            got += 1
        except Exception:
            break
    dt = time.time() - t0
    return got, total_bytes, dt


if __name__ == "__main__":
    print("=== 1. Живые узлы и задержка ===")
    with cf.ThreadPoolExecutor(max_workers=len(NODES)) as ex:
        results = list(ex.map(check, NODES))
    alive = []
    for r in sorted(results, key=lambda x: (not x["ok"], x.get("latency_ms", 9999))):
        if r["ok"]:
            alive.append(r)
            print(f"  OK   {r['latency_ms']:>5} мс | высота {r['height']} | {r['node']}")
        else:
            print(f"  нет  {'':>5}    | {r['err']:<18} | {r['node']}")

    if len(alive) < 2:
        print("\nЖивых узлов меньше двух — замер параллельности невозможен.")
        raise SystemExit(1)

    height = min(a["height"] for a in alive)
    start = height - 5000
    N = 12
    print(f"\n=== 2. Скорость одного узла ({N} блоков с высоты {start}) ===")
    single = fetch_blocks(alive[0]["node"], start, N)
    got, size, dt = single
    print(f"  {alive[0]['node']}")
    print(f"  получено {got} блоков, {size/1024:.0f} КБ за {dt:.2f} с "
          f"-> {got/dt:.1f} блоков/с, {size/dt/1024:.0f} КБ/с")

    print(f"\n=== 3. Те же {N} блоков, но параллельно с {min(4,len(alive))} узлов ===")
    use = alive[: min(4, len(alive))]
    chunk = N // len(use)
    t0 = time.time()
    with cf.ThreadPoolExecutor(max_workers=len(use)) as ex:
        futs = [
            ex.submit(fetch_blocks, u["node"], start + i * chunk, chunk)
            for i, u in enumerate(use)
        ]
        parts = [f.result() for f in futs]
    dt_par = time.time() - t0
    got_par = sum(p[0] for p in parts)
    size_par = sum(p[1] for p in parts)
    print(f"  узлов: {len(use)}, по {chunk} блоков на узел")
    print(f"  получено {got_par} блоков, {size_par/1024:.0f} КБ за {dt_par:.2f} с "
          f"-> {got_par/dt_par:.1f} блоков/с, {size_par/dt_par/1024:.0f} КБ/с")

    if dt > 0 and dt_par > 0:
        speedup = (got_par / dt_par) / (got / dt) if got else 0
        print(f"\n=== ИТОГ: ускорение x{speedup:.2f} ===")
        if speedup < 1.3:
            print("  Выигрыш мал — узкое место НЕ в одном узле. Идею надо пересматривать.")
        else:
            print("  Выигрыш есть — параллельная загрузка оправдана.")
