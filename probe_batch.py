#!/usr/bin/env python3
"""Шаг 1: замер на ПАЧЕЧНЫХ запросах — так работает настоящий кошелёк.

Проверяем три вещи, от которых зависит вся конструкция:
  A. как растёт скорость с размером пачки (один узел, один поток)
  B. что даёт несколько потоков к ОДНОМУ узлу
  C. что даёт раздача по РАЗНЫМ узлам

Если B ≈ C, то раздавать по разным узлам незачем — достаточно нескольких
соединений к одному, и задача решается вдвое проще. Проверяем, а не гадаем.
"""
import json
import time
import urllib.request
import concurrent.futures as cf

TIMEOUT = 20
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


def height(node):
    return rpc(node, "get_info")["result"]["height"]


def batch(node, start, count):
    """Пачка заголовков — ближайший JSON-аналог того, как кошелёк тянет блоки."""
    t0 = time.time()
    try:
        r = rpc(node, "get_block_headers_range",
                {"start_height": start, "end_height": start + count - 1})
        hdrs = r.get("result", {}).get("headers", [])
        return len(hdrs), time.time() - t0
    except Exception:
        return 0, time.time() - t0


if __name__ == "__main__":
    h = height(NODES[0])
    base = h - 20000
    print(f"высота цепи {h}, замеряем от {base}\n")

    print("=== A. Размер пачки (один узел, один поток) ===")
    print(f"{'пачка':>7} {'блоков':>8} {'время':>8} {'блоков/с':>10}")
    best_batch = 100
    best_rate = 0
    for n in (25, 100, 400, 1000):
        got, dt = batch(NODES[0], base, n)
        rate = got / dt if dt > 0 else 0
        if rate > best_rate:
            best_rate, best_batch = rate, n
        print(f"{n:>7} {got:>8} {dt:>7.2f}с {rate:>10.0f}")

    print(f"\nлучший размер пачки: {best_batch} ({best_rate:.0f} блоков/с)\n")

    N_CHUNKS = 8
    total = best_batch * N_CHUNKS

    print(f"=== B. {N_CHUNKS} потоков к ОДНОМУ узлу ({total} блоков) ===")
    t0 = time.time()
    with cf.ThreadPoolExecutor(max_workers=N_CHUNKS) as ex:
        futs = [ex.submit(batch, NODES[0], base + i * best_batch, best_batch)
                for i in range(N_CHUNKS)]
        res = [f.result() for f in futs]
    dt_one = time.time() - t0
    got_one = sum(r[0] for r in res)
    print(f"  {got_one} блоков за {dt_one:.2f}с -> {got_one/dt_one:.0f} блоков/с")

    print(f"\n=== C. {N_CHUNKS} кусков по {len(NODES)} РАЗНЫМ узлам ({total} блоков) ===")
    t0 = time.time()
    with cf.ThreadPoolExecutor(max_workers=N_CHUNKS) as ex:
        futs = [ex.submit(batch, NODES[i % len(NODES)], base + i * best_batch, best_batch)
                for i in range(N_CHUNKS)]
        res = [f.result() for f in futs]
    dt_many = time.time() - t0
    got_many = sum(r[0] for r in res)
    print(f"  {got_many} блоков за {dt_many:.2f}с -> {got_many/dt_many:.0f} блоков/с")

    print("\n=== ВЫВОД ===")
    seq_rate = best_rate
    r_one = got_one / dt_one if dt_one else 0
    r_many = got_many / dt_many if dt_many else 0
    print(f"  один поток, один узел : {seq_rate:>7.0f} блоков/с")
    print(f"  {N_CHUNKS} потоков, один узел  : {r_one:>7.0f} блоков/с  (x{r_one/seq_rate:.2f})")
    print(f"  {N_CHUNKS} потоков, {len(NODES)} узла  : {r_many:>7.0f} блоков/с  (x{r_many/seq_rate:.2f})")
    if r_one > 0:
        gain = r_many / r_one
        print(f"\n  выигрыш РАЗНЫХ узлов над одним: x{gain:.2f}")
        if gain < 1.25:
            print("  -> несколько узлов почти не добавляют: хватит потоков к одному.")
            print("     Это упрощает задачу вдвое, но надо проверить на слабой сети.")
        else:
            print("  -> раздача по разным узлам даёт заметный выигрыш, она нужна.")
