#!/usr/bin/env python3
"""Замер: последовательная загрузка с одного узла против менеджера.

Это и есть «performance benchmarks demonstrating the reduction in sync times»
из условия задачи — отдельный пункт сдачи.
"""
import json
import time
import urllib.request
import sys
from sync_manager import MultiNodeSync, DEFAULT_BATCH

NODES = [
    "http://node.monerodevs.org:18089",
    "http://opennode.xmr-tw.org:18089",
    "http://xmr-node.cakewallet.com:18081",
    "http://nodes.hashvault.pro:18081",
]
TIMEOUT = 25


def rpc(node, method, params=None):
    body = json.dumps({"jsonrpc": "2.0", "id": "0", "method": method,
                       "params": params or {}}).encode()
    req = urllib.request.Request(node + "/json_rpc", data=body,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
        return json.load(r)


def sequential(node, start, end, batch=DEFAULT_BATCH):
    """Как сейчас: один узел, один поток, пачка за пачкой."""
    got = 0
    t0 = time.time()
    h = start
    while h < end:
        n = min(batch, end - h)
        try:
            r = rpc(node, "get_block_headers_range",
                    {"start_height": h, "end_height": h + n - 1})
            got += len(r.get("result", {}).get("headers", []))
        except Exception:
            pass          # как в жизни: пропуск = дыра, кошелёк встанет
        h += n
    return got, time.time() - t0


if __name__ == "__main__":
    blocks = int(sys.argv[1]) if len(sys.argv) > 1 else 8000
    height = rpc(NODES[0], "get_info")["result"]["height"]
    start = height - 120000
    end = start + blocks
    print(f"диапазон: {blocks} блоков, с высоты {start}\n")

    print("=== КАК СЕЙЧАС: один узел, последовательно ===")
    got_seq, dt_seq = sequential(NODES[0], start, end)
    print(f"  {got_seq} блоков за {dt_seq:.1f}с -> {got_seq/dt_seq:.0f} блоков/с\n")

    print("=== МЕНЕДЖЕР: 4 узла, по 2 потока ===")
    mgr = MultiNodeSync(NODES, batch=DEFAULT_BATCH, threads_per_node=2)
    delivered_order = []
    got_par, dt_par = mgr.sync(start, end,
                               on_blocks=lambda hs: delivered_order.append(len(hs)))
    print(f"  {got_par} блоков за {dt_par:.1f}с -> {got_par/dt_par:.0f} блоков/с")
    print(f"  кусков отдано по порядку: {len(delivered_order)}\n")
    mgr.report()

    if dt_par > 0 and got_seq:
        print(f"\n=== ИТОГ: ускорение x{(got_par/dt_par)/(got_seq/dt_seq):.2f} ===")
        print(f"  было {dt_seq:.1f}с -> стало {dt_par:.1f}с на {blocks} блоков")
        full_chain = 3_750_000
        print(f"  в пересчёте на всю цепь ({full_chain:,} блоков): "
              f"{full_chain/(got_seq/dt_seq)/3600:.1f} ч -> "
              f"{full_chain/(got_par/dt_par)/3600:.1f} ч")
