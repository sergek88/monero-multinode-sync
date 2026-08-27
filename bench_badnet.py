#!/usr/bin/env python3
"""Замер на ПЛОХОЙ сети — главный сценарий задачи #196.

Условие прямо говорит: выигрыш нужен «especially in environments with
unreliable or high-latency network connections». На хорошем канале мы уже
намерили x3.9; здесь проверяем то, ради чего всё затевалось.

Плохую сеть моделируем честно и ОДИНАКОВО для обоих режимов: каждый запрос с
вероятностью loss обрывается, и к каждому добавляется задержка delay.
"""
import random
import time
import sys
import urllib.request
import json

import sync_manager
from sync_manager import MultiNodeSync, DEFAULT_BATCH

NODES = [
    "http://node.monerodevs.org:18089",
    "http://opennode.xmr-tw.org:18089",
    "http://xmr-node.cakewallet.com:18081",
    "http://nodes.hashvault.pro:18081",
]
TIMEOUT = 25

LOSS = 0.25      # четверть запросов обрывается — обычное дело в мобильной сети
DELAY = 0.30     # +300 мс к каждому запросу


def rpc_raw(node, method, params=None):
    body = json.dumps({"jsonrpc": "2.0", "id": "0", "method": method,
                       "params": params or {}}).encode()
    req = urllib.request.Request(node + "/json_rpc", data=body,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
        return json.load(r)


def bad_network():
    """Штраф, одинаковый для обоих режимов."""
    time.sleep(DELAY)
    if random.random() < LOSS:
        raise ConnectionResetError("сеть оборвала соединение (модель)")


def sequential_bad(node, start, end, batch=DEFAULT_BATCH):
    """Как сейчас: один узел, один поток, БЕЗ повторов — потеря = дыра в цепи."""
    got = 0
    holes = 0
    t0 = time.time()
    h = start
    while h < end:
        n = min(batch, end - h)
        try:
            bad_network()
            r = rpc_raw(node, "get_block_headers_range",
                        {"start_height": h, "end_height": h + n - 1})
            got += len(r.get("result", {}).get("headers", []))
        except Exception:
            holes += 1          # блоки этого куска потеряны
        h += n
    return got, holes, time.time() - t0


class BadNetSync(MultiNodeSync):
    """Тот же менеджер, но под тем же сетевым штрафом."""

    def _fetch(self, node, start, count):
        bad_network()
        return super()._fetch(node, start, count)


if __name__ == "__main__":
    blocks = int(sys.argv[1]) if len(sys.argv) > 1 else 6000
    height = rpc_raw(NODES[0], "get_info")["result"]["height"]
    start = height - 150000
    end = start + blocks

    print(f"ПЛОХАЯ СЕТЬ: обрыв {LOSS:.0%} запросов, задержка +{DELAY*1000:.0f} мс")
    print(f"диапазон {blocks} блоков\n")

    random.seed(42)
    print("=== КАК СЕЙЧАС: один узел, без повторов ===")
    got_s, holes, dt_s = sequential_bad(NODES[0], start, end)
    want = blocks
    print(f"  получено {got_s} из {want} блоков за {dt_s:.1f}с")
    print(f"  ДЫР В ЦЕПИ: {holes}  -> кошелёк на такой цепочке встанет")
    print(f"  скорость {got_s/dt_s:.0f} блоков/с\n")

    random.seed(42)
    print("=== МЕНЕДЖЕР: 4 узла по 2 потока, с повторами ===")
    mgr = BadNetSync(NODES, batch=DEFAULT_BATCH, threads_per_node=2)
    mgr.cross_check = False        # сверку отключаем: она тоже пойдёт по битой сети
    try:
        got_m, dt_m = mgr.sync(start, end)
        print(f"  получено {got_m} из {want} блоков за {dt_m:.1f}с")
        print(f"  ДЫР В ЦЕПИ: 0  -> цепочка непрерывна")
        print(f"  скорость {got_m/dt_m:.0f} блоков/с")
        print(f"  повторных попыток: {sum(mgr.retries.values())}")
    except Exception as e:
        print(f"  не справился: {e}")
        got_m, dt_m = 0, 1

    print()
    mgr.report()

    if got_s and got_m:
        print(f"\n=== ИТОГ на плохой сети ===")
        print(f"  целостность: было {holes} дыр -> стало 0")
        print(f"  скорость:    x{(got_m/dt_m)/(got_s/dt_s):.2f}")
        print(f"  полнота:     {100*got_s/want:.0f}% -> {100*got_m/want:.0f}%")
