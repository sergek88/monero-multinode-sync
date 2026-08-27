#!/usr/bin/env python3
"""Reference multi-threaded block sync (Monero bounty #196).

Every parameter here comes from measurement (probe_batch.py, probe_limits.py),
not from guesswork:

  * batch of 1000 blocks - an order of magnitude faster than small ones
    (512 vs 49 blocks/s);
  * at most 2-3 requests per node - at 8 concurrent requests throughput stops
    improving (1012 -> 999 blocks/s) while connection resets go 1 -> 5;
  * a reset under load is normal node behaviour, so retries are mandatory;
  * nodes differ in speed: splitting the range evenly up front makes the
    slowest node set the pace, so chunks are handed out as workers free up.

Blocks are delivered to the consumer STRICTLY IN ORDER, as the bounty asks:
the wallet must receive a continuous chain as soon as each chunk is ready,
without waiting for the whole download to finish.
"""
import json
import threading
import time
import urllib.request
import queue
import collections
import os
import hashlib
from verify import verify_internal, verify_junction, verify_cross, ChainError

DEFAULT_BATCH = 1000
DEFAULT_THREADS_PER_NODE = 2      # measured safe limit
MAX_RETRIES = 4
TIMEOUT = 25


class NodeStat:
    """Node health: throughput and failures. Drives which node gets the next chunk."""

    def __init__(self, url):
        self.url = url
        self.blocks = 0
        self.seconds = 0.0
        self.failures = 0
        self.consecutive_failures = 0
        self.inflight = 0
        self.lock = threading.Lock()

    @property
    def rate(self):
        """Blocks per second. An unused node gets the benefit of the doubt."""
        with self.lock:
            if self.seconds <= 0:
                return 500.0
            return self.blocks / self.seconds

    @property
    def healthy(self):
        return self.consecutive_failures < 3

    def note_ok(self, blocks, seconds):
        with self.lock:
            self.blocks += blocks
            self.seconds += seconds
            self.consecutive_failures = 0

    def note_fail(self):
        with self.lock:
            self.failures += 1
            self.consecutive_failures += 1


class MultiNodeSync:
    def __init__(self, nodes, batch=DEFAULT_BATCH,
                 threads_per_node=DEFAULT_THREADS_PER_NODE, cache_dir=None):
        self.stats = {u: NodeStat(u) for u in nodes}
        self.batch = batch
        self.threads_per_node = threads_per_node
        self.chunks = queue.Queue()
        self.done = {}
        self.done_lock = threading.Lock()
        self.stop = threading.Event()
        self.retries = collections.Counter()
        self.delivered = 0
        self.cross_check = True
        self.bad_data = {}
        self._last_chunk = None
        # Local chunk cache (a bounty requirement): re-scans and syncing a second
        # wallet on the same device do not hit the network at all.
        self.cache_dir = cache_dir
        self.cache_hits = 0
        self.cache_writes = 0
        if cache_dir:
            os.makedirs(cache_dir, exist_ok=True)

    # --- local cache ------------------------------------------------------
    def _cache_path(self, start, count):
        key = hashlib.sha256(f"{start}:{count}".encode()).hexdigest()[:20]
        return os.path.join(self.cache_dir, f"{start}_{count}_{key}.json")

    def _cache_get(self, start, count):
        if not self.cache_dir:
            return None
        p = self._cache_path(start, count)
        if not os.path.exists(p):
            return None
        try:
            with open(p, encoding="utf-8") as f:
                headers = json.load(f)
            # The cache is untrusted too: a corrupted file must not slip into the
            # chain silently.
            verify_internal(headers)
            return headers
        except Exception:
            try:
                os.remove(p)
            except OSError:
                pass
            return None

    def _cache_put(self, start, count, headers):
        if not self.cache_dir or not headers:
            return
        tmp = self._cache_path(start, count) + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(headers, f)
            os.replace(tmp, self._cache_path(start, count))
            self.cache_writes += 1
        except Exception:
            pass

    # --- network ----------------------------------------------------------
    def _rpc(self, node, method, params):
        body = json.dumps({"jsonrpc": "2.0", "id": "0",
                           "method": method, "params": params}).encode()
        req = urllib.request.Request(node + "/json_rpc", data=body,
                                     headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
            return json.load(r)

    def _fetch(self, node, start, count):
        r = self._rpc(node, "get_block_headers_range",
                      {"start_height": start, "end_height": start + count - 1})
        if "error" in r:
            raise RuntimeError(r["error"].get("message", "rpc error"))
        return r.get("result", {}).get("headers", [])

    # --- node selection ---------------------------------------------------
    def _pick_node(self):
        """Fastest healthy node that still has a free slot.

        The slots are the measured limit: sending more than threads_per_node
        concurrent requests to one node only adds resets, not throughput.
        """
        best = None
        for st in self.stats.values():
            if not st.healthy:
                continue
            with st.lock:
                if st.inflight >= self.threads_per_node:
                    continue
            if best is None or st.rate > best.rate:
                best = st
        return best

    # --- worker -----------------------------------------------------------
    def _worker(self):
        while not self.stop.is_set():
            try:
                idx, start, count, attempt = self.chunks.get(timeout=0.5)
            except queue.Empty:
                if self._all_done():
                    return
                continue

            cached = self._cache_get(start, count)
            if cached is not None:
                self.cache_hits += 1
                with self.done_lock:
                    self.done[idx] = cached
                self.chunks.task_done()
                continue

            st = None
            while st is None and not self.stop.is_set():
                st = self._pick_node()
                if st is None:
                    time.sleep(0.05)
            if st is None:
                return

            with st.lock:
                st.inflight += 1
            t0 = time.time()
            try:
                headers = self._fetch(st.url, start, count)
                dt = time.time() - t0
                # Check 1: the chunk must be internally linked. A node returning a
                # broken chain is penalised like a dropped connection.
                verify_internal(headers)
                if self.cross_check:
                    verify_cross(headers, list(self.stats.keys()), st.url, samples=1)
                st.note_ok(len(headers), dt)
                self._cache_put(start, count, headers)
                with self.done_lock:
                    self.done[idx] = headers
            except ChainError as e:
                # Bad data, not a network glitch: penalise twice so the node leaves
                # rotation sooner. Slot release and task_done happen in finally -
                # do not repeat them here, finally also runs before continue.
                st.note_fail()
                st.note_fail()
                self.bad_data[st.url] = str(e)
                self.retries[idx] += 1
                if self.retries[idx] <= MAX_RETRIES:
                    self.chunks.put((idx, start, count, attempt + 1))
                else:
                    with self.done_lock:
                        self.done[idx] = None
            except Exception:
                st.note_fail()
                self.retries[idx] += 1
                if self.retries[idx] <= MAX_RETRIES:
                    # the chunk goes back to the queue and lands on another node
                    self.chunks.put((idx, start, count, attempt + 1))
                else:
                    with self.done_lock:
                        self.done[idx] = None   # give up on this chunk, but never break the chain silently
            finally:
                with st.lock:
                    st.inflight -= 1
                self.chunks.task_done()

    def _all_done(self):
        with self.done_lock:
            return len(self.done) >= self.total_chunks

    # --- main pass --------------------------------------------------------
    def sync(self, start_height, end_height, on_blocks=None):
        """Fetch [start_height, end_height) and deliver blocks IN ORDER."""
        ranges = []
        h = start_height
        while h < end_height:
            n = min(self.batch, end_height - h)
            ranges.append((h, n))
            h += n
        self.total_chunks = len(ranges)
        for i, (s, n) in enumerate(ranges):
            self.chunks.put((i, s, n, 0))

        workers = max(1, len(self.stats) * self.threads_per_node)
        threads = [threading.Thread(target=self._worker, daemon=True)
                   for _ in range(workers)]
        t0 = time.time()
        for t in threads:
            t.start()

        # Strict in-order delivery: wait for chunk next_idx, hand it over, move on.
        next_idx = 0
        while next_idx < self.total_chunks:
            with self.done_lock:
                ready = next_idx in self.done
                chunk = self.done.get(next_idx)
            if not ready:
                if all(not t.is_alive() for t in threads):
                    break
                time.sleep(0.02)
                continue
            if chunk is None:
                raise RuntimeError(f"chunk {next_idx} could not be fetched in "
                                   f"{MAX_RETRIES} attempts")
            # Check 2: the junction with the previous chunk - fetched by a DIFFERENT node.
            verify_junction(self._last_chunk, chunk)
            self._last_chunk = chunk
            self.delivered += len(chunk)
            if on_blocks:
                on_blocks(chunk)
            with self.done_lock:
                del self.done[next_idx]
            next_idx += 1

        self.stop.set()
        for t in threads:
            t.join(timeout=1)
        return self.delivered, time.time() - t0

    def report(self):
        print(f"{'узел':<44} {'блоков':>8} {'бл/с':>7} {'отказов':>8}")
        for st in sorted(self.stats.values(), key=lambda s: -s.blocks):
            print(f"{st.url:<44} {st.blocks:>8} {st.rate:>7.0f} {st.failures:>8}")
        total_retries = sum(self.retries.values())
        if total_retries:
            print(f"retries: {total_retries}")
        if self.cache_dir:
            print(f"cache: {self.cache_hits} chunks served, "
                  f"{self.cache_writes} written")
        if self.bad_data:
            print("nodes that returned bad data:")
            for u, why in self.bad_data.items():
                print(f"  {u}: {why}")
