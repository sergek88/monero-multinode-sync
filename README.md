# Multi-node block sync for Monero wallets

A reference implementation for [Monero bounty #196](https://bounties.monero.social/posts/196/3-315m-implement-a-multi-threaded-node-synchronization):
fetch blocks from several remote nodes in parallel instead of one, so mobile
wallets sync faster and survive flaky networks.

Every parameter below was **measured before the code was written**, not guessed.
The measurement scripts are included.

## Results

Live public nodes, 6000–8000 blocks:

| | current single-node | this |
|---|---|---|
| good network | 421 blocks/s | **1649 blocks/s** (x3.9) |
| lossy network (25% drops, +300 ms) | 215 blocks/s, **2 gaps**, 67% delivered | **1168 blocks/s, 0 gaps, 100% delivered** |
| re-scan from local cache | refetch from network | **6.6x faster** |

The lossy-network row matters more than raw speed: the single-node approach
does not deliver a complete chain there at all, and a wallet cannot scan a
chain with holes in it.

## How it works

- the range is split into chunks (default 1000 blocks);
- chunks are handed out **as workers free up**, so a fast node takes more work
  and a slow one does not hold back the rest;
- each node has a small number of slots (default 2) — the measured limit;
- a dropped connection puts the chunk back in the queue for another node;
- a node failing three times in a row leaves the rotation;
- blocks are delivered **strictly in order**, so the wallet can start scanning
  while the rest is still downloading;
- finished chunks are cached on disk for re-scans.

## Integrity

Nodes are untrusted, so three checks (`verify.py`):

1. **linkage inside a chunk** — each block's `prev_hash` matches the previous hash;
2. **the junction between chunks** — they come from *different* nodes; a
   single-node fetch has no such seam, this is the weak spot of the whole idea;
3. **cross-check against a second node** — catches an internally consistent but
   fabricated chain.

All three are tested against forged input. `python3 verify.py` feeds it a
tampered hash, a missing block, a broken junction and a fabricated chain, and
prints what was caught. A deliberately misbehaving node is isolated and the
sync still completes.

## Why these parameters

| parameter | value | why |
|---|---|---|
| batch size | 1000 | 25 blocks → 49 blocks/s, 1000 → 512 blocks/s; latency dominates |
| requests per node | 2 | at 8 concurrent: throughput 1012 → 999 blocks/s, resets 1 → 5 |
| retries | 4 | a reset under load is normal, not exceptional |
| drop node after | 3 consecutive failures | keeps a dead node from stalling the queue |

Reproduce: `probe_batch.py` (batch size), `probe_limits.py` (per-node limit).

## Files

| file | what it does |
|---|---|
| `sync_manager.py` | the download manager |
| `verify.py` | integrity checks + self-test against forgeries |
| `bench.py` | benchmark: sequential vs manager |
| `bench_badnet.py` | benchmark on a lossy network |
| `probe_nodes.py` | is there any gain from multiple nodes at all |
| `probe_batch.py` | batch size sweep |
| `probe_limits.py` | where nodes start refusing requests |

## Running

```
python3 bench.py 8000          # good network
python3 bench_badnet.py 6000   # 25% drops, +300 ms
python3 verify.py              # integrity self-test
```

Python 3, standard library only.

## Status

Standalone reference implementation, as the bounty allows. It uses the JSON-RPC
`get_block_headers_range` endpoint; a wallet integration would use the binary
`getblocks.bin` path, which does not change the scheduling, retry or integrity
logic. Happy to port it into a specific wallet if that is preferred.
