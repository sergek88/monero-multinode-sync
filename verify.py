#!/usr/bin/env python3
"""Integrity checks for blocks fetched from untrusted nodes (bounty #196).

We download from several nodes nobody vouches for, so three checks:

  1. linkage inside a chunk: each block's prev_hash == previous block's hash;
  2. the junction between adjacent chunks - they were fetched by DIFFERENT
     nodes, which is the weak spot of a multi-node scheme; a single-node fetch
     has no such seam at all;
  3. a spot check against a second node: take a random block from the chunk
     and ask another node for its hash.

The third check catches a consistent forgery: a node can return an internally
flawless chain that simply is not the real Monero chain.
"""
import json
import random
import urllib.request

TIMEOUT = 20


class ChainError(Exception):
    pass


def rpc(node, method, params=None):
    body = json.dumps({"jsonrpc": "2.0", "id": "0", "method": method,
                       "params": params or {}}).encode()
    req = urllib.request.Request(node + "/json_rpc", data=body,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
        return json.load(r)


def verify_internal(headers):
    """Check 1: heights are consecutive and each block links to the previous one."""
    if not headers:
        raise ChainError("empty chunk")
    for i, h in enumerate(headers):
        if i > 0:
            prev = headers[i - 1]
            if h["height"] != prev["height"] + 1:
                raise ChainError(
                    f"height gap: {prev['height']} -> {h['height']}")
            if h["prev_hash"] != prev["hash"]:
                raise ChainError(
                    f"chain break at height {h['height']}: "
                    f"prev_hash={h['prev_hash'][:16]}… "
                    f"but previous block is {prev['hash'][:16]}…")
    return True


def verify_junction(prev_chunk, chunk):
    """Check 2: the seam between chunks fetched by DIFFERENT nodes."""
    if not prev_chunk or not chunk:
        return True
    last, first = prev_chunk[-1], chunk[0]
    if first["height"] != last["height"] + 1:
        raise ChainError(
            f"gap at chunk junction: {last['height']} -> {first['height']}")
    if first["prev_hash"] != last["hash"]:
        raise ChainError(
            f"junction mismatch at height {first['height']}: chunks from different "
            f"nodes disagree")
    return True


def verify_cross(headers, nodes, source_node, samples=1):
    """Check 3: spot-check a block hash against a second node."""
    others = [n for n in nodes if n != source_node]
    if not others or not headers:
        return True
    for _ in range(samples):
        h = random.choice(headers)
        other = random.choice(others)
        try:
            r = rpc(other, "get_block_header_by_height", {"height": h["height"]})
            their = r.get("result", {}).get("block_header", {}).get("hash")
        except Exception:
            continue          # an unreachable second node is not evidence of forgery
        if their and their != h["hash"]:
            raise ChainError(
                f"nodes disagree at height {h['height']}: "
                f"{source_node} says {h['hash'][:16]}…, "
                f"{other} says {their[:16]}…")
    return True


# --- self-test: does it actually catch forgeries? -------------------------
if __name__ == "__main__":
    NODES = ["http://node.monerodevs.org:18089",
             "http://opennode.xmr-tw.org:18089"]

    print("=== fetch real blocks ===")
    h = rpc(NODES[0], "get_info")["result"]["height"] - 1000
    real = rpc(NODES[0], "get_block_headers_range",
               {"start_height": h, "end_height": h + 19})["result"]["headers"]
    print(f"  got {len(real)} headers from height {h}")

    print("\n=== check 1: genuine chain ===")
    print("  ", "PASS" if verify_internal(real) else "no")

    print("\n=== check 1: tamper with a hash in the middle ===")
    forged = [dict(x) for x in real]
    forged[10]["hash"] = "de" * 32
    try:
        verify_internal(forged)
        print("   MISSED - the check does not work!")
    except ChainError as e:
        print(f"   CAUGHT: {e}")

    print("\n=== check 1: drop a block (hole in the chain) ===")
    holed = [dict(x) for x in real]
    del holed[5]
    try:
        verify_internal(holed)
        print("   MISSED - the check does not work!")
    except ChainError as e:
        print(f"   CAUGHT: {e}")

    print("\n=== check 2: junction between chunks from different nodes ===")
    a = real[:10]
    b = real[10:]
    print("   genuine junction:", "PASS" if verify_junction(a, b) else "no")
    bad_b = [dict(x) for x in b]
    bad_b[0]["prev_hash"] = "ab" * 32
    try:
        verify_junction(a, bad_b)
        print("   MISSED - the check does not work!")
    except ChainError as e:
        print(f"   CAUGHT: {e}")

    print("\n=== check 3: cross-check against a second node ===")
    try:
        verify_cross(real, NODES, NODES[0], samples=2)
        print("   nodes agree - PASS")
    except ChainError as e:
        print(f"   disagreement: {e}")

    print("\n=== check 3: feed a fabricated hash ===")
    fake = [dict(x) for x in real]
    for x in fake:
        x["hash"] = "cd" * 32
    try:
        verify_cross(fake, NODES, NODES[0], samples=2)
        print("   MISSED - the check does not work!")
    except ChainError as e:
        print(f"   CAUGHT: {e}")
