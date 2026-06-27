#!/usr/bin/env python3
"""
polymarket.py  —  the honest half of the collector.

TWO paths, because retroactive full Polymarket history for 5-min markets DOES NOT EXIST:

1. RETROACTIVE (best-effort, expect near-empty): enumerate the past week's resolved
   btc-updown-5m markets via the Gamma API, and attempt CLOB /prices-history per token.
   Polymarket's prices-history returns EMPTY below 12h fidelity for resolved markets
   (documented limitation), so for 5-min markets this yields ~nothing. Included so you
   can SEE the reality, bounded to a sample so it doesn't hammer 2000+ markets.

2. FORWARD RECORDER (the real PM data path): subscribe to the live CLOB market
   WebSocket in RECORD-ONLY mode and append every book / price_change / trade event to a
   rotating JSONL on disk. Run it for a week and you HAVE the week — full fidelity. This is
   the only way to obtain real Polymarket book/tape for these ephemeral markets.

Endpoints (verified):
  Gamma:  https://gamma-api.polymarket.com/markets  (filter closed/slug/date; no auth)
  CLOB:   https://clob.polymarket.com/prices-history?market={TOKEN_ID}&interval=max&fidelity={min}
  WS:     wss://ws-subscriptions-clob.polymarket.com/ws/market
"""
import json, os, time, asyncio, urllib.request, urllib.parse, urllib.error, datetime as dt

GAMMA = "https://gamma-api.polymarket.com"
CLOB = "https://clob.polymarket.com"
PM_WS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
PM_RECV_TIMEOUT_S = 10.0
# [recorder fix] A passive recorder is NOT trading, so it does NOT need the bot's fast 10s stall-detection.
# The bot's poly_stream survives a 10s timeout only because it subscribes to markets it actively trades
# (constant book updates reset the timer). A recorder sitting on quieter markets falls through to the
# keepalive — and with ping==timeout==10s the silence-timeout RACES the PONG and reconnects every ~20s.
# Give the recorder a long silence-timeout: only reconnect on a GENUINELY dead socket; a quiet-but-live
# market just sits idle and stays connected so we capture the continuous stream when it does flow.
PM_RECORD_RECV_TIMEOUT_S = float(os.environ.get("PM_RECORD_RECV_TIMEOUT", "120"))
PM_RECORD_PING_S = float(os.environ.get("PM_RECORD_PING", "5"))   # keepalive well inside the timeout window
WS_BACKOFF_CAP_S = 20.0


def _get(url, timeout=20, retries=4):
    for a in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "amsa-data-collector/1.0"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            if e.code in (429, 418):
                time.sleep(2 ** a + 1); continue
            if e.code == 404:
                return None
            raise
        except Exception:
            if a == retries - 1:
                return None
            time.sleep(2 ** a)
    return None


# ----------------------------------------------------------------- RETROACTIVE (best-effort)
def list_recent_btc_5m(limit=200, lookback_days=7, slug_contains="btc-updown-5m"):
    """Enumerate recent CLOSED btc-updown-5m markets via Gamma. Returns [{slug, condition_id, tokens:[..]}]."""
    out, offset = [], 0
    cutoff = time.time() - lookback_days * 86400
    while len(out) < limit:
        params = urllib.parse.urlencode({
            "closed": "true", "limit": 100, "offset": offset, "order": "endDate", "ascending": "false",
        })
        batch = _get(f"{GAMMA}/markets?{params}")
        if not batch:
            break
        hit = 0
        for m in batch:
            slug = m.get("slug", "") or ""
            if slug_contains not in slug:
                continue
            # token ids live in clobTokenIds (JSON-encoded string) on Gamma market objects
            toks = m.get("clobTokenIds")
            if isinstance(toks, str):
                try: toks = json.loads(toks)
                except Exception: toks = []
            out.append({"slug": slug, "condition_id": m.get("conditionId"),
                        "tokens": toks or [], "endDate": m.get("endDate")})
            hit += 1
        offset += 100
        if hit == 0 and offset > 2000:   # walked far enough without matches
            break
        time.sleep(0.15)
    return out[:limit]


def prices_history(token_id, fidelity_min=1, interval="max"):
    """Attempt CLOB prices-history for one token. Returns list of {t,p} (likely EMPTY for 5-min markets)."""
    u = f"{CLOB}/prices-history?market={token_id}&interval={interval}&fidelity={fidelity_min}"
    j = _get(u)
    return (j or {}).get("history", []) if isinstance(j, dict) else []


def run_retroactive(save_dir, sample=40, lookback_days=7, log=print):
    """Best-effort retroactive PM pull. Writes whatever (little) comes back + an honest report."""
    os.makedirs(save_dir, exist_ok=True)
    log(f"[pm-retro] enumerating recent btc-updown-5m markets (sample={sample})...")
    mkts = list_recent_btc_5m(limit=sample, lookback_days=lookback_days)
    report = {"markets_found": len(mkts), "with_usable_history": 0, "total_points": 0,
              "note": "prices-history returns empty below 12h fidelity for resolved markets; "
                      "5-min markets yield ~nothing retroactively. Use the forward recorder for real PM data.",
              "samples": []}
    rows = []
    for mk in mkts:
        for tid in (mk.get("tokens") or []):
            hist = prices_history(tid, fidelity_min=1)
            n = len(hist)
            report["total_points"] += n
            if n > 0:
                report["with_usable_history"] += 1
            report["samples"].append({"slug": mk["slug"], "token": str(tid)[:12] + "…", "points": n})
            for h in hist:
                rows.append({"slug": mk["slug"], "token": tid, "t": h.get("t"), "p": h.get("p")})
            time.sleep(0.1)
    if rows:
        import csv
        with open(os.path.join(save_dir, "pm_retroactive_history.csv"), "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["slug", "token", "t", "p"]); w.writeheader(); w.writerows(rows)
    with open(os.path.join(save_dir, "pm_retroactive_report.json"), "w") as f:
        json.dump(report, f, indent=2)
    log(f"[pm-retro] {len(mkts)} markets, {report['with_usable_history']} with any history, "
        f"{report['total_points']} total points (expected ~0 for 5-min markets).")
    return report


# ----------------------------------------------------------------- FORWARD RECORDER (the real data)
async def _discover_active_tokens(slug_prefix="btc-updown-5m"):
    """Pull currently-active btc-updown-5m token ids from Gamma (active=true, closed=false)."""
    params = urllib.parse.urlencode({"active": "true", "closed": "false", "limit": 100, "order": "startDate", "ascending": "false"})
    batch = await asyncio.get_event_loop().run_in_executor(None, lambda: _get(f"{GAMMA}/markets?{params}"))
    toks = []
    for m in (batch or []):
        if slug_prefix in (m.get("slug", "") or ""):
            t = m.get("clobTokenIds")
            if isinstance(t, str):
                try: t = json.loads(t)
                except Exception: t = []
            toks.extend(t or [])
    return list(dict.fromkeys(toks))   # dedupe, preserve order


def _ws_backoff(attempt):
    import random
    return random.uniform(0.0, min(WS_BACKOFF_CAP_S, float(2 ** min(attempt, 16))))


async def run_recorder(save_dir, slug_prefix="btc-updown-5m", refresh_s=60, log=print, stop_event=None):
    """RECORD-ONLY: log every PM market WS event to a rotating hourly JSONL. Never trades.
    Self-heals like the bot's poly_stream (half-open recv timeout + full-jitter backoff). Re-subscribes
    as new 5-min markets spawn (refreshed every `refresh_s`). Market discovery + WS only — no state machine."""
    import websockets
    os.makedirs(save_dir, exist_ok=True)
    sub = set(await _discover_active_tokens(slug_prefix))
    last_refresh = time.time()
    attempt = 0
    _events = [0]   # mutable counter visible to the inner loop (events captured this session)

    def _writer_path():
        return os.path.join(save_dir, f"pm_record_{dt.datetime.now(dt.timezone.utc).strftime('%Y-%m-%d_%H')}Z.jsonl")

    while not (stop_event and stop_event.is_set()):
        try:
            # periodic re-discovery so freshly-created 5-min markets get recorded
            if time.time() - last_refresh > refresh_s:
                fresh = set(await _discover_active_tokens(slug_prefix))
                if fresh and fresh != sub:
                    sub = fresh
                    log(f"[pm-record] refreshed subscription -> {len(sub)} tokens")
                last_refresh = time.time()
            if not sub:
                await asyncio.sleep(3); continue
            async with websockets.connect(PM_WS, ping_interval=None, close_timeout=5) as ws:
                await ws.send(json.dumps({"type": "market", "assets_ids": list(sub), "custom_feature_enabled": True}))
                log(f"[pm-record] subscribed {len(sub)} tokens (recv-timeout {PM_RECORD_RECV_TIMEOUT_S:.0f}s)")
                got = False

                async def _ping():
                    while True:
                        await asyncio.sleep(PM_RECORD_PING_S)
                        try: await ws.send("PING")
                        except: return
                pt = asyncio.ensure_future(_ping())
                try:
                    while not (stop_event and stop_event.is_set()):
                        raw = await asyncio.wait_for(ws.recv(), timeout=PM_RECORD_RECV_TIMEOUT_S)
                        if not got:
                            got = True; attempt = 0
                        if raw == "PONG":
                            continue
                        rec = {"recv_ts": time.time(), "raw": raw}
                        with open(_writer_path(), "a") as f:
                            f.write(json.dumps(rec) + "\n")
                        _events[0] += 1
                        if _events[0] % 250 == 0:   # positive confirmation that data IS being captured
                            log(f"[pm-record] captured {_events[0]} events -> {os.path.basename(_writer_path())}")
                        # opportunistic re-discovery check without blocking the recv loop hard
                        if time.time() - last_refresh > refresh_s:
                            fresh = set(await _discover_active_tokens(slug_prefix))
                            if fresh and fresh != sub:
                                sub = fresh; last_refresh = time.time()
                                break   # reconnect to apply the new subscription set
                            last_refresh = time.time()
                finally:
                    pt.cancel()
        except asyncio.TimeoutError:
            attempt += 1; d = _ws_backoff(attempt)
            log(f"[pm-record] no frames >{PM_RECORD_RECV_TIMEOUT_S:.0f}s (likely dead socket) -> reconnect in {d:.1f}s")
            await asyncio.sleep(d)
        except Exception as e:
            attempt += 1; d = _ws_backoff(attempt)
            log(f"[pm-record] ws error: {e} -> reconnect in {d:.1f}s")
            await asyncio.sleep(d)


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="./data")
    ap.add_argument("--mode", choices=["retro", "record"], default="retro")
    ap.add_argument("--sample", type=int, default=40)
    a = ap.parse_args()
    if a.mode == "retro":
        print(json.dumps(run_retroactive(a.out, sample=a.sample), indent=2))
    else:
        asyncio.run(run_recorder(a.out))
