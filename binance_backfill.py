#!/usr/bin/env python3
"""
binance_backfill.py  —  the REAL half of the collector.

Downloads a week of Binance BTCUSDT data from the public dumps at data.binance.vision
(klines + full aggTrades), parses them, and derives the deterministic 5-min UP/DOWN
outcomes + a per-window signal summary (realized vol, CVD, regime classification).

This is genuine, complete, multi-regime data — and the 5-min outcomes are reconstructable
from it, which is exactly the chop-vs-trend ground truth the strategy work has been missing.

Sources (verified):
  - daily klines:    https://data.binance.vision/data/spot/daily/klines/{SYM}/{intv}/{SYM}-{intv}-{date}.zip
  - daily aggTrades: https://data.binance.vision/data/spot/daily/aggTrades/{SYM}/{SYM}-aggTrades-{date}.zip
  - REST fallback (today's partial day, or a missing dump): https://data-api.binance.vision/api/v3/{klines,aggTrades}
Daily dumps only exist for COMPLETE past UTC days (yesterday appears a few min after 00:00 UTC).
For the current partial day we fall back to the REST mirror.
"""
import io, csv, json, os, time, zipfile, urllib.request, urllib.error, datetime as dt
from collections import defaultdict

VISION = "https://data.binance.vision/data/spot/daily"
REST = "https://data-api.binance.vision/api/v3"   # market-data-only mirror, no auth
KLINE_COLS = ["open_time","open","high","low","close","volume","close_time",
              "quote_volume","num_trades","taker_buy_base","taker_buy_quote","ignore"]
AGG_COLS = ["agg_id","price","qty","first_id","last_id","ts","is_buyer_maker","is_best_match"]


def _http_get(url, timeout=60):
    req = urllib.request.Request(url, headers={"User-Agent": "amsa-data-collector/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def _http_json(url, timeout=30, retries=4):
    for a in range(retries):
        try:
            return json.loads(_http_get(url, timeout))
        except urllib.error.HTTPError as e:
            if e.code in (429, 418):
                time.sleep(2 ** a + 1); continue
            raise
        except Exception:
            if a == retries - 1: raise
            time.sleep(2 ** a)
    return None


def _dump_csv_rows(zip_bytes):
    """Yield CSV rows from a binance.vision daily zip (single CSV inside). Skips a header row if present."""
    zf = zipfile.ZipFile(io.BytesIO(zip_bytes))
    name = zf.namelist()[0]
    first = True
    with zf.open(name) as fh:
        for line in io.TextIOWrapper(fh, encoding="utf-8"):
            line = line.strip()
            if not line:
                continue
            # 2025+ dumps sometimes carry a header line; detect by non-numeric first field
            if first:
                first = False
                head = line.split(",")[0]
                try:
                    float(head)
                except ValueError:
                    continue   # it's a header, skip
            yield line.split(",")


# ----------------------------------------------------------------- klines
def fetch_klines_day(symbol, interval, date_str, save_dir):
    """Return list of kline rows (as lists) for one UTC day. Tries the dump, falls back to REST."""
    url = f"{VISION}/klines/{symbol}/{interval}/{symbol}-{interval}-{date_str}.zip"
    out_csv = os.path.join(save_dir, f"klines_{interval}_{date_str}.csv")
    try:
        raw = _http_get(url)
        rows = list(_dump_csv_rows(raw))
        _write_rows(out_csv, KLINE_COLS, rows)
        return rows, "dump"
    except urllib.error.HTTPError as e:
        if e.code != 404:
            raise
    # 404 -> dump not published yet (today / very recent). REST-paginate the day.
    rows = _rest_klines(symbol, interval, date_str)
    if rows:
        _write_rows(out_csv, KLINE_COLS, rows)
    return rows, "rest"


def _rest_klines(symbol, interval, date_str):
    start = int(dt.datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=dt.timezone.utc).timestamp() * 1000)
    end = start + 86400_000
    out, cur = [], start
    while cur < end:
        u = f"{REST}/klines?symbol={symbol}&interval={interval}&startTime={cur}&endTime={end}&limit=1000"
        batch = _http_json(u)
        if not batch:
            break
        out.extend([[str(x) for x in row] for row in batch])
        nxt = int(batch[-1][6]) + 1   # close_time + 1ms
        if nxt <= cur:
            break
        cur = nxt
        time.sleep(0.2)
    return out


# ----------------------------------------------------------------- aggTrades
def fetch_aggtrades_day(symbol, date_str, save_dir):
    """Return aggTrades rows for one UTC day. Dump first, REST fallback. (Large: ~hundreds of MB/day raw.)"""
    url = f"{VISION}/aggTrades/{symbol}/{symbol}-aggTrades-{date_str}.zip"
    out_csv = os.path.join(save_dir, f"aggTrades_{date_str}.csv")
    try:
        raw = _http_get(url, timeout=180)
        rows = list(_dump_csv_rows(raw))
        _write_rows(out_csv, AGG_COLS, rows)
        return rows, "dump"
    except urllib.error.HTTPError as e:
        if e.code != 404:
            raise
    rows = _rest_aggtrades(symbol, date_str)
    if rows:
        _write_rows(out_csv, AGG_COLS, rows)
    return rows, "rest"


def _rest_aggtrades(symbol, date_str):
    start = int(dt.datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=dt.timezone.utc).timestamp() * 1000)
    end = start + 86400_000
    out, cur = [], start
    # REST aggTrades windows by time; 1000 per call. A full day can be a LOT of calls — bounded by a cap.
    for _ in range(100000):
        u = f"{REST}/aggTrades?symbol={symbol}&startTime={cur}&endTime={min(cur + 3600_000, end)}&limit=1000"
        batch = _http_json(u)
        if not batch:
            cur = min(cur + 3600_000, end)
            if cur >= end:
                break
            continue
        out.extend([[str(b.get(k, "")) for k in ("a","p","q","f","l","T","m","M")] for b in batch])
        last_T = int(batch[-1]["T"])
        nxt = last_T + 1
        if nxt <= cur and len(batch) < 1000:
            cur = min(cur + 3600_000, end)
        else:
            cur = nxt
        if cur >= end:
            break
        time.sleep(0.1)
    return out


def _write_rows(path, header, rows):
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


# ----------------------------------------------------------------- derived: 5-min outcomes + signal summary
def derive_5min_summary(klines_1m, aggtrades, save_path):
    """From 1m klines + aggTrades, build one row per aligned 5-min window:
       open, close, outcome(UP/DOWN), range_bps, realized_vol_bps, cvd_delta, n_trades, directionality, regime.
       outcome = Binance-kline approximation of how a btc-updown-5m market would resolve (close vs open of window).
       directionality = |close-open| / (high-low): ~1 = clean trend, ~0 = chop. regime tags off that + vol."""
    # index 1m klines by open_time(ms) -> (o,h,l,c,vol)
    kl = {}
    for r in klines_1m:
        try:
            kl[int(r[0])] = (float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5]))
        except (ValueError, IndexError):
            continue
    if not kl:
        return []
    # CVD per minute bucket from aggTrades: buyer_maker=true => aggressor SELLER (negative); false => BUYER (positive)
    cvd_min = defaultdict(float)
    trd_min = defaultdict(int)
    for r in aggtrades or []:
        try:
            px = float(r[1]); qty = float(r[2]); ts = int(r[5])
            maker = str(r[6]).lower() in ("true", "1")
        except (ValueError, IndexError):
            continue
        minute = ts - (ts % 60000)
        cvd_min[minute] += (-qty if maker else qty) * px   # signed notional
        trd_min[minute] += 1

    times = sorted(kl.keys())
    if not times:
        return []
    # align windows to 5-min grid
    start = times[0] - (times[0] % 300000)
    end = times[-1]
    rows = []
    t = start
    while t <= end:
        mins = [t + 60000 * i for i in range(5)]
        present = [m for m in mins if m in kl]
        if len(present) >= 3:   # need most of the window
            o = kl[present[0]][0]
            c = kl[present[-1]][3]
            hi = max(kl[m][1] for m in present)
            lo = min(kl[m][2] for m in present)
            rets = []
            for i in range(1, len(present)):
                p0 = kl[present[i-1]][3]; p1 = kl[present[i]][3]
                if p0 > 0:
                    rets.append((p1 - p0) / p0)
            rng = hi - lo
            rv = (sum(x*x for x in rets) / len(rets)) ** 0.5 if rets else 0.0
            cvd = sum(cvd_min.get(m, 0.0) for m in mins)
            ntr = sum(trd_min.get(m, 0) for m in mins)
            direction = abs(c - o) / rng if rng > 0 else 0.0
            move_bps = (c - o) / o * 1e4 if o > 0 else 0.0
            # regime heuristic: clean directional move = TREND; lots of range but little net move = CHOP
            if direction >= 0.55 and abs(move_bps) >= 4:
                regime = "TREND"
            elif direction <= 0.30:
                regime = "CHOP"
            else:
                regime = "MIXED"
            rows.append({
                "window_start_ms": t,
                "window_start_utc": dt.datetime.fromtimestamp(t/1000, dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                "open": round(o, 2), "close": round(c, 2),
                "outcome": "UP" if c > o else "DOWN",
                "move_bps": round(move_bps, 2),
                "range_bps": round(rng / o * 1e4, 2) if o > 0 else 0.0,
                "realized_vol_bps": round(rv * 1e4, 2),
                "cvd_delta_usd": round(cvd, 0),
                "n_trades": ntr,
                "directionality": round(direction, 3),
                "regime": regime,
            })
        t += 300000
    if rows:
        with open(save_path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader(); w.writerows(rows)
    return rows


# ----------------------------------------------------------------- orchestration
def run_backfill(symbol, lookback_days, save_dir, want_1m=True, want_1s=True, want_agg=True, log=print):
    """Download the last `lookback_days` COMPLETE UTC days (+ today's partial via REST). Returns a manifest dict."""
    os.makedirs(save_dir, exist_ok=True)
    today = dt.datetime.now(dt.timezone.utc).date()
    dates = [(today - dt.timedelta(days=d)).strftime("%Y-%m-%d") for d in range(lookback_days, -1, -1)]
    manifest = {"symbol": symbol, "days": dates, "klines_1m": 0, "klines_1s": 0, "aggtrades": 0,
                "summary_windows": 0, "started": time.time(), "errors": []}
    all_1m, all_agg = [], []
    for date_str in dates:
        if want_1m:
            try:
                rows, src = fetch_klines_day(symbol, "1m", date_str, save_dir)
                all_1m.extend(rows); manifest["klines_1m"] += len(rows)
                log(f"[backfill] 1m klines {date_str}: {len(rows)} rows ({src})")
            except Exception as e:
                manifest["errors"].append(f"1m {date_str}: {e}"); log(f"[backfill] 1m {date_str} FAIL: {e}")
        if want_1s:
            try:
                rows, src = fetch_klines_day(symbol, "1s", date_str, save_dir)
                manifest["klines_1s"] += len(rows)
                log(f"[backfill] 1s klines {date_str}: {len(rows)} rows ({src})")
            except Exception as e:
                manifest["errors"].append(f"1s {date_str}: {e}"); log(f"[backfill] 1s {date_str} FAIL: {e}")
        if want_agg:
            try:
                rows, src = fetch_aggtrades_day(symbol, date_str, save_dir)
                all_agg.extend(rows); manifest["aggtrades"] += len(rows)
                log(f"[backfill] aggTrades {date_str}: {len(rows)} rows ({src})")
            except Exception as e:
                manifest["errors"].append(f"agg {date_str}: {e}"); log(f"[backfill] aggTrades {date_str} FAIL: {e}")
    # derived summary across the whole window
    try:
        summ = derive_5min_summary(all_1m, all_agg, os.path.join(save_dir, "signal_summary_5min.csv"))
        manifest["summary_windows"] = len(summ)
        if summ:
            reg = defaultdict(int)
            for r in summ: reg[r["regime"]] += 1
            manifest["regime_counts"] = dict(reg)
            log(f"[backfill] summary: {len(summ)} 5-min windows, regimes={dict(reg)}")
    except Exception as e:
        manifest["errors"].append(f"summary: {e}"); log(f"[backfill] summary FAIL: {e}")
    manifest["finished"] = time.time()
    with open(os.path.join(save_dir, "manifest.json"), "w") as f:
        json.dump(manifest, f, indent=2)
    return manifest


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", default="BTCUSDT")
    ap.add_argument("--days", type=int, default=7)
    ap.add_argument("--out", default="./data")
    ap.add_argument("--no-1s", action="store_true")
    ap.add_argument("--no-agg", action="store_true")
    a = ap.parse_args()
    m = run_backfill(a.symbol, a.days, a.out, want_1s=not a.no_1s, want_agg=not a.no_agg)
    print(json.dumps(m, indent=2))
