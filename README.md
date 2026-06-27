# AMSA Data Collector

A **standalone** Railway service (not the trading bot) that downloads a week of market data and
serves a dashboard to download it. Deploy it alongside the bot; it never trades.

## What you actually get (read this first)

| Source | Retroactive (past week) | Notes |
|---|---|---|
| **Binance klines** (1m, 1s) | ✅ full | from `data.binance.vision` daily dumps; REST fallback for today |
| **Binance aggTrades** (signed flow) | ✅ full | lets you reconstruct CVD / OFI / umbrella offline |
| **Derived 5-min outcomes + regime** | ✅ full | UP/DOWN per window + TREND/CHOP/MIXED classification |
| **Polymarket price history** | ⚠️ ~empty | prices-history returns empty below 12h fidelity for resolved 5-min markets — a documented limitation, not a bug |
| **Polymarket book/tape** | ❌ retroactive / ✅ forward | ephemeral markets — the **forward recorder** is the only way to get real PM data |

**Bottom line:** Binance gives you a genuine, complete week of multi-regime data with deterministic
5-min outcomes — exactly the chop-vs-trend ground truth worth having. Polymarket history can't be
pulled back; to get real PM book/tape you must **record it forward** (a week of recording = a week of data).

## Deploy (GitHub → Railway)
1. Push this folder to a GitHub repo.
2. New Railway project → Deploy from that repo. It reads `Procfile` (`web: python collector.py`).
3. **Mount a volume** and set `DATA_DIR=/data` so downloads survive restarts (otherwise data is on the
   container's ephemeral disk and is lost on redeploy).
4. Open the service URL → the dashboard. The backfill auto-runs on boot; watch progress, then download.

> The network fetches require open egress (Binance / Polymarket). They run on Railway. They will NOT
> run in a sandbox restricted to package registries.

## Configuration (all env vars optional)
```
SYMBOL=BTCUSDT        LOOKBACK_DAYS=7      DATA_DIR=/data       PORT=8080
PULL_1M=1  PULL_1S=1  PULL_AGG=1           # which Binance datasets to pull
PM_RETRO=1            PM_RETRO_SAMPLE=40    # retroactive PM attempt (bounded sample; expect ~empty)
PM_RECORD=0                                # set 1 to START the forward PM recorder (the real PM data)
AUTORUN=1                                  # run backfill on boot (else trigger it from the dashboard)
```
Heavy pulls: `aggTrades` for a week is hundreds of MB/day raw — set `PULL_AGG=0` if you only need
klines + outcomes. `PULL_1S=0` drops the 1-second klines (also large).

## Dashboard / endpoints
- `/` — dashboard: live status, regime mix, file list with ⬇ buttons, log, "Download ALL (zip)".
- `GET /api/status` — JSON progress + file manifest.
- `GET /api/download?f=<name>` — download one file.
- `GET /api/download_all` — zip of everything collected.
- `POST /api/run_backfill` — re-run the backfill.
- `POST /api/start_recorder` — start the forward PM recorder.

## Output files (in DATA_DIR)
- `klines_1m_<date>.csv`, `klines_1s_<date>.csv` — raw OHLCV per UTC day.
- `aggTrades_<date>.csv` — raw signed trades per UTC day.
- `signal_summary_5min.csv` — per 5-min window: open, close, **outcome (UP/DOWN)**, move_bps, range_bps,
  realized_vol_bps, **cvd_delta_usd**, n_trades, directionality, **regime (TREND/CHOP/MIXED)**.
- `manifest.json` — counts, date range, regime breakdown, errors.
- `pm_retroactive_report.json` (+ `.csv` if anything came back) — what the PM retro attempt actually returned.
- `pm_record_<date>_<hour>Z.jsonl` — forward-recorded PM WS events (only if `PM_RECORD=1`).

## Run a piece standalone (in an env with egress)
```
python binance_backfill.py --symbol BTCUSDT --days 7 --out ./data        # Binance only
python binance_backfill.py --days 7 --no-1s --no-agg                      # klines 1m only (light)
python polymarket.py --mode retro --sample 40                            # see the PM emptiness
python polymarket.py --mode record                                       # forward recorder
```

## The 5-min outcome / regime logic
Each aligned 5-min window's outcome is the Binance-kline approximation of how a `btc-updown-5m` market
resolves: **UP if window close > window open**. `directionality = |close-open| / (high-low)` separates a
clean trend (~1) from chop (~0); combined with the net move it tags TREND / CHOP / MIXED. This is the
lever the strategy work has been missing — a labeled week of regimes to test against.
