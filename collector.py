#!/usr/bin/env python3
"""
collector.py  —  AMSA Data Collector service (Railway web entrypoint).

On boot it kicks off (in background threads, so the dashboard is live immediately):
  - Binance week backfill  (klines 1m/1s + aggTrades + derived 5-min outcomes/regime summary)   [real data]
  - Polymarket retroactive attempt   (honest: ~empty for 5-min markets)                          [opt, on by default]
  - Polymarket forward recorder      (record-only WS -> rotating JSONL; the real PM data path)    [opt-in via env]
Then it serves a dashboard with DOWNLOAD BUTTONS for everything collected, on $PORT.

ENV (all optional):
  SYMBOL=BTCUSDT  LOOKBACK_DAYS=7  DATA_DIR=/data  PORT=8080
  PULL_1M=1  PULL_1S=1  PULL_AGG=1            # which Binance datasets
  PM_RETRO=1  PM_RETRO_SAMPLE=40             # retroactive PM attempt
  PM_RECORD=0                                 # forward PM recorder (set 1 to start banking real PM data)
  AUTORUN=1                                    # run the backfill automatically on boot (else wait for the dashboard button)

NOTE: the network fetches require open egress (Binance / Polymarket). They run fine on Railway;
they cannot run in a sandbox locked to package registries.
"""
import os, io, json, time, threading, asyncio, http.server, socketserver, urllib.parse, zipfile, glob, tempfile

import binance_backfill as BB
import polymarket as PM

CFG = {
    "symbol": os.environ.get("SYMBOL", "BTCUSDT"),
    "days": int(os.environ.get("LOOKBACK_DAYS", "7")),
    "data_dir": os.environ.get("DATA_DIR", "./data"),
    "port": int(os.environ.get("PORT", "8080")),
    "pull_1m": os.environ.get("PULL_1M", "1") == "1",
    "pull_1s": os.environ.get("PULL_1S", "1") == "1",
    "pull_agg": os.environ.get("PULL_AGG", "1") == "1",
    "pm_retro": os.environ.get("PM_RETRO", "1") == "1",
    "pm_retro_sample": int(os.environ.get("PM_RETRO_SAMPLE", "40")),
    "pm_record": os.environ.get("PM_RECORD", "0") == "1",
    "autorun": os.environ.get("AUTORUN", "1") == "1",
}
os.makedirs(CFG["data_dir"], exist_ok=True)

STATE = {"phase": "idle", "log": [], "backfill": None, "pm_retro": None,
         "pm_record_running": False, "started": time.time()}
_LOCK = threading.Lock()


def log(msg):
    line = f"{time.strftime('%H:%M:%S')} {msg}"
    print(line, flush=True)
    with _LOCK:
        STATE["log"].append(line)
        STATE["log"] = STATE["log"][-200:]


# ----------------------------------------------------------------- background jobs
def _job_backfill():
    with _LOCK: STATE["phase"] = "binance_backfill"
    log("[collector] starting Binance week backfill...")
    try:
        m = BB.run_backfill(CFG["symbol"], CFG["days"], CFG["data_dir"],
                            want_1m=CFG["pull_1m"], want_1s=CFG["pull_1s"], want_agg=CFG["pull_agg"], log=log)
        with _LOCK: STATE["backfill"] = m
        log("[collector] Binance backfill done.")
    except Exception as e:
        log(f"[collector] backfill ERROR: {e}")
    if CFG["pm_retro"]:
        with _LOCK: STATE["phase"] = "pm_retroactive"
        log("[collector] Polymarket retroactive attempt (expect ~empty for 5-min markets)...")
        try:
            r = PM.run_retroactive(CFG["data_dir"], sample=CFG["pm_retro_sample"], lookback_days=CFG["days"], log=log)
            with _LOCK: STATE["pm_retro"] = r
        except Exception as e:
            log(f"[collector] pm-retro ERROR: {e}")
    with _LOCK: STATE["phase"] = "serving"
    log("[collector] collection complete — files ready to download.")


def _job_recorder():
    with _LOCK: STATE["pm_record_running"] = True
    log("[collector] starting Polymarket FORWARD recorder (record-only)...")
    try:
        asyncio.run(PM.run_recorder(CFG["data_dir"], log=log))
    except Exception as e:
        log(f"[collector] recorder ERROR: {e}")
    with _LOCK: STATE["pm_record_running"] = False


def start_jobs():
    if CFG["autorun"]:
        threading.Thread(target=_job_backfill, daemon=True).start()
    if CFG["pm_record"]:
        threading.Thread(target=_job_recorder, daemon=True).start()


# ----------------------------------------------------------------- file listing
def list_files():
    files = []
    for p in sorted(glob.glob(os.path.join(CFG["data_dir"], "*"))):
        if os.path.isfile(p):
            files.append({"name": os.path.basename(p), "size": os.path.getsize(p),
                          "mtime": os.path.getmtime(p)})
    return files


def build_zip_to_tempfile():
    """Build a zip of all collected files into a TEMP FILE on disk, then return its path + size.

    Why a temp file and not streaming: the previous version hand-rolled HTTP chunked-transfer framing
    straight into the socket. Python's http.server does not guarantee the client sees a clean chunked
    *decode*, so the chunk-size markers leaked into the saved bytes and corrupted the zip. Building to a
    real temp file lets us serve with a correct Content-Length and ZERO transfer framing — the bytes the
    client saves are exactly the zip. It is written incrementally (ZipFile streams each member through),
    so memory stays flat even for GB-scale aggTrades; only disk is used, then the temp file is removed.
    """
    fd, tmp = tempfile.mkstemp(prefix="amsa_bundle_", suffix=".zip", dir=CFG["data_dir"])
    os.close(fd)
    with zipfile.ZipFile(tmp, "w", zipfile.ZIP_DEFLATED, allowZip64=True) as z:
        for f in list_files():
            if f["name"].startswith("amsa_bundle_"):   # never zip a stray bundle into itself
                continue
            z.write(os.path.join(CFG["data_dir"], f["name"]), f["name"])
    return tmp, os.path.getsize(tmp)


# ----------------------------------------------------------------- HTTP server
DASH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dashboard.html")


class Handler(http.server.BaseHTTPRequestHandler):
    def _send(self, code, body, ctype="application/json", headers=None):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        if isinstance(body, str):
            body = body.encode()
        self.wfile.write(body)

    def log_message(self, *a):
        pass

    def do_GET(self):
        path = urllib.parse.urlparse(self.path).path
        q = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
        if path in ("/", "/index.html"):
            try:
                with open(DASH, "rb") as f:
                    self._send(200, f.read(), "text/html; charset=utf-8")
            except FileNotFoundError:
                self._send(200, "<h1>AMSA Data Collector</h1><p>dashboard.html missing</p>", "text/html")
        elif path == "/api/status":
            with _LOCK:
                st = dict(STATE)
            st["files"] = list_files()
            st["cfg"] = {k: CFG[k] for k in ("symbol", "days", "pull_1m", "pull_1s", "pull_agg",
                                             "pm_retro", "pm_record", "autorun")}
            st["uptime_s"] = int(time.time() - STATE["started"])
            self._send(200, json.dumps(st))
        elif path == "/api/download":
            name = (q.get("f") or [""])[0]
            safe = os.path.basename(name)
            full = os.path.join(CFG["data_dir"], safe)
            if safe and os.path.isfile(full):
                with open(full, "rb") as f:
                    self._send(200, f.read(), "application/octet-stream",
                               {"Content-Disposition": f'attachment; filename="{safe}"'})
            else:
                self._send(404, json.dumps({"error": "not found"}))
        elif path == "/api/download_all":
            stamp = time.strftime("%Y-%m-%d_%H%MZ", time.gmtime())
            tmp = None
            try:
                tmp, size = build_zip_to_tempfile()
                self.send_response(200)
                self.send_header("Content-Type", "application/zip")
                self.send_header("Content-Disposition", f'attachment; filename="amsa_data_{stamp}.zip"')
                self.send_header("Content-Length", str(size))   # real length -> no transfer framing, clean bytes
                self.end_headers()
                with open(tmp, "rb") as zf:
                    while True:
                        chunk = zf.read(1024 * 256)   # 256KB disk reads -> flat memory, plain socket writes
                        if not chunk:
                            break
                        self.wfile.write(chunk)
            except (BrokenPipeError, ConnectionResetError):
                pass   # client cancelled the download
            except Exception as e:
                log(f"[collector] download_all ERROR: {e}")
            finally:
                if tmp and os.path.exists(tmp):
                    try: os.remove(tmp)
                    except OSError: pass
        else:
            self._send(404, json.dumps({"error": "unknown route"}))

    def do_POST(self):
        path = urllib.parse.urlparse(self.path).path
        if path == "/api/run_backfill":
            with _LOCK:
                busy = STATE["phase"] in ("binance_backfill", "pm_retroactive")
            if busy:
                self._send(409, json.dumps({"error": "already running"}))
            else:
                threading.Thread(target=_job_backfill, daemon=True).start()
                self._send(200, json.dumps({"ok": True, "started": True}))
        elif path == "/api/start_recorder":
            with _LOCK:
                running = STATE["pm_record_running"]
            if running:
                self._send(409, json.dumps({"error": "recorder already running"}))
            else:
                threading.Thread(target=_job_recorder, daemon=True).start()
                self._send(200, json.dumps({"ok": True, "recorder": "started"}))
        else:
            self._send(404, json.dumps({"error": "unknown route"}))


class ThreadingServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True
    allow_reuse_address = True


def main():
    log(f"BOOT AMSA Data Collector symbol={CFG['symbol']} days={CFG['days']} "
        f"autorun={CFG['autorun']} pm_record={CFG['pm_record']} data_dir={CFG['data_dir']}")
    start_jobs()
    srv = ThreadingServer(("0.0.0.0", CFG["port"]), Handler)
    log(f"[collector] dashboard on :{CFG['port']}")
    srv.serve_forever()


if __name__ == "__main__":
    main()
