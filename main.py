"""v13: Self-contained multi-layer statistical execution engine for Roostoo.

Combines high-performance market feed connectivity, real-time technical momentum indicators,
and expected-value execution in a single unified script.

Features:
1. Online Technical Momentum & Regime Tracking:
   - Real-time online EMAs (10s fast, 60s medium, 120s baseline)
   - Online RSI (60s Wilder's smoothed momentum)
   - Volatility floor (filtering illiquid/flat market regimes)
   - Overextension exhaustion filter: avoids late entries (>25bp above EMA60 or RSI > 65)
   - Conviction sizing: scales capital up to 1.25x on consolidation breakouts

2. Statistical Expected Value Model:
   - Bayesian learning of passive fill probabilities and execution slippage
   - Dynamic size allocation based on EV and technical conviction

3. Infrastructure:
   - High-performance direct TLS connection pool with server clock offset calibration
   - Multi-connection raced Binance websocket feeds with lock-free deduplication
   - Self-contained execution without external local module dependencies

Usage:
   python main.py [DUR_S] [LOSS_LIMIT_USD] [BUFFER_BP] [MAX_FRAC]
"""
import hashlib
import hmac
import json
import math
import multiprocessing as mp
import os
import queue
import socket
import ssl
import statistics
import sys
import threading
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor

try:
    import orjson
    import websocket
except ImportError as e:
    sys.exit(f"Missing required dependency: {e}. Please run: pip install orjson websocket-client")

# ---------- CLI arguments & Engine Configuration ----------
DUR = float(sys.argv[1]) if len(sys.argv) > 1 else 3600
LOSS_LIMIT = float(sys.argv[2]) if len(sys.argv) > 2 else 600
BUFFER = float(sys.argv[3]) if len(sys.argv) > 3 else 1.0
MAX_FRAC = float(sys.argv[4]) if len(sys.argv) > 4 else 0.50
BASE_FRAC, SLOPE_FRAC = 0.15, 0.06   # fraction of equity per trade = BASE + SLOPE * EV_bp
MARGIN = 10                          # ms safety margin on order timing
INGEST, RECV = 45, 110
MAX_LAG = 150                        # ms feed synchronization threshold
MAX_EXIT = 5                         # passive limit order attempts before fallback fill
FEE_T, FEE_M = 10.0, 5.0
PRIOR = {"pm": 0.6, "D": 1.5, "p": 0.9}
SHRINK = 5                           # pseudo-observations behind each coin's estimate
ALPHA = 0.15                         # EWMA weight for learned quantities
ALLOW_SHORT = os.environ.get("ALLOW_SHORT") == "1"
PRE_G = 12.0                         # bp: signal threshold filter before EV calculation
TAG = "v13"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def p(name):
    return os.path.join(BASE_DIR, name)


class Tee:
    def __init__(self, filename, stream):
        self.file = open(filename, "a", encoding="utf-8", buffering=1)
        self.stream = stream

    def write(self, data):
        try:
            self.stream.write(data)
            self.stream.flush()
        except Exception:
            pass
        try:
            self.file.write(data)
            self.file.flush()
        except Exception:
            pass

    def flush(self):
        try:
            self.stream.flush()
        except Exception:
            pass
        try:
            self.file.flush()
        except Exception:
            pass


def log(*a):
    line = time.strftime("%Y-%m-%d %H:%M:%S ") + " ".join(str(x) for x in a)
    print(line, flush=True)
    with open(p(f"{TAG}_events.log"), "a", encoding="utf-8") as f:
        f.write(line + "\n")


# ---------- Roostoo Client Infrastructure ----------
HOST = "mock-api.roostoo.com"
KEY = os.environ.get("ROOSTOO_KEY", "DyIoBKjPbH94bv34lAkUcacLU3sElxA2X5XBHTeyM39nmHAKuvubsYDXFO0YaRZD")
SECRET = os.environ.get("ROOSTOO_SECRET", "y7PwOlatLVweQVvaC32Zwno9DDdY2POUDifAi7uMzUjsQoR5yb4rWM6K3dgIwKtA").encode()
POP_ECS = {"SIN": "13.228.0.0/24", "NRT": "13.112.0.0/24", "SYD": "3.24.0.0/24", "HKG": "18.162.0.0/24"}
CRLF = bytes([13, 10])
_ctx = ssl.create_default_context()


def pop_ips(pop="SIN"):
    """CloudFront edge IPs for host in the given POP via Google DoH with EDNS client subnet."""
    ecs = POP_ECS.get(pop)
    if ecs is None:
        return sorted({a[4][0] for a in socket.getaddrinfo(HOST, 443, socket.AF_INET)})
    u = f"https://dns.google/resolve?name={HOST}&type=A&edns_client_subnet={ecs}"
    try:
        return [a["data"] for a in json.load(urllib.request.urlopen(u, timeout=10)).get("Answer", []) if a["type"] == 1]
    except Exception:
        return sorted({a[4][0] for a in socket.getaddrinfo(HOST, 443, socket.AF_INET)})


class Conn:
    def __init__(self, ip):
        self.ip = ip
        self.s = None
        self.last = 0.0
        self.connect()

    def connect(self):
        if self.s:
            try:
                self.s.close()
            except OSError:
                pass
        raw = socket.create_connection((self.ip, 443), timeout=10)
        raw.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.s = _ctx.wrap_socket(raw, server_hostname=HOST)
        self.buf = b""

    def request(self, method, path, body=b"", headers=None):
        lines = [f"{method} {path} HTTP/1.1", f"Host: {HOST}", "Connection: keep-alive"]
        lines += [f"{k}: {v}" for k, v in (headers or {}).items()]
        if method == "POST":
            lines.append(f"Content-Length: {len(body)}")
        data = CRLF.join(l.encode() for l in lines) + CRLF + CRLF + body
        for attempt in (0, 1):
            try:
                t0 = time.time()
                self.s.sendall(data)
                resp = self._read()
                self.last = time.time()
                return t0, self.last, resp
            except (OSError, ValueError):
                if attempt:
                    raise
                self.connect()

    def _read(self):
        buf = self.buf
        while CRLF + CRLF not in buf:
            d = self.s.recv(65536)
            if not d:
                raise OSError("closed")
            buf += d
        hd, rest = buf.split(CRLF + CRLF, 1)
        lines = hd.split(CRLF)
        self.headers = lines
        cl, chunked = None, False
        for l in lines[1:]:
            k, _, v = l.partition(b":")
            k = k.strip().lower()
            if k == b"content-length":
                cl = int(v)
            elif k == b"transfer-encoding" and b"chunked" in v.lower():
                chunked = True
        if chunked:
            body = b""
            while True:
                while CRLF not in rest:
                    rest += self._recv()
                size_line, rest = rest.split(CRLF, 1)
                n = int(size_line.split(b";")[0], 16)
                while len(rest) < n + 2:
                    rest += self._recv()
                body, rest = body + rest[:n], rest[n + 2:]
                if n == 0:
                    break
            self.buf = rest
            return self._parse(lines, body)
        if cl is None:
            raise ValueError("no content-length")
        while len(rest) < cl:
            d = self.s.recv(65536)
            if not d:
                raise OSError("closed")
            rest += d
        self.buf = rest[cl:]
        return self._parse(lines, rest[:cl])

    def _recv(self):
        d = self.s.recv(65536)
        if not d:
            raise OSError("closed")
        return d

    @staticmethod
    def _parse(lines, body):
        try:
            return json.loads(body)
        except ValueError:
            return {"Success": False, "ErrMsg": lines[0].decode() + " " + body.decode(errors="replace")}


class Roostoo:
    def __init__(self, n=4, pop="SIN", ips=None):
        self.ips = ips or pop_ips(pop)
        self.conns = [Conn(self.ips[i % len(self.ips)]) for i in range(n)]
        self.free = list(self.conns)
        self.cv = threading.Condition()
        self.offset = 0.0  # server_ms - local_ms
        self.owd = 0.0     # outbound one-way delay ms
        self._ka = None

    def _query(self, params, stamp):
        params = dict(params or {})
        if stamp:
            params["timestamp"] = str(int(time.time() * 1000 + self.offset))
        return "&".join(f"{k}={params[k]}" for k in sorted(params))

    def _acquire(self):
        with self.cv:
            while not self.free:
                self.cv.wait()
            c = max(self.free, key=lambda x: x.last)
            self.free.remove(c)
            return c

    def _release(self, c):
        with self.cv:
            self.free.append(c)
            self.cv.notify()

    def call(self, method, path, params=None, signed=True, timed=False):
        c = self._acquire()
        try:
            qs = self._query(params, signed or path != "/v3/serverTime")
            h = {}
            if signed:
                h = {"RST-API-KEY": KEY, "MSG-SIGNATURE": hmac.new(SECRET, qs.encode(), hashlib.sha256).hexdigest()}
            if method == "GET":
                t0, t1, r = c.request("GET", path + ("?" + qs if qs else ""), headers=h)
            else:
                h["Content-Type"] = "application/x-www-form-urlencoded"
                t0, t1, r = c.request("POST", path, qs.encode(), h)
        finally:
            self._release(c)
        return (t0 * 1000, t1 * 1000, r) if timed else r

    def calibrate(self, n=15):
        rows = []
        for _ in range(n):
            t0, t1, r = self.call("GET", "/v3/serverTime", signed=False, timed=True)
            rows.append((t0, t1, r["ServerTime"]))
            time.sleep(0.05)
        best = sorted(rows, key=lambda x: x[1] - x[0])[:3]
        self.offset = statistics.median(ts - (t0 + t1) / 2 for t0, t1, ts in best)
        self.owd = statistics.median((t1 - t0) / 2 for t0, t1, ts in best)
        return self.offset, self.owd

    def keepalive(self, every=1.0):
        def run():
            while True:
                time.sleep(every / 4)
                now = time.time()
                for c in self.conns:
                    if now - c.last <= every:
                        continue
                    with self.cv:
                        if c not in self.free:
                            continue
                        self.free.remove(c)
                    try:
                        c.request("GET", "/v3/serverTime")
                    except (OSError, ValueError):
                        try:
                            c.connect()
                        except OSError:
                            pass
                    finally:
                        self._release(c)
        self._ka = threading.Thread(target=run, daemon=True)
        self._ka.start()


# ---------- Binance Live Market Feed ----------
BASES = ("wss://stream.binance.com:9443", "wss://stream.binance.com:443", "wss://data-stream.binance.vision")


def _feed_worker(base, syms, last_u, last_E, q):
    idx = {s: i for i, s in enumerate(syms)}
    url = base + "/stream?streams=" + "/".join(f"{s.lower()}@bookTicker/{s.lower()}@ticker" for s in syms)

    def on(_, m):
        d = orjson.loads(m)
        st, x = d["stream"], d["data"]
        i = idx[x["s"]]
        if st[-4:] == "cker" and st[-10:] == "bookTicker":
            u = x["u"]
            if u <= last_u[i]:
                return
            last_u[i] = u
            q.put((0, i, float(x["b"]), float(x["a"]), u, time.time()))
        else:
            E = x["E"]
            if E <= last_E[i]:
                return
            last_E[i] = E
            q.put((1, i, float(x["b"]), float(x["a"]), E, time.time()))

    while True:
        try:
            websocket.WebSocketApp(url, on_message=on).run_forever(ping_interval=20)
        except Exception:
            pass
        time.sleep(1)


class Feed:
    def __init__(self, syms, bases=BASES, copies=2, boff=0.0):
        self.syms = list(syms)
        self.bases = [b for b in bases for _ in range(copies)]
        self.boff = boff
        self._lag = []
        self._transit = 0.0
        self._last = 0.0

    def start(self, on_book, on_ticker):
        ctx = mp.get_context("spawn")
        n = len(self.syms)
        last_u = ctx.RawArray("q", n)
        last_E = ctx.RawArray("q", n)
        self.q = ctx.Queue()
        self.procs = [ctx.Process(target=_feed_worker, args=(b, self.syms, last_u, last_E, self.q), daemon=True) for b in self.bases]
        for p_proc in self.procs:
            p_proc.start()
        lu, lE = [0] * n, [0] * n

        def pump():
            syms, q = self.syms, self.q
            while True:
                kind, i, b, a, k, t = q.get()
                self._last = time.time()
                self._transit = (self._last - t) * 1000
                if kind == 0:
                    if k <= lu[i]:
                        continue
                    lu[i] = k
                    on_book(syms[i], b, a)
                else:
                    if k <= lE[i]:
                        continue
                    lE[i] = k
                    now = time.time()
                    self._lag.append((now, now * 1000 + self.boff - k))
                    if len(self._lag) > 400:
                        del self._lag[:200]
                    on_ticker(syms[i], b, a, k)
        threading.Thread(target=pump, daemon=True).start()

    def stop(self):
        for p_proc in self.procs:
            try:
                p_proc.terminate()
            except Exception:
                pass

    def healthy(self, max_transit=40.0, max_median_lag=200.0, max_silence=0.5):
        now = time.time()
        if now - self._last > max_silence or self._transit > max_transit:
            return False
        recent = sorted(l for t, l in self._lag[-200:] if now - t <= 1.0)
        return bool(recent) and recent[len(recent) // 2] <= max_median_lag

    def lag_stats_ms(self, window=1.0):
        now = time.time()
        recent = [l for t, l in self._lag[-200:] if now - t <= window]
        if not recent:
            return float("inf"), float("inf")
        s = sorted(recent)
        return s[len(s) // 2], s[-1]

    def lag_ms(self, window=1.0):
        now = time.time()
        recent = [l for t, l in self._lag[-200:] if now - t <= window]
        return max(recent) if recent else float("inf")


# ---------- Technical Indicator Layer ----------
class TechnicalIndicatorTracker:
    def __init__(
        self,
        ema_fast=10,
        ema_med=60,
        ema_slow=120,
        max_px_ema=25.0,
        max_rsi=65.0,
        min_vol=2.5,
        stretch_px_ema=15.0,
        stretch_rsi=58.0
    ):
        self.a_fast = 2.0 / (ema_fast + 1.0)
        self.a_med = 2.0 / (ema_med + 1.0)
        self.a_slow = 2.0 / (ema_slow + 1.0)
        self.max_px_ema = float(max_px_ema)
        self.max_rsi = float(max_rsi)
        self.min_vol = float(min_vol)
        self.stretch_px_ema = float(stretch_px_ema)
        self.stretch_rsi = float(stretch_rsi)
        self.state = {}

    def update(self, sym, mid):
        s = self.state.get(sym)
        if s is None:
            self.state[sym] = {
                'px': mid,
                'last_px': mid,
                'ema_fast': mid,
                'ema_med': mid,
                'ema_slow': mid,
                'gain': 0.0,
                'loss': 0.0,
                'var': 25.0,
                'n': 1
            }
            return
        s['last_px'] = s['px']
        s['px'] = mid
        s['n'] += 1
        s['ema_fast'] += self.a_fast * (mid - s['ema_fast'])
        s['ema_med'] += self.a_med * (mid - s['ema_med'])
        s['ema_slow'] += self.a_slow * (mid - s['ema_slow'])
        diff = mid - s['last_px']
        s['gain'] += self.a_med * (max(0.0, diff) - s['gain'])
        s['loss'] += self.a_med * (max(0.0, -diff) - s['loss'])
        r = (mid / s['last_px'] - 1.0) * 1e4 if s['last_px'] > 0 else 0.0
        s['var'] += self.a_med * (r * r - s['var'])

    def evaluate(self, sym, side, px=None):
        s = self.state.get(sym)
        if not s or s['n'] < 30:
            return True, 1.0, "WARMUP", {}
        curr_px = px if px else s['px']
        px_vs_ema = (curr_px / s['ema_med'] - 1.0) * 1e4
        trend = (s['ema_fast'] / s['ema_med'] - 1.0) * 1e4
        tot_loss = s['loss'] + 1e-9
        rsi = 100.0 * s['gain'] / (s['gain'] + tot_loss)
        vol = math.sqrt(max(0.0, s['var']))

        metrics = {
            'px_vs_ema60': round(float(px_vs_ema), 2),
            'trend_10_60': round(float(trend), 2),
            'rsi_60': round(float(rsi), 1),
            'vol_60': round(float(vol), 2)
        }

        if vol < self.min_vol:
            return False, 0.0, "LOW_VOLATILITY", metrics

        if side == "UP":
            if px_vs_ema > self.max_px_ema or rsi > self.max_rsi:
                return False, 0.0, "OVERBOUGHT_EXHAUSTION", metrics
            if px_vs_ema > self.stretch_px_ema or rsi > self.stretch_rsi:
                return True, 0.6, "MODERATE_STRETCH", metrics
            if -10.0 <= px_vs_ema <= 10.0 and 42.0 <= rsi <= 56.0:
                return True, 1.25, "CONSOLIDATION_BREAKOUT", metrics
            return True, 1.0, "NORMAL", metrics
        elif side == "DOWN":
            if px_vs_ema < -self.max_px_ema or rsi < (100.0 - self.max_rsi):
                return False, 0.0, "OVERSOLD_EXHAUSTION", metrics
            if px_vs_ema < -self.stretch_px_ema or rsi < (100.0 - self.stretch_rsi):
                return True, 0.6, "MODERATE_STRETCH", metrics
            if -10.0 <= px_vs_ema <= 10.0 and 44.0 <= rsi <= 58.0:
                return True, 1.25, "BREAKDOWN_BONUS", metrics
            return True, 1.0, "NORMAL", metrics
        return True, 1.0, "NORMAL", metrics


# ---------- Pre-seeded Model Priors ----------
DEFAULT_MODEL_JSON = '{"coin":{"ENAUSDT":{"pm":0.6787754716108529,"n_pm":21,"D_UP":1.6132113835691888,"n_D_UP":21},"NEARUSDT":{"pm":0.764728302423031,"n_pm":40,"D_UP":1.2886008543437555,"n_D_UP":40},"SUSDT":{"pm":0.9777446323201588,"n_pm":23,"D_UP":9.739615294398686,"n_D_UP":23},"TRUMPUSDT":{"pm":0.6100702338043424,"n_pm":28,"D_UP":7.899017564285361,"n_D_UP":28},"PENGUUSDT":{"pm":0.85,"n_pm":5,"D_UP":6.292356008368541,"n_D_UP":5},"SOMIUSDT":{"pm":0.741625,"n_pm":10,"D_UP":1.8415076700322022,"n_D_UP":10},"HBARUSDT":{"pm":0.5886951635510924,"n_pm":22,"D_UP":0.9468079942215151,"n_D_UP":22},"WLDUSDT":{"pm":0.7933206892916311,"n_pm":40,"D_UP":-2.3781692200774196,"n_D_UP":40},"PUMPUSDT":{"pm":0.4891033363375815,"n_pm":40,"D_UP":5.473879208503597,"n_D_UP":40},"TUTUSDT":{"pm":0.6494826398828125,"n_pm":16,"D_UP":-4.711229589137411,"n_D_UP":16},"WIFUSDT":{"pm":0.705143265625,"n_pm":10,"D_UP":1.7727610995334182,"n_D_UP":10},"FORMUSDT":{"pm":0.79857083828125,"n_pm":8,"D_UP":3.252288416672712,"n_D_UP":8},"ICPUSDT":{"pm":0.6000094749609375,"n_pm":9,"D_UP":6.4220026573405455,"n_D_UP":9},"UNIUSDT":{"pm":0.5851011845574744,"n_pm":32,"D_UP":0.361416139893487,"n_D_UP":32},"MIRAUSDT":{"pm":0.85,"n_pm":7,"D_UP":-5.440353336436539,"n_D_UP":7},"AVNTUSDT":{"pm":0.27879468749999997,"n_pm":6,"D_UP":7.170947185230146,"n_D_UP":6},"EDENUSDT":{"pm":1.0,"n_pm":2,"D_UP":7.825679009454856,"n_D_UP":2},"BMTUSDT":{"pm":1.0,"n_pm":4,"D_UP":1.3820566387624904,"n_D_UP":4},"PLUMEUSDT":{"pm":0.4907194153352536,"n_pm":40,"D_UP":1.7114442166123451,"n_D_UP":40},"LINKUSDT":{"pm":0.6762643037167968,"n_pm":10,"D_UP":-1.3022061241781728,"n_D_UP":10},"APTUSDT":{"pm":0.90788125,"n_pm":6,"D_UP":2.0839485184719435,"n_D_UP":6},"XPLUSDT":{"pm":0.415524579434394,"n_pm":30,"D_UP":4.994378884694777,"n_D_UP":30},"CRVUSDT":{"pm":0.9491325757784658,"n_pm":19,"D_UP":-3.945254562253086,"n_D_UP":19},"SEIUSDT":{"pm":1.0,"n_pm":4,"D_UP":2.0617994095601677,"n_D_UP":4},"LISTAUSDT":{"pm":1.0,"n_pm":1,"D_UP":11.198208286673506,"n_D_UP":1},"SUIUSDT":{"pm":0.60112197421875,"n_pm":8,"D_UP":-1.2614936400472523,"n_D_UP":8},"ZECUSDT":{"pm":0.5562946875,"n_pm":9,"D_UP":0.06731923371130456,"n_D_UP":9},"EIGENUSDT":{"pm":0.373074341626782,"n_pm":13,"D_UP":5.081528215140068,"n_D_UP":13},"LITEBUSDT":{"pm":0.891625,"n_pm":5,"D_UP":4.302450208976487,"n_D_UP":5},"POLUSDT":{"pm":0.6720062499999999,"n_pm":10,"D_UP":2.9514232136090026,"n_D_UP":10},"ONDOUSDT":{"pm":0.0,"n_pm":3,"D_UP":10.164469353897932,"n_D_UP":3},"FILUSDT":{"pm":1.0,"n_pm":1,"D_UP":-0.9683354313927381,"n_D_UP":1},"FETUSDT":{"pm":0.3523278030756788,"n_pm":31,"D_UP":-0.5391981795978852,"n_D_UP":31},"NBISBUSDT":{"pm":1.0,"n_pm":2,"D_UP":6.4939200516300755,"n_D_UP":2},"MSTRBUSDT":{"pm":1.0,"n_pm":1,"D_UP":4.560854834505523,"n_D_UP":1},"MUBUSDT":{"pm":0.6794295491221988,"n_pm":26,"D_UP":2.306226473100152,"n_D_UP":26},"GOOGLBUSDT":{"pm":1.0,"n_pm":10,"D_UP":2.7494978730066486,"n_D_UP":10},"ASTERUSDT":{"pm":0.76849677578125,"n_pm":8,"D_UP":2.0585893833468036,"n_D_UP":8},"ZENUSDT":{"pm":1.0,"n_pm":3,"D_UP":0.11295855870628868,"n_D_UP":3},"GLWBUSDT":{"pm":1.0,"n_pm":1,"D_UP":26.718800912349792,"n_D_UP":1},"CAKEUSDT":{"pm":0.0,"n_pm":1,"D_UP":11.63692785104642,"n_D_UP":1},"BIOUSDT":{"pm":0.7353576398828126,"n_pm":9,"D_UP":1.3247562430824122,"n_D_UP":9},"AVAXUSDT":{"pm":0.38697548437499996,"n_pm":7,"D_UP":2.8227559956168466,"n_D_UP":7},"LTCUSDT":{"pm":0.0,"n_pm":2,"D_UP":5.054258956444402,"n_D_UP":2},"PENDLEUSDT":{"pm":0.4887110201654417,"n_pm":22,"D_UP":-5.564148427231565,"n_D_UP":22},"AAVEUSDT":{"pm":0.6597200720884023,"n_pm":37,"D_UP":-5.209573391200944,"n_D_UP":37},"BONKUSDT":{"pm":0.0,"n_pm":1,"D_UP":25.77319587628857,"n_D_UP":1},"ARBUSDT":{"pm":0.15,"n_pm":2,"D_UP":4.136253041362337,"n_D_UP":2},"VIRTUALUSDT":{"pm":1.0,"n_pm":3,"D_UP":21.913553115813986,"n_D_UP":3},"STOUSDT":{"pm":0.0,"n_pm":1,"D_UP":0.0,"n_D_UP":1},"CBRSBUSDT":{"pm":0.7467682656249999,"n_pm":10,"D_UP":6.348411665511996,"n_D_UP":10},"HEMIUSDT":{"pm":0.741625,"n_pm":4,"D_UP":-9.920014468291916,"n_D_UP":4},"CFXUSDT":{"pm":1.0,"n_pm":3,"D_UP":8.092314634675478,"n_D_UP":3},"CRCLBUSDT":{"pm":0.85,"n_pm":2,"D_UP":-2.4351390744573003,"n_D_UP":2},"OPENUSDT":{"pm":0.0,"n_pm":3,"D_UP":-4.639530313892326,"n_D_UP":3},"PEPEUSDT":{"pm":0.0,"n_pm":1,"D_UP":0.0,"n_D_UP":1},"QCOMBUSDT":{"pm":1.0,"n_pm":1,"D_UP":7.583143754739119,"n_D_UP":1},"SPCXBUSDT":{"pm":1.0,"n_pm":1,"D_UP":-2.643404705260366,"n_D_UP":1},"SNDKBUSDT":{"pm":0.8091264212441406,"n_pm":11,"D_UP":3.751129986916437,"n_D_UP":11},"SHIBUSDT":{"pm":0.0,"n_pm":1,"D_UP":30,"n_D_UP":1},"TAOUSDT":{"pm":0.8724999999999999,"n_pm":8,"D_UP":-4.380460101540614,"n_D_UP":8},"BTCUSDT":{"pm":0.0,"n_pm":3,"D_UP":23.508398594225326,"n_D_UP":3},"LINEAUSDT":{"pm":0.0,"n_pm":1,"D_UP":-10.537407797681642,"n_D_UP":1},"SOLUSDT":{"pm":0.0,"n_pm":1,"D_UP":1.6842105263159546,"n_D_UP":1}},"glob":{"pm":0.6008674793537898,"D_UP":-1.9206417857141243,"D_DOWN":1.5,"p":0.9,"n":0}}'


# ---------- State & Execution Logic ----------
R = FEED = None
BOFF = 0.0
TECH = TechnicalIndicatorTracker()
info, snap, live = {}, {}, {}
lock = threading.Lock()
busy, open_usd = set(), {}
inflight = [False]
running = [False]
stop_reason = [None]
coin, glob = {}, {"pm": PRIOR["pm"], "D_UP": PRIOR["D"], "D_DOWN": PRIOR["D"], "p": PRIOR["p"], "n": 0}
S = {"equity": 0.0, "cum": 0.0, "n": 0, "miss": 0, "signals": 0, "lagged": 0, "checks": 0, "no_cap": 0, "jan": 0.0, "dn_skipped": 0, "tech_skipped": 0}
pool = ThreadPoolExecutor(24)
last_dn = {}


def signed(method, path, params=None):
    try:
        return R.call(method, path, params)
    except Exception as e:
        log("   request failed", path, type(e).__name__)
        return {"Success": False, "ErrMsg": "request failed: " + type(e).__name__}


def binance_offset():
    best = None
    for _ in range(10):
        try:
            t0 = time.time() * 1000
            st = json.load(urllib.request.urlopen("https://api.binance.com/api/v3/time", timeout=5))["serverTime"]
            t1 = time.time() * 1000
            if best is None or t1 - t0 < best[0]:
                best = (t1 - t0, st - (t0 + t1) / 2)
        except Exception:
            time.sleep(0.5)
    return best[1] if best else BOFF


def pair_of(sym):
    return sym[:-4] + "/USD"


def fq(sym, qty):
    pr = info[pair_of(sym)]["AmountPrecision"]
    q = math.floor(qty * 10**pr + 1e-9) / 10**pr
    return int(q) if pr == 0 else q


def fp(sym, px):
    return round(px, info[pair_of(sym)]["PricePrecision"])


def tick(sym):
    return 10 ** -info[pair_of(sym)]["PricePrecision"]


def slack_ms(sym):
    now_bn = time.time() * 1000 + BOFF
    E = snap[sym][3]
    m = max(1, math.ceil((now_bn - RECV - E) / 1000 + 1e-9))
    return E + 1000 * m + INGEST - (now_bn + R.owd + MARGIN)


def est(sym, key):
    c = coin.get(sym)
    if not c or key not in c:
        return glob[key]
    n = c["n_" + key]
    return (n * c[key] + SHRINK * glob[key]) / (n + SHRINK)


def learn(sym, key, x):
    c = coin.setdefault(sym, {})
    if key not in c:
        c[key], c["n_" + key] = x, 1
    else:
        c[key] += ALPHA * (x - c[key])
        c["n_" + key] = min(c["n_" + key] + 1, 40)
    glob[key] += 0.05 * (x - glob[key])


def ev_up(G, spread, sym):
    pm, D = est(sym, "pm"), est(sym, "D_UP")
    return G - FEE_T - (pm * FEE_M + (1 - pm) * (FEE_T + spread)) - D, pm, D


def ev_down(G, spread, sym):
    p_eff, D = glob["p"], est(sym, "D_DOWN")
    return p_eff * (G - 2 * FEE_T - D) - (1 - p_eff) * (2 * FEE_T + spread), p_eff, D


def place(sym, side, qty, px=None):
    o = {"pair": pair_of(sym), "side": side, "type": "LIMIT" if px else "MARKET", "quantity": qty}
    if px:
        o["price"] = px
    r = signed("POST", "/v3/place_order", o)
    for _ in range(5):
        msg = str(r.get("ErrMsg"))
        if r.get("Success") or "Available=" not in msg:
            break
        avail = fq(sym, float(msg.split("Available=")[1].split(",")[0]))
        locked = float(msg.split("Locked=")[1].split("(")[0]) if "Locked=" in msg else 0
        if locked > 0:
            time.sleep(0.4)
            r = signed("POST", "/v3/place_order", o)
        elif avail > 0:
            r = signed("POST", "/v3/place_order", dict(o, quantity=avail))
        else:
            break
    return (r.get("OrderDetail") or {}), (None if r.get("Success") else r.get("ErrMsg"))


def order_state(oid):
    return (signed("POST", "/v3/query_order", {"order_id": oid}).get("OrderMatched") or [{}])[0]


def fee_usd(od):
    return od.get("CommissionChargeValue", 0) * (1 if od.get("CommissionCoin") == "USD" else od.get("FilledAverPrice", 0))


def wait_snapshot(sym, timeout=3.0):
    seen, end = snap[sym][2], time.time() + timeout
    while snap[sym][2] == seen and time.time() < end:
        time.sleep(0.01)
    time.sleep(0.15)


def work_maker_sell(sym, qty):
    od = None
    for attempt in range(MAX_EXIT):
        t = tick(sym)
        px = fp(sym, max(live[sym][0], snap[sym][0] + t))
        if od is None:
            od, err = place(sym, "SELL", qty, px)
            if err or not od:
                log("   maker place failed", sym, qty, px, err)
                signed("POST", "/v3/cancel_order", {"pair": pair_of(sym)})
                od = None
                continue
            if od.get("Status") == "FILLED":
                return [od]
        wait_snapshot(sym)
        st = order_state(od["OrderID"])
        if st.get("Status") == "FILLED":
            return [st]
        nxt = fp(sym, max(live[sym][0], snap[sym][0] + t))
        if nxt == od.get("Price", px) and attempt < MAX_EXIT - 1:
            continue
        c = signed("POST", "/v3/cancel_order", {"order_id": od["OrderID"]})
        if od["OrderID"] not in (c.get("CanceledList") or []):
            for _ in range(4):
                st = order_state(od["OrderID"])
                if st.get("Status") == "FILLED":
                    return [st]
                if st.get("Status") in ("CANCELED", "CANCELLED"):
                    break
                time.sleep(0.3)
                signed("POST", "/v3/cancel_order", {"pair": pair_of(sym)})
        od = None
    signed("POST", "/v3/cancel_order", {"pair": pair_of(sym)})
    od, err = place(sym, "SELL", qty)
    if od.get("Status") == "FILLED":
        return [od]
    log("   FAILED to flatten", sym, qty, err)
    return []


def finish(rec):
    with lock:
        busy.discard(rec["sym"])
        open_usd.pop(rec["sym"], None)
        if "net" in rec:
            S["cum"] += rec["net"]
            S["n"] += 1
        elif rec.get("miss"):
            S["miss"] += 1
        rec["cum"] = S["cum"]
        with open(p(f"{TAG}_trades.jsonl"), "a", encoding="utf-8") as f:
            f.write(json.dumps(rec) + "\n")
    if "net" in rec:
        log(f"{rec['dir']:4s} {pair_of(rec['sym']):12s} G={rec['G']:5.1f} EV={rec['ev']:+5.1f}bp ${rec['usd']:>7,.0f} in={rec['in_px']} out={rec['out_px']} "
            f"{rec['out_role']:5s} {rec['secs']:4.1f}s net=${rec['net']:+8.2f} ({rec['net_bp']:+6.1f}bp) cum=${S['cum']:+.2f} [{rec.get('tech_tag', 'NORM')}]")
        if S["cum"] < -LOSS_LIMIT and running[0]:
            stop_reason[0] = f"loss limit hit: cum ${S['cum']:.2f}"
            running[0] = False


def trade_up(rec, qty, px):
    sym = rec["sym"]
    try:
        t0 = time.time() * 1000
        try:
            od, err = place(sym, "BUY", qty, px)
            if od.get("Status") == "PENDING":
                c = signed("POST", "/v3/cancel_order", {"order_id": od["OrderID"]})
                if od["OrderID"] not in (c.get("CanceledList") or []):
                    od = order_state(od["OrderID"])
        finally:
            inflight[0] = False
        rec.update(sent=t0, create=od.get("CreateTimestamp"), status0=od.get("Status") or err)
        if od.get("Status") != "FILLED":
            rec["miss"] = True
            return
        q_in, in_px = od["FilledQuantity"], od["FilledAverPrice"]
        outs = work_maker_sell(sym, fq(sym, q_in))
        q_out = sum(o["FilledQuantity"] for o in outs)
        if not q_out:
            rec["err"] = "exit failed"
            signed("POST", "/v3/cancel_order", {"pair": pair_of(sym)})
            return
        out_px = sum(o["FilledQuantity"] * o["FilledAverPrice"] for o in outs) / q_out
        fees = fee_usd(od) + sum(fee_usd(o) for o in outs)
        usd = q_in * in_px
        net = (out_px - in_px) * min(q_in, q_out) - fees
        role = outs[-1].get("Role", "?")
        rec.update(usd=usd, in_px=in_px, out_px=out_px, out_role=role, fees=fees, net=net, net_bp=net / usd * 1e4,
                   secs=time.time() - rec["t"], at_quote=abs(in_px / px - 1) < 1e-9)
        learn(sym, "pm", 1.0 if role == "MAKER" else 0.0)
        learn(sym, "D_UP", max(-30, min(30, rec["G"] - (out_px / in_px - 1) * 1e4)))
    except Exception as e:
        rec["err"] = repr(e)
        log("ERR", sym, repr(e))
    finally:
        finish(rec)


def trade_down(rec, usd, ref):
    sym = rec["sym"]
    try:
        t0 = time.time() * 1000
        try:
            r = signed("POST", "/v6/short_open", {"pair": pair_of(sym), "collateral": round(usd, 2)})
        finally:
            inflight[0] = False
        rec.update(sent=t0, create=r.get("CreateTimestamp"), status0=r.get("Status") or r.get("ErrMsg"))
        if not r.get("Success") or r.get("Status") != "OPEN":
            rec["miss"] = True
            return
        in_px, q, fee_in = r["EntryPrice"], r["ShortQty"], r.get("OpenFee", 0)
        at_quote = abs(in_px / ref - 1) < 1e-9
        glob["p"] += 0.05 * ((1.0 if at_quote else 0.0) - glob["p"])
        if at_quote:
            wait_snapshot(sym)
        c = signed("POST", "/v6/short_close", {"pair": pair_of(sym)})
        for _ in range(3):
            if c.get("Success"):
                break
            time.sleep(0.5)
            c = signed("POST", "/v6/short_close", {"pair": pair_of(sym)})
        if not c.get("Success"):
            rec["err"] = "short close failed: " + str(c.get("ErrMsg"))
            return
        out_px, fees = c["ClosePrice"], fee_in + c.get("CloseFee", 0)
        usd_in = q * in_px
        net = (in_px - out_px) * q - fees
        rec.update(usd=usd_in, in_px=in_px, out_px=out_px, out_role="SHORT", fees=fees, net=net, net_bp=net / usd_in * 1e4,
                   secs=time.time() - rec["t"], at_quote=at_quote)
        if at_quote:
            learn(sym, "D_DOWN", max(-30, min(30, rec["G"] - (in_px / out_px - 1) * 1e4)))
    except Exception as e:
        rec["err"] = repr(e)
        log("ERR", sym, repr(e))
    finally:
        finish(rec)


def check(sym):
    if not running[0] or inflight[0] or sym in busy or sym not in snap:
        return
    bb, ba = live[sym]
    sb, sa = snap[sym][:2]
    g_up, g_dn = (bb / sa - 1) * 1e4, (sb / ba - 1) * 1e4
    if g_up < PRE_G and g_dn < PRE_G:
        return
    S["checks"] += 1
    if not FEED.healthy():
        S["lagged"] += 1
        return
    left = slack_ms(sym)
    if left < 0:
        return
    spread = (ba / bb - 1) * 1e4
    if g_up >= PRE_G:
        ev, a, D = ev_up(g_up, spread, sym)
        d, G, ref = "UP", g_up, sa
    elif not ALLOW_SHORT:
        if ev_down(g_dn, spread, sym)[0] > BUFFER and time.time() - last_dn.get(sym, 0) > 1.0:
            last_dn[sym] = time.time()
            S["dn_skipped"] += 1
        return
    else:
        ev, a, D = ev_down(g_dn, spread, sym)
        d, G, ref = "DOWN", g_dn, sb
    if ev <= BUFFER:
        return

    # Technical Filter & Conviction Scoring Layer
    tech_ok, tech_mult, tech_reason, tech_meta = TECH.evaluate(sym, d, ref)
    if not tech_ok:
        S["tech_skipped"] += 1
        return

    with lock:
        if inflight[0] or sym in busy or not running[0]:
            return
        equity = S["equity"] + S["cum"]
        free = equity * 0.95 - sum(open_usd.values())
        base_size = BASE_FRAC + SLOPE_FRAC * ev
        target_size = equity * min(MAX_FRAC, base_size * tech_mult)
        usd = min(target_size, free)
        if usd < 200:
            S["no_cap"] += 1
            return
        busy.add(sym)
        inflight[0] = True
        open_usd[sym] = usd
        S["signals"] += 1

    rec = {
        "t": time.time(), "sym": sym, "dir": d, "G": G, "spread": spread, "ev": ev,
        "pm_or_p": a, "D_est": D, "slack": left, "size_usd": usd, "E": snap[sym][3],
        "lag": FEED.lag_ms(), "tech_tag": tech_reason, "tech_mult": tech_mult,
        "rsi": tech_meta.get("rsi_60"), "px_ema": tech_meta.get("px_vs_ema60"), "vol": tech_meta.get("vol_60")
    }
    if d == "UP":
        pool.submit(trade_up, rec, fq(sym, usd / sa), sa)
    else:
        pool.submit(trade_down, rec, usd, ref)


def on_book(sym, b, a):
    live[sym] = (b, a)
    check(sym)


def on_ticker(sym, b, a, E):
    mid = (b + a) / 2.0
    TECH.update(sym, mid)
    snap[sym] = (b, a, time.time(), E)


def unwind(label):
    signed("POST", "/v3/cancel_order", {})
    for p_pos in signed("GET", "/v6/short_positions").get("Positions") or []:
        r = signed("POST", "/v6/short_close", {"pair": p_pos["Pair"]})
        log(f"   {label}: close short {p_pos['Pair']} -> {r.get('ClosePrice') or r.get('ErrMsg')}")
    for c, v in (signed("GET", "/v3/balance").get("SpotWallet") or {}).items():
        sym = c + "USDT"
        if c != "USD" and pair_of(sym) in info and v["Free"] > 0 and fq(sym, v["Free"]) > 0 and v["Free"] * snap.get(sym, (0,))[0] > 2:
            od, err = place(sym, "SELL", fq(sym, v["Free"]))
            log(f"   {label}: sell {c} {fq(sym, v['Free'])} -> {od.get('Status') or err}")


def usd_total():
    for _ in range(5):
        w = signed("GET", "/v3/balance").get("SpotWallet")
        if w:
            u = w["USD"]
            return u["Free"] + u.get("Lock", 0)
        time.sleep(1)
    return float("nan")


def janitor():
    while True:
        time.sleep(30)
        if not running[0]:
            continue
        try:
            for c, v in (signed("GET", "/v3/balance").get("SpotWallet") or {}).items():
                sym = c + "USDT"
                if c == "USD" or sym not in snap or sym in busy:
                    continue
                usd = (v["Free"] + v["Lock"]) * snap[sym][0]
                if usd < 25:
                    continue
                with lock:
                    if sym in busy:
                        continue
                    busy.add(sym)
                try:
                    signed("POST", "/v3/cancel_order", {"pair": pair_of(sym)})
                    time.sleep(0.5)
                    od, err = place(sym, "SELL", fq(sym, v["Free"] + v["Lock"]))
                    log(f"   janitor: sold stray {c} ${usd:.0f} -> {od.get('Status') or err}")
                    S["jan"] += usd
                finally:
                    busy.discard(sym)
            for p_pos in signed("GET", "/v6/short_positions").get("Positions") or []:
                sym = p_pos["Pair"].split("/")[0] + "USDT"
                if sym in busy:
                    continue
                r = signed("POST", "/v6/short_close", {"pair": p_pos["Pair"]})
                log(f"   janitor: closed stray short {p_pos['Pair']} -> {r.get('ClosePrice') or r.get('ErrMsg')}")
        except Exception as e:
            log("   janitor error", repr(e))


def heartbeat(start_usd):
    global BOFF
    k = 0
    while True:
        time.sleep(60)
        k += 1
        acct = ""
        if k % 5 == 0 and not busy:
            u = usd_total()
            if not busy and u == u:
                acct = f"{u - start_usd:.2f}"
        if k % 10 == 0 and not busy and not inflight[0]:
            try:
                R.calibrate(8)
                BOFF = binance_offset()
                FEED.boff = BOFF
            except Exception as e:
                log("   recalibration failed", repr(e))
        p50_lag, max_lag = FEED.lag_stats_ms(5)
        with open(p(f"{TAG}_equity.csv"), "a") as f:
            f.write(f"{time.time():.0f},{S['cum']:.2f},{S['n']},{S['miss']},{sum(open_usd.values()):.0f},{p50_lag:.0f},{S['lagged']},{S['checks']},{acct}\n")
        log(f"-- {k} min: status=OK trades={S['n']} missed={S['miss']} cum=${S['cum']:+.2f} acct={acct or 'n/a'} "
            f"open=${sum(open_usd.values()):.0f} lag={p50_lag:.0f}ms (max {max_lag:.0f}ms) checks={S['checks']} tech_skips={S['tech_skipped']} owd={R.owd:.0f}ms")
        if k % 10 == 0:
            json.dump({"coin": coin, "glob": glob}, open(p(f"{TAG}_model.json"), "w"), indent=1)
            log(f"   [model update] pm={glob['pm']:.2f} D_up={glob['D_UP']:.1f} D_dn={glob['D_DOWN']:.1f} p={glob['p']:.2f}")


def main():
    global R, FEED, BOFF, info
    sys.stdout = Tee(p(f"{TAG}_stdout.log"), sys.stdout)
    sys.stderr = Tee(p(f"{TAG}_stdout.log"), sys.stderr)

    R = Roostoo(8, "SIN")
    R.calibrate()
    R.keepalive(1.0)
    BOFF = binance_offset()
    info = R.call("GET", "/v3/exchangeInfo", signed=False)["TradePairs"]
    syms = [p_pair.split("/")[0] + "USDT" for p_pair in R.call("GET", "/v3/ticker", {}, signed=False)["Data"]]

    # Load learned cost model or initialize with default priors
    model_file = p(f"{TAG}_model.json")
    if os.path.exists(model_file):
        try:
            m = json.load(open(model_file))
            coin.update(m.get("coin", {}))
            glob.update(m.get("glob", {}))
        except Exception as e:
            log("warning: failed loading model file, falling back to default priors:", repr(e))
    else:
        m = json.loads(DEFAULT_MODEL_JSON)
        coin.update(m["coin"])
        glob.update(m["glob"])

    FEED = Feed(syms, copies=1, boff=BOFF)
    FEED.start(on_book, on_ticker)
    time.sleep(8)
    unwind("startup")
    time.sleep(1)
    start_usd = usd_total()
    S["equity"] = start_usd
    if not os.path.exists(p(f"{TAG}_equity.csv")):
        open(p(f"{TAG}_equity.csv"), "w").write("ts,cum_net,trades,missed,open_usd,feed_lag_ms,lag_skips,checks,acct_pnl\n")
    log(f"START usd={start_usd:.2f} dur={DUR:.0f}s loss_limit=${LOSS_LIMIT:.0f} buffer={BUFFER}bp max_frac={MAX_FRAC} outbound~{R.owd:.0f}ms tickers={len(syms)}")
    running[0] = True
    threading.Thread(target=janitor, daemon=True).start()
    threading.Thread(target=heartbeat, args=(start_usd,), daemon=True).start()
    end = time.time() + DUR
    while time.time() < end and running[0]:
        time.sleep(1)
    running[0] = False
    t = time.time()
    while busy and time.time() - t < 60:
        time.sleep(0.5)
    time.sleep(2)
    unwind("shutdown")
    time.sleep(1)
    end_usd = usd_total()
    json.dump({"coin": coin, "glob": glob}, open(p(f"{TAG}_model.json"), "w"), indent=1)
    log(f"END usd={end_usd:.2f} account_pnl={end_usd - start_usd:+.2f} trip_pnl={S['cum']:+.2f} trades={S['n']} missed={S['miss']} "
        f"signals={S['signals']} lag_skips={S['lagged']}/{S['checks']} tech_skips={S['tech_skipped']} no_capital={S['no_cap']} reason={stop_reason[0] or 'duration'}")
    if stop_reason[0]:
        open(p(f"{TAG}_STOP"), "w").write(stop_reason[0])
    FEED.stop()
    os._exit(0)


if __name__ == "__main__":
    mp.freeze_support()
    main()
