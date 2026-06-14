"""
BTC Pulse Monitor v4 — 機関視点のトレンド判定 + 構造化フェーズログ + モメンタム
======================================================================

v3 をベースに以下2点を追加 (2026-06-14):
  改良1: フェーズの構造化ログ出力 (btc_phase_log.csv へ analyze_market 結果を1行追記)
         → 検証データの前向き蓄積。
  改良2: トレンドの「傾き」(モメンタム) 算出と表示
         → bull_score は遅行指標 (水準) のため、転換点で「bear継続(ショート有利)」と
           「bear終わりかけ=転換警戒(ショート危険)」を区別できない。
           phase_log.csv の履歴から bull_score の変化方向 (score_delta) を計算し、
           「bear緩和・転換警戒」を別軸として出す。

--- 以下 v3 のドキュメント (機能は完全保持) ---
BTC を中心に、価格・出来高・デリバティブ(FR/OI)・機関フロー(Coinbase Premium)
の4軸で総合判定。

通知タイプ:
  📅 朝/夜ブリーフ   : 毎日 9:00 JST / 21:00 JST に4軸の総まとめ
                     + クロス取引所比較 (Binance/Bybit/OKX の FR・OI スプレッド)
  🔀 トレンド転換    : EMA クロス + FR/CB 反転（FR 復活で 6 種に拡張）
  ⚡ BTC 急変       : 直近15分で BTC が ±2% 動いた時(出来高・FR含む)

データソース(全て無料):
  - Kraken (klines) ※Binance/Bybit/OKX/MEXC は GitHub Actions(米国IP) から 451/403 拒否
  - Coinbase Pro (現物価格 — Coinbase プレミアム計算)
  - **Coinalyze v1 (Funding Rate / Open Interest)** ※2026-05-15 復活
    - エンドポイント: /v1/funding-rate-history, /v1/open-interest-history
    - 認証: 環境変数 COINALYZE_API_KEY (api_key ヘッダ)
    - 無料プラン: 40 req/min(本BOTは15分に3req=月8,640req、十分余裕)
    - シンボル: BTCUSDT_PERP.A (Binance USDT-Margined Perp、最も流動性が高い)
    - 取得失敗時は従来通り neutral 扱いで継続（後方互換）

ネットワーク: http_get_json は 4回リトライ + 指数バックオフ。429/5xx をハンドル。

スケジュール: 15分ごと
通知先: DISCORD_WEBHOOK_TREND
"""
import os
import sys
import csv
import json
import time
import random
import urllib.request
import urllib.parse
import urllib.error
from datetime import datetime, timezone, timedelta
from pathlib import Path

# ============= 設定 =============
DISCORD_WEBHOOK = os.environ.get("DISCORD_WEBHOOK_TREND", "").strip()

# Coinalyze API (FR/OI 復活、2026-05-15)
COINALYZE_API_KEY = os.environ.get("COINALYZE_API_KEY", "").strip()
COINALYZE_BASE = "https://api.coinalyze.net/v1"
# Binance USDT-Margined BTC Perpetual (最も流動性が高い)。
# 取引所横断値が欲しい場合はカンマ区切りで複数指定可: "BTCUSDT_PERP.A,BTCUSDT_PERP.6"
COINALYZE_SYMBOL = os.environ.get("COINALYZE_BTC_SYMBOL", "BTCUSDT_PERP.A").strip()
# クロス取引所比較: Binance + Bybit + OKX の BTC USDT Perp
CROSS_EXCHANGE_SYMBOLS = [
    ('BTCUSDT_PERP.A', 'Binance'),
    ('BTCUSDT.6',      'Bybit'),
    ('BTCUSDT_PERP.3', 'OKX'),
]

# 急変アラート
SUDDEN_MOVE_THRESHOLD = 0.02
SUDDEN_MOVE_COOLDOWN_HOURS = 1

# ブリーフ (1日2回: 9:00 JST = 0 UTC, 21:00 JST = 12 UTC)
DAILY_BRIEF_UTC_HOURS = [0, 12]

# トレンド転換
TREND_COOLDOWN_HOURS = 6

# 新メトリクス閾値
FR_HOT_THRESHOLD = 0.0002        # +0.02%/8h 以上 = 過熱(年率約 22%超)
FR_COLD_THRESHOLD = -0.00005     # マイナス = ショート過剰 or 現物優勢
FR_CHANGE_THRESHOLD = 0.0002     # 24h で 0.02%以上変化したら通知
CB_PREMIUM_HOT = 0.0008          # +0.08% で米国買い顕著
CB_PREMIUM_COLD = -0.0005        # -0.05% で米国売り
CB_PREMIUM_FLIP_THRESHOLD = 0.0008  # 0%付近で反転(±0.08% またぎ)
OI_CHANGE_BIG = 0.05             # 5% OI変化 = 大きい
VOL_SURGE_RATIO = 1.5            # 24h出来高/7日平均 > 1.5 = 急増

STATE_FILE = Path(__file__).parent / "btc_pulse_state.json"
# 改良1: フェーズの構造化ログ (STATE_FILE と同じディレクトリ = btc_pulse.py 隣)
PHASE_LOG_FILE = STATE_FILE.parent / "btc_phase_log.csv"

# phase_log.csv のカラム順 (ヘッダ)
PHASE_LOG_COLUMNS = [
    'ts_utc', 'btc_price', 'bull_score', 'phase_label',
    'ret_7d', 'ret_30d',
    'ema20', 'ema50', 'ema200',
    'ema20_gt_ema50', 'ema50_gt_ema200', 'price_gt_ema200',
    'fr_current', 'fr_state', 'cb_premium', 'cb_state',
    'oi_change_24h', 'vol_ratio', 'drawdown_90d',
]


# ============= ヘルパー =============
def log(msg):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")
    print(f"[{ts}] {msg}", flush=True)


def now_utc():
    return datetime.now(timezone.utc)


def to_iso(dt):
    return dt.astimezone(timezone.utc).isoformat()


def from_iso(s):
    return datetime.fromisoformat(s).astimezone(timezone.utc)


def to_jst(dt):
    return dt.astimezone(timezone(timedelta(hours=9)))


def fmt_jst(dt):
    return to_jst(dt).strftime("%Y-%m-%d %H:%M JST")


def load_state():
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except Exception:
            pass
    return {
        "last_brief_key": None,        # "YYYY-MM-DD-HH" 形式で送信済みスロット記録
        "last_trend_state": None,
        "last_trend_alert": None,
        "last_sudden_move_alert": None,
        "last_fr_state": None,         # FR の状態(hot/cold/neutral)
        "last_premium_state": None,    # CB premium 状態
    }


def save_state(state):
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2))


# ============= 改良1: フェーズの構造化ログ =============
def _csv_num(x):
    """数値を csv 用に整形。None は空文字。"""
    if x is None:
        return ''
    return x


def append_phase_log(m, path=None):
    """
    analyze_market() の結果 m を btc_phase_log.csv に1行追記する。
    - path 省略時は本番デフォルト (PHASE_LOG_FILE = btc_pulse.py 隣)。
      テスト時は別パスを渡すことで本番ログを汚さない。
    - ファイルが無ければヘッダを書いてから追記。
    - 追記は 'a' モード + newline='' (Windows 改行二重化回避)。
    - 例外が出てもメイン処理を止めない (ログ警告のみ)。
    """
    log_path = Path(path) if path else PHASE_LOG_FILE
    try:
        ts = m['trend_state']
        row = {
            'ts_utc':          now_utc().isoformat(),
            'btc_price':       _csv_num(m.get('btc_price')),
            'bull_score':      _csv_num(m.get('bull_score')),
            'phase_label':     m['phase'][1] if m.get('phase') else '',
            'ret_7d':          _csv_num(m.get('btc_ret_7d')),
            'ret_30d':         _csv_num(m.get('btc_ret_30d')),
            'ema20':           _csv_num(m.get('ema20')),
            'ema50':           _csv_num(m.get('ema50')),
            'ema200':          _csv_num(m.get('ema200')),
            'ema20_gt_ema50':  bool(ts.get('ema20_above_ema50')) if ts else '',
            'ema50_gt_ema200': bool(ts.get('ema50_above_ema200')) if ts else '',
            'price_gt_ema200': bool(ts.get('price_above_ema200')) if ts else '',
            'fr_current':      _csv_num(m.get('fr_current')),
            'fr_state':        m.get('fr_state', ''),
            'cb_premium':      _csv_num(m.get('cb_premium')),
            'cb_state':        m.get('cb_state', ''),
            'oi_change_24h':   _csv_num(m.get('oi_change_24h')),
            'vol_ratio':       _csv_num(m.get('vol_ratio')),
            'drawdown_90d':    _csv_num(m.get('drawdown_90d')),
        }
        write_header = not log_path.exists()
        with open(log_path, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=PHASE_LOG_COLUMNS)
            if write_header:
                writer.writeheader()
            writer.writerow(row)
        log(f"  📝 phase_log 追記: {log_path.name} (score {row['bull_score']})")
    except Exception as e:
        log(f"  ⚠ phase_log 追記失敗(処理は継続): {e}")


# ============= 改良2: モメンタム算出 =============
def _read_phase_log_rows(path=None):
    """
    phase_log.csv を読んで dict 行のリストを返す (時系列順)。
    無い/空/ヘッダのみ/読み取り失敗 → [] (安全)。
    """
    log_path = Path(path) if path else PHASE_LOG_FILE
    if not log_path.exists():
        return []
    try:
        with open(log_path, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            return [r for r in reader]
    except Exception as e:
        log(f"  ⚠ phase_log 読み取り失敗: {e}")
        return []


def _to_float(x):
    try:
        if x is None or x == '':
            return None
        return float(x)
    except (TypeError, ValueError):
        return None


def _to_bool(x):
    """csv の 'True'/'False'/'' を bool or None に。"""
    if x is None or x == '':
        return None
    s = str(x).strip().lower()
    if s == 'true':
        return True
    if s == 'false':
        return False
    return None


def _find_score_at_hours_ago(rows, target_hours, now=None, tol_hours=4):
    """
    rows (時系列、各行に ts_utc, bull_score) から、
    now の約 target_hours 前 (±tol_hours で最も近い行) の bull_score を返す。
    該当なし → None。
    """
    if not rows:
        return None
    now = now or now_utc()
    target_ts = now - timedelta(hours=target_hours)
    best = None
    best_diff = None
    for r in rows:
        try:
            rts = from_iso(r['ts_utc'])
        except Exception:
            continue
        diff = abs((rts - target_ts).total_seconds()) / 3600.0
        if diff <= tol_hours and (best_diff is None or diff < best_diff):
            score = _to_float(r.get('bull_score'))
            if score is not None:
                best = score
                best_diff = diff
    return best


def compute_momentum(m, state, path=None):
    """
    改良2: トレンドの「傾き」(モメンタム) を算出。
    phase_log.csv の履歴から、現在 bull_score と約24h前/72h前の bull_score の差分を取り、
    変化の向きでラベルを付ける。履歴ゼロ/不足でも KeyError/IndexError で絶対に落ちない。

    返り値 dict: {score_delta_24h, score_delta_72h, ema20_cross_up, momentum_label}
    """
    result = {
        'score_delta_24h': None,
        'score_delta_72h': None,
        'ema20_cross_up': None,
        'momentum_label': '→ 継続/横ばい (履歴不足)',
    }
    try:
        cur_score = m.get('bull_score')
        rows = _read_phase_log_rows(path)
        now = now_utc()

        score_24h_ago = _find_score_at_hours_ago(rows, 24, now=now, tol_hours=4)
        score_72h_ago = _find_score_at_hours_ago(rows, 72, now=now, tol_hours=4)

        if cur_score is not None and score_24h_ago is not None:
            result['score_delta_24h'] = cur_score - score_24h_ago
        if cur_score is not None and score_72h_ago is not None:
            result['score_delta_72h'] = cur_score - score_72h_ago

        # ema20_cross_up: 直近ログ行で ema20_gt_ema50 が False→True に転じたか
        # (前回ログ行と現在の m を比較)
        if rows:
            prev_cross = _to_bool(rows[-1].get('ema20_gt_ema50'))
            cur_cross = None
            ts = m.get('trend_state') or {}
            if ts:
                cur_cross = bool(ts.get('ema20_above_ema50'))
            if prev_cross is not None and cur_cross is not None:
                result['ema20_cross_up'] = (prev_cross is False and cur_cross is True)

        # fr_easing: fr_state が cold→neutral へ戻った or neutral からの変化
        # (state['last_fr_state'] 活用、無ければ None)
        fr_easing = None
        last_fr = state.get('last_fr_state') if isinstance(state, dict) else None
        cur_fr = m.get('fr_state')
        if last_fr is not None and cur_fr is not None:
            if last_fr == 'cold' and cur_fr == 'neutral':
                fr_easing = True
            elif last_fr == 'neutral' and cur_fr != 'neutral':
                fr_easing = True
            else:
                fr_easing = False
        result['fr_easing'] = fr_easing

        # === momentum_label 判定 (優先順) ===
        d24 = result['score_delta_24h']
        has_history = d24 is not None
        suffix = '' if has_history else ' (履歴不足)'

        if has_history and cur_score is not None and cur_score <= -2 and d24 >= 3:
            label = '🔄 bear緩和・転換警戒（ショート利食い/新規ショート慎重）'
        elif has_history and cur_score is not None and cur_score >= 2 and d24 <= -3:
            label = '🔄 bull息切れ（弱含み）'
        elif has_history and d24 >= 2:
            label = '↗ 改善方向'
        elif has_history and d24 <= -2:
            label = '↘ 悪化方向'
        else:
            label = '→ 継続/横ばい' + suffix
        result['momentum_label'] = label
    except Exception as e:
        # 想定外でも絶対に落とさない
        log(f"  ⚠ compute_momentum 例外(neutral返却): {e}")
    return result


# ============= API 取得 =============
def http_get_json(url, timeout=20, headers=None, max_retries=4):
    """
    HTTP GET → JSON。指数バックオフ付き 4回リトライ。
    - 429 (rate limit) / 5xx をリトライ
    - ネットワーク例外 (URLError, TimeoutError, JSONDecodeError) もリトライ
    - リトライ間隔: 2^attempt + ジッタ (1, 2, 4, 8秒ベース)
    monitor.py の cg_get() 相当の堅牢性を btc_pulse にも付与（2026-05-15）。
    """
    hdrs = {'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json'}
    if headers:
        hdrs.update(headers)

    last_err = None
    for attempt in range(max_retries):
        try:
            req = urllib.request.Request(url, headers=hdrs)
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            last_err = e
            # 429 (rate limit) / 5xx はリトライ。それ以外は即 raise
            if e.code == 429 or e.code >= 500:
                if attempt < max_retries - 1:
                    wait = (2 ** attempt) + random.uniform(0, 0.5)
                    log(f"  HTTP {e.code} → {wait:.1f}秒後リトライ ({attempt+1}/{max_retries})")
                    time.sleep(wait)
                    continue
            raise
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as e:
            last_err = e
            if attempt < max_retries - 1:
                wait = (2 ** attempt) + random.uniform(0, 0.5)
                log(f"  {type(e).__name__} → {wait:.1f}秒後リトライ ({attempt+1}/{max_retries})")
                time.sleep(wait)
                continue
            raise
    if last_err:
        raise last_err
    raise Exception("http_get_json: 不明なエラー")


def coinalyze_get(path, params=None):
    """
    Coinalyze v1 API GET。
    - 認証: api_key ヘッダ (環境変数 COINALYZE_API_KEY)
    - レスポンスはルートが list の場合が多い (history 系)
    - エラー時は {"message": "..."} の dict が返るので例外化
    """
    if not COINALYZE_API_KEY:
        raise Exception("COINALYZE_API_KEY 未設定")
    qs = ''
    if params:
        qs = '?' + urllib.parse.urlencode(params)
    url = f"{COINALYZE_BASE}/{path}{qs}"
    headers = {'api_key': COINALYZE_API_KEY, 'accept': 'application/json'}
    data = http_get_json(url, headers=headers)
    # エラーレスポンスは {"message": "..."} 形式
    if isinstance(data, dict) and 'message' in data and 'history' not in data:
        raise Exception(f"Coinalyze {path} error: {data['message']}")
    return data


def fetch_klines(symbol, interval, limit=200, source='spot'):
    """Kraken OHLC (米国IP対応) / Binance・Bybit は GHA から HTTP 451/403 で拒否されるため Kraken に再移行(2026-05-12)."""
    sym_map = {"BTCUSDT": "XBTUSDT", "ETHUSDT": "ETHUSDT"}
    pair = sym_map.get(symbol, symbol)
    iv_map = {"1d": 1440, "12h": 720, "4h": 240, "1h": 60, "30m": 30, "15m": 15, "5m": 5, "1m": 1}
    iv = iv_map.get(interval, 60)
    url = f"https://api.kraken.com/0/public/OHLC?pair={pair}&interval={iv}"
    data = http_get_json(url)
    if data.get("error"):
        raise Exception(f"Kraken error: {data['error']}")
    result = data.get("result", {})
    keys = [k for k in result if k != "last"]
    if not keys:
        raise Exception("Kraken: no result key")
    rows = result[keys[0]]
    # Kraken形式: [time(sec), open, high, low, close, vwap, volume, count]
    # → Binance互換: [open_time(ms), open, high, low, close, volume]
    klines = [[int(r[0])*1000, r[1], r[2], r[3], r[4], r[6]] for r in rows]
    return klines[-limit:] if limit else klines


def _coinalyze_history_to_rows(resp):
    """
    Coinalyze の履歴レスポンス共通パーサ。
    レスポンス形: [{"symbol": "...", "history": [{"t": <sec>, "o": ..., "h": ..., "l": ..., "c": ...}, ...]}]
    複数シンボルが返った場合は最初の history を採用。
    """
    if not isinstance(resp, list) or not resp:
        return []
    first = resp[0]
    if not isinstance(first, dict):
        return []
    return first.get('history') or []


def fetch_funding_rate(symbol='BTCUSDT', limit=60):
    """
    Coinalyze v1: Funding Rate 履歴 (4時間足、OHLC)。
    Endpoint: /v1/funding-rate-history
    Note: Coinalyze は '8hour' を非サポート (1min/5min/15min/30min/1hour/2hour/4hour/6hour/12hour/daily のみ)。
    そのため 4hour で取得し、analyze_market 側で「2サイクル=8h」相当として扱う:
      - 24h前 = 6 cycles ago (index -7)
      - 7日 = 42 cycles (index -42:)
    OHLC の close (c) を採用 → 旧 Binance Futures fundingRate 形式に整形して返す。
    キー未設定 / API エラー時は [] (analyze_market で fr_state='neutral' として吸収)。
    """
    if not COINALYZE_API_KEY:
        return []
    try:
        now = int(datetime.now(timezone.utc).timestamp())
        # 4h * limit + バッファで遡る
        frm = now - (4 * 3600 * (limit + 2))
        data = coinalyze_get('funding-rate-history', {
            'symbols': COINALYZE_SYMBOL,
            'interval': '4hour',
            'from': frm,
            'to': now,
        })
        rows = _coinalyze_history_to_rows(data)
        if not rows:
            return []
        out = []
        for row in rows[-limit:]:
            if not isinstance(row, dict):
                continue
            close = row.get('c')
            if close is None:
                continue
            out.append({
                'fundingRate': close,            # 旧コードが float(x['fundingRate']) で読む
                'fundingTime': (row.get('t') or 0) * 1000,  # ms に変換 (旧Binance互換)
            })
        return out
    except Exception as e:
        log(f"  ⚠ Coinalyze FR 取得失敗: {e}")
        return []


def fetch_oi_history(symbol='BTCUSDT', period='1h', limit=48):
    """
    Coinalyze v1: Open Interest 履歴 (USD 換算)。
    Endpoint: /v1/open-interest-history
    OHLC の close (c) を採用 → 旧形式 [{'sumOpenInterest': ..., 'timestamp': ...}, ...] に整形。
    キー未設定 / API エラー時は []。
    """
    if not COINALYZE_API_KEY:
        return []
    try:
        # interval 変換: '1h' → '1hour' / '5m' → '5min' など
        iv_map = {'1m':'1min','5m':'5min','15m':'15min','30m':'30min',
                  '1h':'1hour','2h':'2hour','4h':'4hour','12h':'12hour','1d':'daily'}
        iv = iv_map.get(period, '1hour')
        # 分換算で from を計算
        minutes = {'1min':1,'5min':5,'15min':15,'30min':30,
                   '1hour':60,'2hour':120,'4hour':240,'12hour':720,'daily':1440}.get(iv, 60)
        now = int(datetime.now(timezone.utc).timestamp())
        frm = now - (minutes * 60 * (limit + 2))
        data = coinalyze_get('open-interest-history', {
            'symbols': COINALYZE_SYMBOL,
            'interval': iv,
            'from': frm,
            'to': now,
            'convert_to_usd': 'true',
        })
        rows = _coinalyze_history_to_rows(data)
        if not rows:
            return []
        out = []
        for row in rows[-limit:]:
            if not isinstance(row, dict):
                continue
            close = row.get('c')
            if close is None:
                continue
            out.append({
                'sumOpenInterest': close,        # USD 建て
                'timestamp': (row.get('t') or 0) * 1000,
            })
        return out
    except Exception as e:
        log(f"  ⚠ Coinalyze OI 履歴取得失敗: {e}")
        return []


def fetch_oi_now(symbol='BTCUSDT'):
    """
    現在 OI。OI 履歴の最新足を流用（追加API呼び出しを節約）。
    返り値: {'openInterest': <USD>} or None
    ※ Coinalyze は convert_to_usd=true で USD 建てを返すため、
       analyze_market 側の表示は `oi_current` をそのまま USD 表記に。
    """
    hist = fetch_oi_history(symbol, period='1h', limit=2)
    if not hist:
        return None
    try:
        latest = float(hist[-1]['sumOpenInterest'])
        return {'openInterest': latest, '_unit': 'USD'}
    except Exception:
        return None


def fetch_cross_exchange_snapshot():
    """
    Coinalyze v1: 複数取引所の現在 FR と OI を一括取得して比較データを生成。
    Endpoints: /v1/funding-rate, /v1/open-interest (convert_to_usd=true)
    Returns: [{'exchange': 'Binance', 'symbol': '...', 'fr': float, 'oi': float}, ...]
    キー未設定 / API エラー時は []。
    """
    if not COINALYZE_API_KEY:
        return []
    try:
        symbols_csv = ','.join(s for s, _ in CROSS_EXCHANGE_SYMBOLS)
        # 現在 FR
        fr_resp = coinalyze_get('funding-rate', {'symbols': symbols_csv})
        # 現在 OI (USD)
        oi_resp = coinalyze_get('open-interest', {'symbols': symbols_csv, 'convert_to_usd': 'true'})
        # symbol → value マッピング
        fr_map = {item['symbol']: item.get('value') for item in (fr_resp or []) if isinstance(item, dict)}
        oi_map = {item['symbol']: item.get('value') for item in (oi_resp or []) if isinstance(item, dict)}
        # 取引所ごとに整理 (元の順番を維持)
        result = []
        for sym, label in CROSS_EXCHANGE_SYMBOLS:
            result.append({
                'exchange': label,
                'symbol': sym,
                'fr': fr_map.get(sym),
                'oi': oi_map.get(sym),
            })
        return result
    except Exception as e:
        log(f"  ⚠ Coinalyze クロス取引所取得失敗: {e}")
        return []


def fetch_coinbase_btc():
    """Coinbase Pro の BTC-USD 価格"""
    url = "https://api.exchange.coinbase.com/products/BTC-USD/ticker"
    return http_get_json(url)


def safe(func, *args, **kwargs):
    """エラーが起きても None を返す安全ラッパ"""
    try:
        return func(*args, **kwargs)
    except Exception as e:
        log(f"  ⚠ {func.__name__} 失敗: {e}")
        return None


def ema(values, period):
    if len(values) < period:
        return None
    k = 2 / (period + 1)
    result = sum(values[:period]) / period
    for v in values[period:]:
        result = v * k + result * (1 - k)
    return result


def atr(klines, period=14):
    trs = []
    for i in range(1, len(klines)):
        h = float(klines[i][2])
        l = float(klines[i][3])
        pc = float(klines[i-1][4])
        trs.append(max(h-l, abs(h-pc), abs(l-pc)))
    if len(trs) < period:
        return None
    return sum(trs[-period:]) / period


# ============= 包括的市場分析 =============
def analyze_market():
    """4軸メトリクスを統合計算"""
    log("市場データ取得中...")

    btc_d = safe(fetch_klines, 'BTCUSDT', '1d', 250) or []
    btc_h = safe(fetch_klines, 'BTCUSDT', '1h', 200) or []
    btc_15m = safe(fetch_klines, 'BTCUSDT', '15m', 6) or []
    eth_h = safe(fetch_klines, 'ETHUSDT', '1h', 200) or []
    eth_d = safe(fetch_klines, 'ETHUSDT', '1d', 50) or []

    if not btc_d or not btc_h:
        raise Exception("BTC データ取得失敗")

    fr_history = safe(fetch_funding_rate, 'BTCUSDT', 60) or []
    oi_now_data = safe(fetch_oi_now, 'BTCUSDT')
    oi_hist = safe(fetch_oi_history, 'BTCUSDT', '1h', 48) or []
    cb_data = safe(fetch_coinbase_btc)
    cross_ex = safe(fetch_cross_exchange_snapshot) or []

    # === Price ===
    btc_d_closes = [float(k[4]) for k in btc_d]
    btc_h_closes = [float(k[4]) for k in btc_h]
    btc_h_volumes = [float(k[5]) for k in btc_h]
    eth_h_closes = [float(k[4]) for k in eth_h] if eth_h else []
    eth_d_closes = [float(k[4]) for k in eth_d] if eth_d else []

    btc_price = btc_h_closes[-1]
    eth_price = eth_h_closes[-1] if eth_h_closes else None

    # リターン
    btc_ret_24h = btc_price / btc_h_closes[-25] - 1 if len(btc_h_closes) >= 25 else None
    btc_ret_7d  = btc_price / btc_h_closes[-min(168, len(btc_h_closes)-1)] - 1 if len(btc_h_closes) > 24 else None
    btc_ret_30d = btc_price / btc_d_closes[-31] - 1 if len(btc_d_closes) >= 31 else None
    eth_ret_24h = eth_price / eth_h_closes[-25] - 1 if eth_h_closes and len(eth_h_closes) >= 25 else None
    eth_ret_30d = eth_price / eth_d_closes[-31] - 1 if eth_d_closes and len(eth_d_closes) >= 31 else None

    # EMA
    ema20 = ema(btc_d_closes[-50:], 20)
    ema50 = ema(btc_d_closes[-100:], 50)
    ema200 = ema(btc_d_closes, 200) if len(btc_d_closes) >= 200 else None
    atr14 = atr(btc_d, 14)

    # 90日 高値からの距離
    drawdown_90d = btc_price / max(btc_d_closes[-90:]) - 1 if len(btc_d_closes) >= 90 else None

    # 15分急変
    btc_15m_ago = float(btc_15m[-2][4]) if len(btc_15m) >= 2 else btc_price
    sudden_move_15m = btc_price / btc_15m_ago - 1

    # === 出来高分析 ===
    vol_24h = sum(btc_h_volumes[-24:]) if len(btc_h_volumes) >= 24 else None
    vol_7d_avg_24h = sum(btc_h_volumes[-168:]) / 7 if len(btc_h_volumes) >= 168 else None
    vol_ratio = vol_24h / vol_7d_avg_24h if (vol_24h and vol_7d_avg_24h and vol_7d_avg_24h > 0) else None

    # === Funding Rate (4hour cycles via Coinalyze) ===
    fr_current = fr_24h_ago = fr_7d_avg = fr_change_24h = None
    fr_state = 'neutral'
    if fr_history and len(fr_history) >= 42:
        try:
            fr_current = float(fr_history[-1]['fundingRate'])
            fr_24h_ago = float(fr_history[-7]['fundingRate'])  # 6 cycles ago = 24h (4h * 6)
            last_42 = [float(f['fundingRate']) for f in fr_history[-42:]]  # 42 * 4h = 168h = 7d
            fr_7d_avg = sum(last_42) / len(last_42)
            fr_change_24h = fr_current - fr_24h_ago
            if fr_current >= FR_HOT_THRESHOLD:
                fr_state = 'hot'      # ロング過熱
            elif fr_current <= FR_COLD_THRESHOLD:
                fr_state = 'cold'     # ショート過剰 or 現物優勢
            else:
                fr_state = 'neutral'
        except (TypeError, ValueError, KeyError, IndexError) as e:
            log(f"  ⚠ FR パース失敗: {e}")

    # === Coinbase Premium ===
    cb_price = None
    cb_premium = None
    cb_state = 'neutral'
    if cb_data and 'price' in cb_data:
        cb_price = float(cb_data['price'])
        cb_premium = (cb_price - btc_price) / btc_price
        if cb_premium >= CB_PREMIUM_HOT:
            cb_state = 'hot'    # 米国機関買い
        elif cb_premium <= CB_PREMIUM_COLD:
            cb_state = 'cold'   # 米国機関売り
        else:
            cb_state = 'neutral'

    # === Open Interest ===
    # Coinalyze は convert_to_usd=true で USD 建てを返す。oi_current は USD ($)
    oi_current = None
    oi_24h_ago = None
    oi_change_24h = None
    if oi_now_data and 'openInterest' in oi_now_data:
        try:
            oi_current = float(oi_now_data['openInterest'])
        except (TypeError, ValueError):
            pass
    if oi_hist and len(oi_hist) >= 25:
        try:
            oi_24h_ago = float(oi_hist[-25]['sumOpenInterest'])
            if oi_current and oi_24h_ago:
                oi_change_24h = oi_current / oi_24h_ago - 1
        except (TypeError, ValueError, KeyError, IndexError) as e:
            log(f"  ⚠ OI 履歴パース失敗: {e}")

    # === ETH先行 ===
    eth_lead_24h = (eth_ret_24h - btc_ret_24h) if (eth_ret_24h is not None and btc_ret_24h is not None) else None
    eth_btc_ratio = (eth_price / btc_price) if (eth_price and btc_price) else None

    # === クロス取引所統計 ===
    cross_summary = None
    if cross_ex:
        frs = [(x['exchange'], x['fr']) for x in cross_ex if x.get('fr') is not None]
        ois = [(x['exchange'], x['oi']) for x in cross_ex if x.get('oi') is not None]
        if frs:
            max_fr = max(frs, key=lambda v: v[1])
            min_fr = min(frs, key=lambda v: v[1])
            fr_spread = max_fr[1] - min_fr[1]
        else:
            max_fr = min_fr = None; fr_spread = None
        if ois:
            total_oi = sum(v for _, v in ois)
            top_oi = max(ois, key=lambda v: v[1])
        else:
            total_oi = None; top_oi = None
        cross_summary = {
            'rows': cross_ex,        # [{exchange, symbol, fr, oi}]
            'max_fr': max_fr,        # ('Exchange', value)
            'min_fr': min_fr,
            'fr_spread': fr_spread,  # FR の取引所間スプレッド (Bybit -0.56% と OKX +0.25% なら 0.81%)
            'top_oi': top_oi,
            'total_oi': total_oi,
        }

    # === 総合スコア(機関視点) ===
    bull_score = 0
    score_reasons = []

    # 価格 EMA 系(最大 +3 / -3)
    if ema20 and ema50:
        if ema20 > ema50: bull_score += 1; score_reasons.append('EMA20>EMA50')
        else: bull_score -= 1; score_reasons.append('EMA20<EMA50')
    if ema50 and ema200:
        if ema50 > ema200: bull_score += 1; score_reasons.append('EMA50>EMA200(ゴールデン)')
        else: bull_score -= 1; score_reasons.append('EMA50<EMA200(デッド)')
    if ema200 and btc_price:
        if btc_price > ema200: bull_score += 1; score_reasons.append('価格>EMA200')
        else: bull_score -= 1; score_reasons.append('価格<EMA200')

    # 30日リターン(最大 +2 / -2)
    if btc_ret_30d is not None:
        if btc_ret_30d > 0.20: bull_score += 2; score_reasons.append(f'30日 +{btc_ret_30d*100:.0f}%(強気)')
        elif btc_ret_30d > 0.05: bull_score += 1; score_reasons.append(f'30日 +{btc_ret_30d*100:.0f}%')
        elif btc_ret_30d < -0.20: bull_score -= 2; score_reasons.append(f'30日 {btc_ret_30d*100:.0f}%(弱気)')
        elif btc_ret_30d < -0.05: bull_score -= 1; score_reasons.append(f'30日 {btc_ret_30d*100:.0f}%')

    # FR(機関視点で重要)— マイナスはむしろブル候補
    if fr_state == 'hot':
        bull_score -= 2  # ロング過熱は天井近い
        score_reasons.append('FR過熱(天井候補)')
    elif fr_state == 'cold':
        bull_score += 2  # FRマイナス = 現物優勢、ブル転換候補
        score_reasons.append('FRマイナス(現物優勢、ブル候補)')

    # Coinbase Premium(機関フロー)
    if cb_state == 'hot':
        bull_score += 2
        score_reasons.append(f'CBプレミアム +{cb_premium*100:.2f}%(米国買い)')
    elif cb_state == 'cold':
        bull_score -= 2
        score_reasons.append(f'CB ディスカウント {cb_premium*100:.2f}%(米国売り)')

    # OI(価格との組み合わせで意味が変わる)
    if oi_change_24h is not None and btc_ret_24h is not None:
        if oi_change_24h > OI_CHANGE_BIG and btc_ret_24h > 0:
            bull_score += 1
            score_reasons.append(f'OI +{oi_change_24h*100:.1f}% × 価格上昇(健全)')
        elif oi_change_24h > OI_CHANGE_BIG and btc_ret_24h < 0:
            bull_score -= 2
            score_reasons.append(f'OI +{oi_change_24h*100:.1f}% × 価格下落(ショート流入、弱気)')
        elif oi_change_24h < -OI_CHANGE_BIG and btc_ret_24h > 0:
            bull_score += 0
            score_reasons.append(f'OI {oi_change_24h*100:.1f}% × 価格上昇(踏み上げ、不安定)')

    # 出来高
    if vol_ratio is not None and vol_ratio > VOL_SURGE_RATIO:
        score_reasons.append(f'出来高 急増 ({vol_ratio:.1f}x)')

    # フェーズ判定
    if bull_score >= 5:
        phase = ('🚀', '強気トレンド', 0x5cb85c, 'BTC は明確に上昇、機関も積極買い')
    elif bull_score >= 2:
        phase = ('📈', '弱強気', 0x86d981, '緩やかに上昇、ブル優勢')
    elif bull_score >= -1:
        phase = ('📊', 'レンジ相場', 0xf0b648, '横ばい、ETHスイング戦略の本命相場')
    elif bull_score >= -4:
        phase = ('📉', '弱弱気', 0xe06c6c, '緩やかに下降、ショート優位')
    else:
        phase = ('💀', '強弱気', 0xa02525, '明確な下落、ショート絶好調')

    return {
        # 価格
        'btc_price': btc_price, 'eth_price': eth_price,
        'btc_ret_24h': btc_ret_24h, 'btc_ret_7d': btc_ret_7d, 'btc_ret_30d': btc_ret_30d,
        'eth_ret_24h': eth_ret_24h, 'eth_ret_30d': eth_ret_30d,
        'eth_lead_24h': eth_lead_24h, 'eth_btc_ratio': eth_btc_ratio,
        'ema20': ema20, 'ema50': ema50, 'ema200': ema200,
        'atr14': atr14, 'drawdown_90d': drawdown_90d,
        'sudden_move_15m': sudden_move_15m,
        # 出来高
        'vol_24h': vol_24h, 'vol_7d_avg_24h': vol_7d_avg_24h, 'vol_ratio': vol_ratio,
        # FR
        'fr_current': fr_current, 'fr_24h_ago': fr_24h_ago,
        'fr_7d_avg': fr_7d_avg, 'fr_change_24h': fr_change_24h,
        'fr_state': fr_state,
        # CB premium
        'cb_price': cb_price, 'cb_premium': cb_premium, 'cb_state': cb_state,
        # OI (USD 建て — Coinalyze convert_to_usd)
        'oi_current': oi_current, 'oi_24h_ago': oi_24h_ago, 'oi_change_24h': oi_change_24h,
        # クロス取引所
        'cross_summary': cross_summary,
        # 総合
        'bull_score': bull_score, 'score_reasons': score_reasons,
        'phase': phase,
        # トレンド状態(離散値、転換検知用)
        'trend_state': {
            'ema20_above_ema50': bool(ema20 and ema50 and ema20 > ema50),
            'ema50_above_ema200': bool(ema50 and ema200 and ema50 > ema200),
            'price_above_ema200': bool(ema200 and btc_price > ema200),
            'fr_state': fr_state,
            'cb_state': cb_state,
        }
    }


# ============= フォーマッタ =============
def fmt_pct(x, signed=True, decimals=2):
    if x is None: return '—'
    s = f'{x*100:.{decimals}f}%'
    return ('+' + s) if (signed and x > 0) else s


def fmt_dollar(x, decimals=2):
    if x is None: return '—'
    return f'${x:,.{decimals}f}'


def fmt_dollar_short(x):
    """大きい数値を K/M/B 表記"""
    if x is None: return '—'
    if x >= 1e9: return f'${x/1e9:.2f}B'
    if x >= 1e6: return f'${x/1e6:.2f}M'
    if x >= 1e3: return f'${x/1e3:.2f}K'
    return f'${x:.2f}'


def fmt_fr(x):
    """ファンディングレートのフォーマット(% per 8h)"""
    if x is None: return '—'
    return f'{x*100:+.4f}%'


def fmt_score_delta(x):
    """score_delta を符号付き整数風で。None は '—'。"""
    if x is None:
        return '—'
    return f'{x:+g}'


# ============= Discord =============
def discord_notify(content, embeds=None):
    if not DISCORD_WEBHOOK:
        log("WARN: DISCORD_WEBHOOK_TREND 未設定")
        return False
    payload = {"content": content}
    if embeds:
        payload["embeds"] = embeds
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(DISCORD_WEBHOOK, data=body, headers={
        "Content-Type": "application/json",
        "User-Agent": "DiscordBot (https://github.com/kmd0704/altcoin-pump-monitor, btc-pulse-4.0)"
    })
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            log(f"Discord通知 HTTP {r.status}")
            return r.status in (200, 204)
    except Exception as e:
        log(f"Discord通知失敗: {e}")
        return False


# ============= Embed builders =============
def _build_cross_exchange_fields(cs):
    """クロス取引所比較セクションを生成 (build_daily_brief_embed のヘルパー)."""
    if not cs or not cs.get('rows'):
        return []
    # FR テーブル
    fr_lines = []
    for r in cs['rows']:
        fr = r.get('fr')
        fr_str = fmt_fr(fr) if fr is not None else '—'
        fr_lines.append(f"• **{r['exchange']:<8s}** {fr_str}")
    # OI テーブル + シェア
    oi_lines = []
    total = cs.get('total_oi') or 0
    for r in cs['rows']:
        oi = r.get('oi')
        if oi is None:
            oi_lines.append(f"• **{r['exchange']:<8s}** —")
            continue
        share = (oi / total * 100) if total else 0
        oi_lines.append(f"• **{r['exchange']:<8s}** {fmt_dollar_short(oi)} ({share:.0f}%)")
    # スプレッド情報
    spread_str = ''
    if cs.get('fr_spread') is not None and cs.get('max_fr') and cs.get('min_fr'):
        spread_str = (f"\n**FRスプレッド**: {cs['fr_spread']*100:+.4f}%\n"
                      f"(max: {cs['max_fr'][0]} {fmt_fr(cs['max_fr'][1])} / "
                      f"min: {cs['min_fr'][0]} {fmt_fr(cs['min_fr'][1])})")
    return [
        {"name": "🌐 クロス取引所 — Funding Rate", "value": '\n'.join(fr_lines) + spread_str, "inline": True},
        {"name": "🌐 クロス取引所 — Open Interest", "value": '\n'.join(oi_lines), "inline": True},
    ]


def _build_momentum_field(m):
    """
    改良2: 「📐 トレンドの傾き」フィールドを生成 (build_daily_brief_embed のヘルパー)。
    m['momentum'] 未格納でも落ちないようガード。
    """
    mom = m.get('momentum') or {}
    label = mom.get('momentum_label', '→ 継続/横ばい (履歴不足)')
    d24 = fmt_score_delta(mom.get('score_delta_24h'))
    d72 = fmt_score_delta(mom.get('score_delta_72h'))
    cross_up = mom.get('ema20_cross_up')
    cross_str = ('🟢 あり (EMA20>EMA50 へ転換)' if cross_up is True
                 else 'なし' if cross_up is False else '—')
    return {
        "name": "📐 トレンドの傾き (モメンタム)",
        "value": (
            f"**{label}**\n"
            f"score変化: 24h {d24} / 72h {d72} (現在 {m.get('bull_score', '—'):+})\n"
            f"EMA短期上抜け: {cross_str}\n"
            f"※ bull_scoreは水準(遅行)、傾きは向き。bear継続とbear終わりかけを区別。"
        ),
        "inline": False,
    }


def build_daily_brief_embed(m, brief_label='📅 ブリーフ'):
    emoji, phase_label, color, desc = m['phase']
    fr_emoji = '🔥' if m['fr_state'] == 'hot' else '❄️' if m['fr_state'] == 'cold' else '⚖️'
    cb_emoji = '🟢' if m['cb_state'] == 'hot' else '🔴' if m['cb_state'] == 'cold' else '⚖️'
    eth_swing_ok = m['bull_score'] <= 1
    new_long_ok = m['bull_score'] >= 2 and m['fr_state'] != 'hot'

    return {
        "title": f"{brief_label} — {emoji} {phase_label}",
        "description": f"**{desc}**\nスコア: **{m['bull_score']:+}**(+5以上=強気 / +2〜=弱強気 / -1〜+1=レンジ / -2〜=弱気)",
        "color": color,
        "fields": [
            # === 価格 ===
            {"name": "💰 BTC 価格", "value":
                f"**{fmt_dollar(m['btc_price'])}**\n"
                f"24h {fmt_pct(m['btc_ret_24h'])} / 7d {fmt_pct(m['btc_ret_7d'])} / 30d {fmt_pct(m['btc_ret_30d'])}",
             "inline": True},
            {"name": "Ξ ETH 価格", "value":
                f"**{fmt_dollar(m['eth_price'])}**\n"
                f"24h {fmt_pct(m['eth_ret_24h'])} / 30d {fmt_pct(m['eth_ret_30d'])}\n"
                f"ETH先行: {fmt_pct(m['eth_lead_24h'])}",
             "inline": True},
            # === トレンドの傾き (改良2) ===
            _build_momentum_field(m),
            # === 機関フロー ===
            {"name": f"{cb_emoji} Coinbase プレミアム", "value":
                f"**{fmt_pct(m['cb_premium'])}** ({m['cb_state']})\n"
                f"CB: {fmt_dollar(m['cb_price'])}\n"
                f"Kraken基準: {fmt_dollar(m['btc_price'])}\n"
                f"{'米国機関買い' if m['cb_state']=='hot' else '米国機関売り' if m['cb_state']=='cold' else '中立'}",
             "inline": False},
            # === デリバティブ ===
            {"name": f"{fr_emoji} ファンディングレート (Binance USDT-Perp / 8h)", "value":
                f"現在: **{fmt_fr(m['fr_current'])}** ({m['fr_state']})\n"
                f"24h前: {fmt_fr(m['fr_24h_ago'])} → {fmt_pct(m['fr_change_24h'], decimals=4) if m['fr_change_24h'] else '—'}\n"
                f"7日平均: {fmt_fr(m['fr_7d_avg'])}",
             "inline": True},
            {"name": "📊 Open Interest (USD換算)", "value":
                f"現在: {fmt_dollar_short(m['oi_current'])}\n"
                f"24h変化: {fmt_pct(m['oi_change_24h'])}",
             "inline": True},
            # === クロス取引所比較 ===
            *(_build_cross_exchange_fields(m['cross_summary']) if m.get('cross_summary') else []),
            # === 出来高 ===
            {"name": "📈 出来高 / ボラティリティ", "value":
                f"24h出来高: {fmt_dollar_short((m['vol_24h']*m['btc_price']) if m['vol_24h'] else None)}\n"
                f"7日平均比: {fmt_pct(m['vol_ratio']-1) if m['vol_ratio'] else '—'} ({m['vol_ratio']:.2f}x)\n"
                f"ATR(14): {fmt_dollar(m['atr14'])} ({fmt_pct(m['atr14']/m['btc_price'], False) if m['atr14'] else '—'})",
             "inline": False},
            # === EMA ===
            {"name": "📉 トレンド構造", "value":
                f"EMA20: {fmt_dollar(m['ema20'])} {'>' if m['trend_state']['ema20_above_ema50'] else '<'} EMA50: {fmt_dollar(m['ema50'])}\n"
                f"EMA50: {fmt_dollar(m['ema50'])} {'>' if m['trend_state']['ema50_above_ema200'] else '<'} EMA200: {fmt_dollar(m['ema200'])}\n"
                f"90日高値から: {fmt_pct(m['drawdown_90d'])}",
             "inline": False},
            # === スコア根拠 ===
            {"name": "🧮 スコア内訳",
             "value": '\n'.join(f'• {r}' for r in m['score_reasons'][:8]) if m['score_reasons'] else 'データ不足',
             "inline": False},
            # === アクション ===
            {"name": "💡 今日のアクション目安", "value":
                f"・**ETH swing**: {'🟢 推奨(レンジ・弱気)' if eth_swing_ok else '🔴 見送り(ブル相場)'}\n"
                f"・**altcoin pump short**: 🟢 通常運用OK\n"
                f"・**新規ロング**: {'🟢 検討OK(機関買い+FR冷却)' if new_long_ok else '🟡 慎重' if m['bull_score']>=0 else '🔴 待機'}",
             "inline": False},
        ],
        "footer": {"text": "次回ブリーフ: 明日 9:00 JST / FR/OI ソース: Coinalyze / 価格: Kraken+Coinbase"},
        "timestamp": now_utc().isoformat()
    }


def build_trend_change_embed(m, change_type, detail):
    icon_color = {
        'ema_cross_up':         ('🟢', 0x86d981),
        'ema_cross_down':       ('🔴', 0xe06c6c),
        'golden_cross':         ('✨', 0x5cb85c),
        'death_cross':          ('💀', 0xa02525),
        'price_above_ema200':   ('📈', 0x86d981),
        'price_below_ema200':   ('📉', 0xe06c6c),
        'fr_flip_to_cold':      ('❄️', 0x4fc3f7),
        'fr_flip_to_hot':       ('🔥', 0xe06c6c),
        'cb_flip_to_hot':       ('🟢', 0x5cb85c),
        'cb_flip_to_cold':      ('🔴', 0xe06c6c),
    }
    title_map = {
        'ema_cross_up':         'EMA20 が EMA50 を上抜け(短期上昇)',
        'ema_cross_down':       'EMA20 が EMA50 を下抜け(短期下降)',
        'golden_cross':         'ゴールデンクロス(EMA50 が EMA200 を上抜け)',
        'death_cross':          'デッドクロス(EMA50 が EMA200 を下抜け)',
        'price_above_ema200':   'BTC 価格が EMA200 を上抜け',
        'price_below_ema200':   'BTC 価格が EMA200 を下抜け',
        'fr_flip_to_cold':      '🌊 ファンディングがマイナス転換(現物優勢、ブル転換候補)',
        'fr_flip_to_hot':       '🔥 ファンディング過熱転換(ロング過剰、天井候補)',
        'cb_flip_to_hot':       '🟢 Coinbase プレミアム +(米国機関買い始動)',
        'cb_flip_to_cold':      '🔴 Coinbase ディスカウント転換(米国機関売り始動)',
    }
    emoji, color = icon_color.get(change_type, ('🔀', 0xf0b648))
    return {
        "title": f"{emoji} トレンド転換検知 — {title_map.get(change_type, change_type)}",
        "description": detail,
        "color": color,
        "fields": [
            {"name": "BTC 価格", "value": fmt_dollar(m['btc_price']), "inline": True},
            {"name": "30日リターン", "value": fmt_pct(m['btc_ret_30d']), "inline": True},
            {"name": "総合スコア", "value": f"**{m['bull_score']:+}** ({m['phase'][1]})", "inline": True},
            {"name": "ファンディング", "value": f"{fmt_fr(m['fr_current'])} ({m['fr_state']})", "inline": True},
            {"name": "CB プレミアム", "value": f"{fmt_pct(m['cb_premium'])} ({m['cb_state']})", "inline": True},
            {"name": "OI 24h変化", "value": fmt_pct(m['oi_change_24h']), "inline": True},
        ],
        "footer": {"text": "BTC Pulse v4 / トレンド転換アラート"},
        "timestamp": now_utc().isoformat()
    }


def build_sudden_move_embed(m):
    move = m['sudden_move_15m']
    is_up = move > 0
    mom = m.get('momentum') or {}
    momentum_label = mom.get('momentum_label', '→ 継続/横ばい (履歴不足)')
    return {
        "title": f"{'🚀' if is_up else '🔻'} BTC 急変 — 直近15分で {fmt_pct(move)}",
        "description": (
            f"**BTC が直近15分で {'急騰' if is_up else '急落'}しました。**\n\n"
            f"既存ポジション確認 + 新規エントリーは慎重に。"
        ),
        "color": 0x5cb85c if is_up else 0xe06c6c,
        "fields": [
            {"name": "BTC 価格", "value": fmt_dollar(m['btc_price']), "inline": True},
            {"name": "15分前比", "value": fmt_pct(move), "inline": True},
            {"name": "24h リターン", "value": fmt_pct(m['btc_ret_24h']), "inline": True},
            {"name": "ETH 連動", "value": f"{fmt_pct(m['eth_ret_24h'])} (先行 {fmt_pct(m['eth_lead_24h'])})", "inline": True},
            {"name": "ファンディング", "value": f"{fmt_fr(m['fr_current'])} ({m['fr_state']})", "inline": True},
            {"name": "出来高", "value": f"{m['vol_ratio']:.2f}x" if m['vol_ratio'] else '—', "inline": True},
            {"name": "💡 解釈ヒント",
             "value": (
                f"• トレンドの傾き: {momentum_label}\n"
                f"• 出来高 {'急増' if m['vol_ratio'] and m['vol_ratio'] > 1.5 else '通常'} = "
                f"{'本物の動き' if m['vol_ratio'] and m['vol_ratio'] > 1.5 else 'ノイズの可能性'}\n"
                f"• ファンディング {'過熱' if m['fr_state']=='hot' else '冷却中' if m['fr_state']=='cold' else '中立'} → "
                f"{'天井近い可能性' if (m['fr_state']=='hot' and is_up) else '転換候補' if (m['fr_state']=='cold' and is_up) else ''}"
             ), "inline": False},
        ],
        "footer": {"text": "BTC Pulse v4 / 急変アラート(クールダウン1h)"},
        "timestamp": now_utc().isoformat()
    }


# ============= 各チェック =============
def check_daily_brief(state, m):
    """9:00 JST (= 0:00 UTC) と 21:00 JST (= 12:00 UTC) の2回ブリーフ送信."""
    now = now_utc()
    if now.hour not in DAILY_BRIEF_UTC_HOURS:
        return False
    # slot キー: "YYYY-MM-DD-HH" (UTC時刻ベース)
    slot_key = now.strftime('%Y-%m-%d-%H')
    if state.get('last_brief_key') == slot_key:
        return False
    # 朝(9:00 JST)・夜(21:00 JST) を区別
    jst_hour = to_jst(now).hour
    label = '🌅 朝ブリーフ' if jst_hour == 9 else '🌃 夜ブリーフ'
    today_jst = to_jst(now).strftime('%Y-%m-%d')
    embed = build_daily_brief_embed(m, label)
    discord_notify(f"📅 **{today_jst} {label}** ({jst_hour:02d}:00 JST)", embeds=[embed])
    state['last_brief_key'] = slot_key
    log(f"📅 {label} 送信: {slot_key}")
    return True


def check_trend_change(state, m):
    cur = m['trend_state']
    prev = state.get('last_trend_state')
    if prev is None:
        state['last_trend_state'] = cur
        return False

    last_alert = state.get('last_trend_alert')
    if last_alert:
        elapsed = (now_utc() - from_iso(last_alert)).total_seconds() / 3600
        if elapsed < TREND_COOLDOWN_HOURS:
            state['last_trend_state'] = cur
            return False

    changes = []
    # EMA系
    if prev.get('ema50_above_ema200') != cur['ema50_above_ema200']:
        ct = 'golden_cross' if cur['ema50_above_ema200'] else 'death_cross'
        d = f"EMA50({fmt_dollar(m['ema50'])}) と EMA200({fmt_dollar(m['ema200'])}) のクロス。**長期トレンドの強い転換シグナル**。"
        changes.append((ct, d, 1))  # priority 1 (最重要)
    if prev.get('price_above_ema200') != cur['price_above_ema200']:
        ct = 'price_above_ema200' if cur['price_above_ema200'] else 'price_below_ema200'
        d = f"BTC 価格が EMA200({fmt_dollar(m['ema200'])}) を{'上' if cur['price_above_ema200'] else '下'}抜け。長期サポート/レジスタンスの突破。"
        changes.append((ct, d, 2))
    if prev.get('ema20_above_ema50') != cur['ema20_above_ema50']:
        ct = 'ema_cross_up' if cur['ema20_above_ema50'] else 'ema_cross_down'
        d = f"EMA20({fmt_dollar(m['ema20'])}) と EMA50({fmt_dollar(m['ema50'])}) が交差。短期トレンドの転換候補。"
        changes.append((ct, d, 3))
    # FR 状態変化
    if prev.get('fr_state') != cur['fr_state']:
        if cur['fr_state'] == 'cold':
            ct = 'fr_flip_to_cold'
            d = (f"ファンディング {fmt_fr(m['fr_current'])} に転換。**ショート過剰 or 現物優勢を示唆**。\n"
                 f"24h前: {fmt_fr(m['fr_24h_ago'])} → 今: {fmt_fr(m['fr_current'])}\n"
                 f"歴史的にここからの巻き戻し上昇が多い。")
            changes.append((ct, d, 1))
        elif cur['fr_state'] == 'hot':
            ct = 'fr_flip_to_hot'
            d = (f"ファンディング {fmt_fr(m['fr_current'])} に過熱。**ロング過剰、天井近い可能性**。\n"
                 f"24h前: {fmt_fr(m['fr_24h_ago'])} → 今: {fmt_fr(m['fr_current'])}\n"
                 f"清算スパイクに警戒。")
            changes.append((ct, d, 1))
    # CB プレミアム反転
    if prev.get('cb_state') != cur['cb_state']:
        if cur['cb_state'] == 'hot':
            ct = 'cb_flip_to_hot'
            d = (f"Coinbase プレミアム +{m['cb_premium']*100:.2f}% に転換。**米国機関の買いフロー始動**。\n"
                 f"CB: {fmt_dollar(m['cb_price'])} / Kraken基準: {fmt_dollar(m['btc_price'])}")
            changes.append((ct, d, 2))
        elif cur['cb_state'] == 'cold':
            ct = 'cb_flip_to_cold'
            d = (f"Coinbase ディスカウント {m['cb_premium']*100:.2f}% に転換。**米国機関の売りフロー始動**。\n"
                 f"CB: {fmt_dollar(m['cb_price'])} / Kraken基準: {fmt_dollar(m['btc_price'])}")
            changes.append((ct, d, 2))

    if not changes:
        state['last_trend_state'] = cur
        return False

    # 優先度順に1件だけ通知
    changes.sort(key=lambda x: x[2])
    ct, detail, _ = changes[0]
    embed = build_trend_change_embed(m, ct, detail)
    discord_notify(f"🔀 **BTC トレンド転換** — {ct}", embeds=[embed])
    state['last_trend_state'] = cur
    state['last_trend_alert'] = to_iso(now_utc())
    log(f"🔀 トレンド転換通知: {ct}")
    return True


def check_sudden_move(state, m):
    move = m['sudden_move_15m']
    if abs(move) < SUDDEN_MOVE_THRESHOLD:
        return False
    last_alert = state.get('last_sudden_move_alert')
    if last_alert:
        elapsed = (now_utc() - from_iso(last_alert)).total_seconds() / 3600
        if elapsed < SUDDEN_MOVE_COOLDOWN_HOURS:
            log(f"急変 {move*100:+.2f}% — クールダウン中")
            return False
    embed = build_sudden_move_embed(m)
    discord_notify(f"⚡ **BTC 急変** {fmt_pct(move)} — 直近15分", embeds=[embed])
    state['last_sudden_move_alert'] = to_iso(now_utc())
    log(f"⚡ 急変通知: {move*100:+.2f}%")
    return True


# ============= メイン =============
def main():
    log("=== BTC Pulse Monitor v4 起動(機関視点 / Coinalyze FR/OI + クロス取引所 + フェーズログ + モメンタム) ===")
    if not DISCORD_WEBHOOK:
        log("WARN: DISCORD_WEBHOOK_TREND 未設定")
    if not COINALYZE_API_KEY:
        log("WARN: COINALYZE_API_KEY 未設定 — FR/OI は無効化されます")
    else:
        log(f"✓ Coinalyze API 設定済み — FR/OI 取得を有効化 (symbol={COINALYZE_SYMBOL})")

    if os.environ.get("TEST_DISCORD_TREND", "").strip() == "1":
        log("🧪 TEST_DISCORD_TREND モード")
        coinalyze_status = "✅ 有効" if COINALYZE_API_KEY else "⚠️ 未設定（FR/OI は無効）"
        ok = discord_notify(
            "🧪 **BTC Pulse v4 接続テスト**\n"
            "GitHub Actions から正常に到達しました。\n\n"
            "📅 朝/夜ブリーフ (9:00 / 21:00 JST) — 4軸統合 + クロス取引所比較 + トレンドの傾き\n"
            "🔀 トレンド転換検知(EMA / FR / CBプレミアム — 最大6種)\n"
            "⚡ BTC 急変(±2% / 15分、出来高・FR込み)\n"
            "📝 フェーズ構造化ログ (btc_phase_log.csv)\n\n"
            f"データソース:\n"
            f"  • 価格(klines): Kraken\n"
            f"  • 機関フロー: Coinbase Pro\n"
            f"  • FR/OI/クロス取引所: Coinalyze ({coinalyze_status})\n"
            f"  • HTTP: 4回リトライ+指数バックオフ\n\n"
            "確認後、TEST_DISCORD_TREND を削除してください。"
        )
        log(f"テスト結果: {'✅ 成功' if ok else '❌ 失敗'}")
        return

    state = load_state()

    try:
        m = analyze_market()
    except Exception as e:
        log(f"市場分析エラー: {e}")
        sys.exit(1)

    # 改良2: モメンタム (傾き) を m に格納してから各 check を呼ぶ
    m['momentum'] = compute_momentum(m, state)

    log(f"  BTC: {fmt_dollar(m['btc_price'])} / フェーズ: {m['phase'][1]} (score {m['bull_score']:+})")
    log(f"  FR: {fmt_fr(m['fr_current'])} ({m['fr_state']}) / CB: {fmt_pct(m['cb_premium'])} ({m['cb_state']})")
    if m['vol_ratio']:
        log(f"  OI 24h変化: {fmt_pct(m['oi_change_24h'])} / 出来高: {m['vol_ratio']:.2f}x")
    else:
        log(f"  OI 24h変化: {fmt_pct(m['oi_change_24h'])} / 出来高: データ不足")
    if m.get('cross_summary') and m['cross_summary'].get('fr_spread') is not None:
        log(f"  クロス取引所 FR スプレッド: {m['cross_summary']['fr_spread']*100:+.4f}%")
    log(f"  15分変動: {m['sudden_move_15m']*100:+.2f}%")
    log(f"  📐 トレンドの傾き: {m['momentum']['momentum_label']} "
        f"(Δ24h {fmt_score_delta(m['momentum']['score_delta_24h'])} / "
        f"Δ72h {fmt_score_delta(m['momentum']['score_delta_72h'])})")

    sent_brief = check_daily_brief(state, m)
    sent_trend = check_trend_change(state, m)
    sent_sudden = check_sudden_move(state, m)

    # 改良1: フェーズ構造化ログを追記 (state 保存近辺)。
    # コミット頻度抑制(§5-7 abuse誤検知回避)のため、毎時1回=正時台(分<15)の実行のみ追記。
    # momentum の 24h/72h 前参照は1時間粒度で十分機能する。
    if now_utc().minute < 15:
        append_phase_log(m)
    # last_fr_state を更新 (compute_momentum の fr_easing 用、次回実行で参照)。
    state['last_fr_state'] = m.get('fr_state')
    state['last_premium_state'] = m.get('cb_state')
    save_state(state)

    summary = []
    if sent_brief: summary.append('📅ブリーフ')
    if sent_trend: summary.append('🔀トレンド転換')
    if sent_sudden: summary.append('⚡急変')
    log(f"=== 完了 / 送信: {' / '.join(summary) if summary else 'なし(平常)'} ===")


if __name__ == "__main__":
    main()
