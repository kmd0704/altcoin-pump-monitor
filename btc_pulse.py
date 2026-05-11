"""
BTC Pulse Monitor v2 — 機関視点のトレンド判定
==============================================

BTC を中心に、価格・出来高・デリバティブ(FR/OI)・機関フロー(Coinbase Premium)
の4軸で総合判定。

通知タイプ:
  📅 朝ブリーフ      : 毎日 9:00 JST(0:00 UTC)に4軸の総まとめ
  🔀 トレンド転換    : EMA クロス + FR 急変 + Coinbase プレミアム反転
  ⚡ BTC 急変       : 直近15分で BTC が ±2% 動いた時(出来高・FR含む)

データソース(全て無料):
  - Kraken (klines) ※Binance・Bybit は GitHub Actions(米国IP)から 451/403 拒否のため Kraken に再移行(2026-05-12)
  - FR / OI は米国IP対応の無料データソース不在のため一時無効化（analyze_market は None/neutral 扱いで吸収）
  - Coinbase Pro (現物価格 — Coinbaseプレミアム計算)

スケジュール: 15分ごと
通知先: DISCORD_WEBHOOK_TREND
"""
import os
import sys
import json
import time
import urllib.request
import urllib.parse
import urllib.error
from datetime import datetime, timezone, timedelta
from pathlib import Path

# ============= 設定 =============
DISCORD_WEBHOOK = os.environ.get("DISCORD_WEBHOOK_TREND", "").strip()

# 急変アラート
SUDDEN_MOVE_THRESHOLD = 0.02
SUDDEN_MOVE_COOLDOWN_HOURS = 1

# 朝ブリーフ
DAILY_BRIEF_UTC_HOUR = 0  # 0:00 UTC = 9:00 JST

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
        "daily_briefed_date": None,
        "last_trend_state": None,
        "last_trend_alert": None,
        "last_sudden_move_alert": None,
        "last_fr_state": None,         # FR の状態(hot/cold/neutral)
        "last_premium_state": None,    # CB premium 状態
    }


def save_state(state):
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2))


# ============= API 取得 =============
def http_get_json(url, timeout=20):
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


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


def fetch_funding_rate(symbol='BTCUSDT', limit=30):
    """FR取得は米国IPから無料アクセス可能なデータソース不在のため一時無効化(2026-05-12).
    analyze_market 側で fr_state='neutral' として扱われる。"""
    return []


def fetch_oi_now(symbol='BTCUSDT'):
    """OI取得は米国IP対応の無料データソース不在のため一時無効化(2026-05-12)."""
    return None


def fetch_oi_history(symbol='BTCUSDT', period='1h', limit=48):
    """OI履歴も一時無効化(2026-05-12)."""
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

    fr_history = safe(fetch_funding_rate, 'BTCUSDT', 30) or []
    oi_now_data = safe(fetch_oi_now, 'BTCUSDT')
    oi_hist = safe(fetch_oi_history, 'BTCUSDT', '1h', 48) or []
    cb_data = safe(fetch_coinbase_btc)

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

    # === Funding Rate ===
    fr_current = fr_24h_ago = fr_7d_avg = fr_change_24h = None
    fr_state = 'neutral'
    if fr_history and len(fr_history) >= 21:
        fr_current = float(fr_history[-1]['fundingRate'])
        fr_24h_ago = float(fr_history[-4]['fundingRate'])  # 3 cycles ago = 24h
        last_21 = [float(f['fundingRate']) for f in fr_history[-21:]]
        fr_7d_avg = sum(last_21) / len(last_21)
        fr_change_24h = fr_current - fr_24h_ago
        if fr_current >= FR_HOT_THRESHOLD:
            fr_state = 'hot'      # ロング過熱
        elif fr_current <= FR_COLD_THRESHOLD:
            fr_state = 'cold'     # ショート過剰 or 現物優勢
        else:
            fr_state = 'neutral'

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
    oi_current = None
    oi_24h_ago = None
    oi_change_24h = None
    if oi_now_data and 'openInterest' in oi_now_data:
        oi_current = float(oi_now_data['openInterest'])
    if oi_hist and len(oi_hist) >= 25:
        oi_24h_ago = float(oi_hist[-25]['sumOpenInterest'])
        if oi_current and oi_24h_ago:
            oi_change_24h = oi_current / oi_24h_ago - 1

    # === ETH先行 ===
    eth_lead_24h = (eth_ret_24h - btc_ret_24h) if (eth_ret_24h is not None and btc_ret_24h is not None) else None
    eth_btc_ratio = (eth_price / btc_price) if (eth_price and btc_price) else None

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
        phase = ('📈', '弑強気', 0x86d981, '緩やかに上昇、ブル優勢')
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
        # OI
        'oi_current': oi_current, 'oi_24h_ago': oi_24h_ago, 'oi_change_24h': oi_change_24h,
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
        "User-Agent": "DiscordBot (https://github.com/kmd0704/altcoin-pump-monitor, btc-pulse-2.0)"
    })
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            log(f"Discord通知 HTTP {r.status}")
            return r.status in (200, 204)
    except Exception as e:
        log(f"Discord通知失敗: {e}")
        return False


# ============= Embed builders =============
def build_daily_brief_embed(m):
    emoji, label, color, desc = m['phase']
    fr_emoji = '🔥' if m['fr_state'] == 'hot' else '❄️' if m['fr_state'] == 'cold' else '⚖️'
    cb_emoji = '🟢' if m['cb_state'] == 'hot' else '🔴' if m['cb_state'] == 'cold' else '⚖️'
    eth_swing_ok = m['bull_score'] <= 1
    new_long_ok = m['bull_score'] >= 2 and m['fr_state'] != 'hot'

    return {
        "title": f"📅 朝ブリーフ — {emoji} {label}",
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
            # === 機関フロー ===
            {"name": f"{cb_emoji} Coinbase プレミアム", "value":
                f"**{fmt_pct(m['cb_premium'])}** ({m['cb_state']})\n"
                f"CB: {fmt_dollar(m['cb_price'])}\n"
                f"Binance: {fmt_dollar(m['btc_price'])}\n"
                f"{'米国機関買い' if m['cb_state']=='hot' else '米国機関売り' if m['cb_state']=='cold' else '中立'}",
             "inline": False},
            # === デリバティブ ===
            {"name": f"{fr_emoji} ファンディングレート (8h単位)", "value":
                f"現在: **{fmt_fr(m['fr_current'])}** ({m['fr_state']})\n"
                f"24h前: {fmt_fr(m['fr_24h_ago'])} → {fmt_pct(m['fr_change_24h'], decimals=4) if m['fr_change_24h'] else '—'}\n"
                f"7日平均: {fmt_fr(m['fr_7d_avg'])}",
             "inline": True},
            {"name": "📊 Open Interest", "value":
                f"現在: {fmt_dollar_short(m['oi_current'] * m['btc_price']) if m['oi_current'] else '—'}\n"
                f"24h: {fmt_pct(m['oi_change_24h'])}\n"
                f"({fmt_dollar_short(m['oi_current']) + ' BTC' if m['oi_current'] else '—'})",
             "inline": True},
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
        "footer": {"text": "次回ブリーフ: 明日 9:00 JST / トレンド転換と急変は随時通知"},
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
        "footer": {"text": "BTC Pulse v2 / トレンド転換アラート"},
        "timestamp": now_utc().isoformat()
    }


def build_sudden_move_embed(m):
    move = m['sudden_move_15m']
    is_up = move > 0
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
                f"• 出来高 {'急増' if m['vol_ratio'] and m['vol_ratio'] > 1.5 else '通常'} = "
                f"{'本物の動き' if m['vol_ratio'] and m['vol_ratio'] > 1.5 else 'ノイズの可能性'}\n"
                f"• ファンディング {'過熱' if m['fr_state']=='hot' else '冷却中' if m['fr_state']=='cold' else '中立'} → "
                f"{'天井近い可能性' if (m['fr_state']=='hot' and is_up) else '転換候補' if (m['fr_state']=='cold' and is_up) else ''}"
             ), "inline": False},
        ],
        "footer": {"text": "BTC Pulse v2 / 急変アラート(クールダウン1h)"},
        "timestamp": now_utc().isoformat()
    }


# ============= 各チェック =============
def check_daily_brief(state, m):
    now = now_utc()
    today_jst = to_jst(now).strftime('%Y-%m-%d')
    if state.get('daily_briefed_date') == today_jst:
        return False
    if now.hour != DAILY_BRIEF_UTC_HOUR:
        return False
    embed = build_daily_brief_embed(m)
    discord_notify(f"📅 **{today_jst} 朝ブリーフ**", embeds=[embed])
    state['daily_briefed_date'] = today_jst
    log(f"📅 朝ブリーフ送信: {today_jst}")
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
                 f"CB: {fmt_dollar(m['cb_price'])} / Binance: {fmt_dollar(m['btc_price'])}")
            changes.append((ct, d, 2))
        elif cur['cb_state'] == 'cold':
            ct = 'cb_flip_to_cold'
            d = (f"Coinbase ディスカウント {m['cb_premium']*100:.2f}% に転換。**米国機関の売りフロー始動**。\n"
                 f"CB: {fmt_dollar(m['cb_price'])} / Binance: {fmt_dollar(m['btc_price'])}")
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
    log("=== BTC Pulse Monitor v2 起動(機関視点) ===")
    if not DISCORD_WEBHOOK:
        log("WARN: DISCORD_WEBHOOK_TREND 未設定")

    if os.environ.get("TEST_DISCORD_TREND", "").strip() == "1":
        log("🧪 TEST_DISCORD_TREND モード")
        ok = discord_notify(
            "🧪 **BTC Pulse v2 接続テスト**\n"
            "GitHub Actions から正常に到達しました。\n\n"
            "📅 朝ブリーフ(毎日 9:00 JST)— 4軸統合\n"
            "🔀 トレンド転換検知(EMA / FR / CBプレミアム)\n"
            "⚡ BTC 急変(±2% / 15分、出来高・FR込み)\n\n"
            "データソース: Binance (Spot/Futures) + Coinbase Pro\n"
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

    log(f"  BTC: {fmt_dollar(m['btc_price'])} / フェーズ: {m['phase'][1]} (score {m['bull_score']:+})")
    log(f"  FR: {fmt_fr(m['fr_current'])} ({m['fr_state']}) / CB: {fmt_pct(m['cb_premium'])} ({m['cb_state']})")
    log(f"  OI 24h変化: {fmt_pct(m['oi_change_24h'])} / 出来高: {m['vol_ratio']:.2f}x" if m['vol_ratio'] else "  OI/Vol: データ不足")
    log(f"  15分変動: {m['sudden_move_15m']*100:+.2f}%")

    sent_brief = check_daily_brief(state, m)
    sent_trend = check_trend_change(state, m)
    sent_sudden = check_sudden_move(state, m)
    save_state(state)

    summary = []
    if sent_brief: summary.append('📅朝ブリーフ')
    if sent_trend: summary.append('🔀トレンド転換')
    if sent_sudden: summary.append('⚡急変')
    log(f"=== 完了 / 送信: {' / '.join(summary) if summary else 'なし(平常)'} ===")


if __name__ == "__main__":
    main()
