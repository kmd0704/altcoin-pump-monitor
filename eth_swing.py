"""
ETH/BTC Swing Short Monitor — 戦略A

戦略A: 24h ETH先行 +3% 検出 → 4hピーク確定 → ETH ショート → TP-4% / SL+4% / 5日保有

過去1年バックテスト:
  ・57件 / 勝率63% / 平均PnL +0.99%/trade / 累計 +56.5%
  ・5年データでも構造的に支持(ETH先行+6% → 5-10日後 中央値 -2.81%)

通知:
  🔵 検知通知    : 24h ETH先行+3%確認、ピーク確定待ち
  ⚡ エントリー通知: 4時間連続新高値なし → ピーク確定、ショート発注
  ⏰ 決済通知    : 5日経過、強制決済

環境変数:
  DISCORD_WEBHOOK_ETH:   Discord webhook URL(専用チャンネル)
  ACCOUNT_BALANCE_ETH:   口座残高 USD(デフォルト 1000)
  POSITION_PCT_ETH:      ポジションサイズ(デフォルト 0.10 = 10%)
  TEST_DISCORD_ETH:      "1" で接続テストのみ
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
DISCORD_WEBHOOK = os.environ.get("DISCORD_WEBHOOK_ETH", "").strip()
ACCOUNT_BALANCE = float(os.environ.get("ACCOUNT_BALANCE_ETH", "1000"))  # USD
POSITION_PCT = float(os.environ.get("POSITION_PCT_ETH", "0.10"))         # 10%

# 戦略A パラメータ
LEAD_THRESHOLD = 0.03    # 24h ETH先行 +3% で検知
NNH_HOURS = 4            # 4時間連続新高値なし → ピーク確定
TP_PCT = 0.04             # 利確 -4%
SL_PCT = 0.04             # ストップ +4%
HOLD_HOURS = 120          # 5日(120h)
ENTRY_WAIT_LIMIT = 24     # 検知後24h以内にピーク確定しなければ機会消失
EXIT_WINDOW_MIN = 60      # 決済通知発火窓
DEDUP_HOURS = 48          # 同一シグナル重複抑止

STATE_FILE = Path(__file__).parent / "eth_swing_state.json"


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
    return {"scheduled": [], "last_alert_time": None}


def save_state(state):
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2))


# ============= Binance API =============
def fetch_binance_klines(symbol, interval='1h', hours=30):
    """Binance Public API で klines 取得"""
    end_ms = int(time.time() * 1000)
    start_ms = end_ms - hours * 3600 * 1000
    params = {
        'symbol': symbol,
        'interval': interval,
        'startTime': start_ms,
        'endTime': end_ms,
        'limit': 200,
    }
    url = f"https://api.binance.com/api/v3/klines?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())


def calc_24h_lead():
    """24時間 ETH先行を計算"""
    btc = fetch_binance_klines('BTCUSDT', '1h', 30)
    eth = fetch_binance_klines('ETHUSDT', '1h', 30)
    if len(btc) < 25 or len(eth) < 25:
        raise Exception(f"klines データ不足: btc={len(btc)} eth={len(eth)}")

    btc_24h_ago = float(btc[-25][4])  # 24時間前のクローズ
    btc_now = float(btc[-1][4])
    eth_24h_ago = float(eth[-25][4])
    eth_now = float(eth[-1][4])

    btc_ret = (btc_now / btc_24h_ago) - 1
    eth_ret = (eth_now / eth_24h_ago) - 1
    eth_lead = eth_ret - btc_ret

    return {
        'btc_ret_24h': btc_ret,
        'eth_ret_24h': eth_ret,
        'eth_lead_24h': eth_lead,
        'btc_close': btc_now,
        'eth_close': eth_now,
        'eth_klines': eth,
        'btc_klines': btc,
    }


def fetch_eth_recent():
    """直近の ETH klines(NNH+α 時間)"""
    return fetch_binance_klines('ETHUSDT', '1h', NNH_HOURS + 4)


# ============= Discord =============
def discord_notify(content, embeds=None):
    if not DISCORD_WEBHOOK:
        log("WARN: DISCORD_WEBHOOK_ETH 未設定、通知スキップ")
        return False
    payload = {"content": content}
    if embeds:
        payload["embeds"] = embeds
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(DISCORD_WEBHOOK, data=body, headers={
        "Content-Type": "application/json",
        "User-Agent": "DiscordBot (https://github.com/kmd0704/altcoin-pump-monitor, eth-swing-1.0)"
    })
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            log(f"Discord通知 HTTP {r.status}")
            return r.status in (200, 204)
    except urllib.error.HTTPError as e:
        try:
            err = e.read().decode('utf-8', errors='replace')[:200]
        except Exception:
            err = ''
        log(f"Discord通知失敗: HTTP {e.code} | {err}")
        return False
    except Exception as e:
        log(f"Discord通知失敗: {type(e).__name__}: {e}")
        return False


# ============= Embed builders =============
def build_detection_embed(data):
    eth_lead = data['eth_lead_24h']
    eth_close = data['eth_close']
    btc_close = data['btc_close']
    return {
        "title": "🔵 ETH先行検知 — スイングショート候補",
        "url": "https://www.coingecko.com/ja/coins/ethereum",
        "description": (
            f"**24h ETH先行: +{eth_lead*100:.2f}%**(BTCより)\n"
            f"ETH 24h: {data['eth_ret_24h']*100:+.2f}% / BTC 24h: {data['btc_ret_24h']*100:+.2f}%\n\n"
            "📌 **行動**\n"
            "・**今すぐ何もしない**\n"
            "・ETH が4時間連続で新高値を更新しなくなったら ⚡ エントリー通知が来る\n"
            "・チャート確認しておくと吉"
        ),
        "color": 0x4fc3f7,
        "fields": [
            {"name": "ETH価格", "value": f"${eth_close:,.2f}", "inline": True},
            {"name": "BTC価格", "value": f"${btc_close:,.2f}", "inline": True},
            {"name": "ETH/BTC比率", "value": f"{eth_close/btc_close:.5f}", "inline": True},
        ],
        "footer": {"text": "戦略A: 24h ETH先行 +3% / 5日ホールド / 過去1年勝率63%"},
        "timestamp": now_utc().isoformat()
    }


def build_entry_embed(schedule, peak, cur_price):
    entry_time = from_iso(schedule['entry_time'])
    exit_time = from_iso(schedule['exit_time'])
    delta_min = (now_utc() - entry_time).total_seconds() / 60
    position_usd = ACCOUNT_BALANCE * POSITION_PCT
    stop_price = cur_price * (1 + SL_PCT)
    tp_price = cur_price * (1 - TP_PCT)
    chart_url = "https://www.coingecko.com/ja/coins/ethereum"
    mexc_url = "https://futures.mexc.com/exchange/ETH_USDT"

    return {
        "title": "⚡ ETH スイングショート — エントリー時刻",
        "url": chart_url,
        "description": (
            f"**いますぐ MEXC で ETH ショート発注** | 推奨時刻 {fmt_jst(entry_time)}({int(delta_min):+}分)\n"
            f"📊 [**CoinGecko**]({chart_url}) | ⚡ [**MEXC**]({mexc_url})"
        ),
        "color": 0xe06c6c,
        "fields": [
            {"name": "🎯 銘柄", "value": "`ETHUSDT` (Perpetual)", "inline": True},
            {"name": "ピーク価格", "value": f"`${peak:,.2f}`", "inline": True},
            {"name": "エントリー価格", "value": f"`${cur_price:,.2f}`", "inline": True},
            {"name": "ポジション", "value": f"**${position_usd:.2f} USDT** ({int(POSITION_PCT*100)}%)", "inline": True},
            {"name": "💰 利確指値 (TP -4%)", "value": f"`${tp_price:,.2f}`", "inline": True},
            {"name": "🛑 ストップ指値 (SL +4%)", "value": f"`${stop_price:,.2f}`", "inline": True},
            {"name": "⏰ 強制決済(5日後)", "value": fmt_jst(exit_time), "inline": False},
            {"name": "✅ 鎌田さん手動チェック",
             "value": (
                "**□ BTC 30日 +20% 超えてないか**(超えてたら見送り)\n"
                "**□ FOMC・CPI等の重大日でないか**\n"
                "**□ チャートで明確な上昇トレンドの最中ではないか**\n"
                "**□ ETHのファンディングレートは異常値でないか**"
             ), "inline": False},
            {"name": "📋 執行手順",
             "value": (
                f"**① MEXC で ETHUSDT を開く** [→ クリック]({mexc_url})\n"
                "**② USDT-M Perpetual** を選択(現物ではない)\n"
                "**③ ⚠ ショート(Short/Sell)を選択** ← 一番大事\n"
                "**④ レバレッジを 1x に設定**(必須)\n"
                f"**⑤ 証拠金 `${position_usd:.2f} USDT` 入力**\n"
                "**⑥ 成行注文(Market)で発注**\n"
                f"**⑦ ストップロス指値: `${stop_price:,.2f}`**\n"
                f"**⑧ 利確指値: `${tp_price:,.2f}`**\n"
                f"**⑨ カレンダーに {fmt_jst(exit_time)} を登録**"
             ), "inline": False},
            {"name": "🔗 取引所",
             "value": (
                f"⚡ [**MEXC(推奨)**]({mexc_url}) | "
                "[Bybit](https://www.bybit.com/en/trade/usdt/ETHUSDT) | "
                "[Binance](https://www.binance.com/en/futures/ETHUSDT)"
             ), "inline": False},
        ],
        "footer": {"text": "戦略A / TP-4% / SL+4% / Hold 5日 / 過去1年勝率63%"},
        "timestamp": now_utc().isoformat()
    }


def build_exit_embed(schedule):
    chart_url = "https://www.coingecko.com/ja/coins/ethereum"
    return {
        "title": "⏰ ETH スイングショート — 強制決済時刻",
        "url": chart_url,
        "description": "保有 5日(120h)経過。**成行で決済**してください。",
        "color": 0x4fc3f7,
        "fields": [
            {"name": "銘柄", "value": "ETHUSDT", "inline": True},
            {"name": "エントリー時刻", "value": fmt_jst(from_iso(schedule['entry_time'])), "inline": True},
            {"name": "決済時刻", "value": fmt_jst(now_utc()), "inline": True},
            {"name": "📊 チャート / 取引所",
             "value": (
                f"📈 [**CoinGecko**]({chart_url})\n"
                "💱 [Bybit](https://www.bybit.com/en/trade/usdt/ETHUSDT) | "
                "[Binance](https://www.binance.com/en/futures/ETHUSDT) | "
                "[MEXC](https://futures.mexc.com/exchange/ETH_USDT)"
             ), "inline": False},
        ],
        "footer": {"text": "戦略A / 5日経過"}
    }


# ============= 状態管理 =============
def is_alerted_recently(state):
    last_alert = state.get('last_alert_time')
    if not last_alert:
        return False
    return (now_utc() - from_iso(last_alert)).total_seconds() < DEDUP_HOURS * 3600


def cleanup_state(state):
    cutoff = now_utc() - timedelta(days=10)
    kept = []
    for s in state.get('scheduled', []):
        # exit_time が無いか、cutoff より新しいものは保持
        exit_t = s.get('exit_time')
        if exit_t and from_iso(exit_t) < cutoff:
            continue
        kept.append(s)
    state['scheduled'] = kept
    return state


# ============= メインフロー =============
def detect_phase(state):
    log("Phase 1: 24h ETH先行 計算...")
    try:
        data = calc_24h_lead()
    except Exception as e:
        log(f"Binance API エラー: {e}")
        return 0

    eth_lead = data['eth_lead_24h']
    log(f"  24h ETH先行: {eth_lead*100:+.2f}% / 閾値: +{LEAD_THRESHOLD*100:.1f}%")
    log(f"  ETH 24h: {data['eth_ret_24h']*100:+.2f}% / BTC 24h: {data['btc_ret_24h']*100:+.2f}%")

    if eth_lead < LEAD_THRESHOLD:
        log(f"  シグナルなし(閾値未満)")
        return 0
    if is_alerted_recently(state):
        log(f"  既に直近{DEDUP_HOURS}h以内に検知済み(スキップ)")
        return 0

    detect_time = now_utc()
    # 直近 NNH 時間の高値を初期 running_max に
    init_max = max(float(k[2]) for k in data['eth_klines'][-NNH_HOURS:])
    sched = {
        "detect_time": to_iso(detect_time),
        "eth_lead_24h": eth_lead,
        "btc_ret_24h": data['btc_ret_24h'],
        "eth_ret_24h": data['eth_ret_24h'],
        "eth_close_at_detect": data['eth_close'],
        "btc_close_at_detect": data['btc_close'],
        "running_max": init_max,
        "running_max_time": to_iso(detect_time),
        "entry_time": None,
        "exit_time": None,
        "detection_notified": False,
        "entry_notified": False,
        "exit_notified": False,
        "abandoned": False,
        "peak_price": None,
        "entry_price": None,
    }
    state['scheduled'].append(sched)
    state['last_alert_time'] = to_iso(detect_time)
    log(f"  ✓ ETH先行検知: +{eth_lead*100:.2f}% / 初期running_max=${init_max:.2f}")
    return 1


def schedule_phase(state):
    log("Phase 2: スケジュール処理...")
    notified = {'detect': 0, 'entry': 0, 'exit': 0}
    now = now_utc()

    cur_eth_price = None
    recent_eth_high = None
    try:
        eth_klines = fetch_eth_recent()
        cur_eth_price = float(eth_klines[-1][4])
        recent_eth_high = float(eth_klines[-1][2])
    except Exception as e:
        log(f"Binance データ取得エラー(Phase2スキップ): {e}")
        return notified

    for s in state['scheduled']:
        if s.get('abandoned'):
            continue

        # === 検知通知 ===
        if not s['detection_notified']:
            data_for_embed = {
                'eth_lead_24h': s['eth_lead_24h'],
                'btc_ret_24h': s['btc_ret_24h'],
                'eth_ret_24h': s['eth_ret_24h'],
                'eth_close': s['eth_close_at_detect'],
                'btc_close': s['btc_close_at_detect'],
            }
            embed = build_detection_embed(data_for_embed)
            discord_notify(f"🔵 **ETH先行検知** +{s['eth_lead_24h']*100:.2f}% — ピーク確定を待機", embeds=[embed])
            s['detection_notified'] = True
            notified['detect'] += 1
            log(f"  📨 検知通知: ETH +{s['eth_lead_24h']*100:.2f}%")

        # === エントリー通知(ピーク確定したら) ===
        if not s['entry_notified']:
            since_detect = (now - from_iso(s['detect_time'])).total_seconds() / 3600
            if since_detect > ENTRY_WAIT_LIMIT:
                log(f"  ⚠ 検知から{ENTRY_WAIT_LIMIT}h以上経過、機会消失で打ち切り")
                s['abandoned'] = True
                s['entry_notified'] = True
                continue

            # running_max 更新
            running_max = s.get('running_max', s['eth_close_at_detect'])
            running_max_time = from_iso(s.get('running_max_time', s['detect_time']))

            if recent_eth_high > running_max:
                running_max = recent_eth_high
                running_max_time = now
                s['running_max'] = running_max
                s['running_max_time'] = to_iso(now)

            # ピーク確定: running_max_time から NNH時間 経過
            since_max = (now - running_max_time).total_seconds() / 3600
            if since_max >= NNH_HOURS:
                peak = running_max
                cur = cur_eth_price
                entry_time = now
                exit_time = entry_time + timedelta(hours=HOLD_HOURS)
                s['peak_price'] = peak
                s['entry_price'] = cur
                s['entry_time'] = to_iso(entry_time)
                s['exit_time'] = to_iso(exit_time)

                embed = build_entry_embed(s, peak, cur)
                discord_notify(f"⚡ **ETH スイングショート エントリー時刻** — いますぐ発注!", embeds=[embed])
                s['entry_notified'] = True
                notified['entry'] += 1
                log(f"  ⚡ エントリー通知: peak=${peak:.2f} entry=${cur:.2f}")
                time.sleep(1)

        # === 決済通知 ===
        if s['entry_notified'] and not s['exit_notified'] and s.get('exit_time'):
            mins_until = abs((from_iso(s['exit_time']) - now).total_seconds() / 60)
            if mins_until <= EXIT_WINDOW_MIN:
                embed = build_exit_embed(s)
                discord_notify(f"⏰ **ETH決済時刻** 5日経過、成行決済を!", embeds=[embed])
                s['exit_notified'] = True
                notified['exit'] += 1
                log(f"  🏁 決済通知")

    return notified


def main():
    log("=== ETH/BTC Swing Short Monitor 起動 / 戦略A ===")
    log(f"  パラメータ: 24h ETH先行 ≥ +{LEAD_THRESHOLD*100:.0f}% / NNH={NNH_HOURS}h / TP-{int(TP_PCT*100)}% / SL+{int(SL_PCT*100)}% / Hold {HOLD_HOURS}h")
    log(f"  口座: ${ACCOUNT_BALANCE:.0f} / ポジサイズ: {POSITION_PCT*100:.0f}%")

    if not DISCORD_WEBHOOK:
        log("WARN: DISCORD_WEBHOOK_ETH 未設定(通知なし)")

    if os.environ.get("TEST_DISCORD_ETH", "").strip() == "1":
        log("🧪 TEST_DISCORD_ETH モード:接続テスト送信中...")
        ok = discord_notify(
            "🧪 **ETH Swing Bot 接続テスト**\n"
            "GitHub Actions から正常に到達しました。\n"
            "戦略A: 24h ETH先行 +3% / 4hピーク確定 / 5日スイングショート\n"
            "過去1年バックテスト: 勝率63% / 平均+0.99% / 累計+56.5%\n"
            "確認後、GitHub Variable の TEST_DISCORD_ETH を削除してください。"
        )
        log(f"テスト結果: {'✅ 成功' if ok else '❌ 失敗'}")
        return

    state = load_state()
    state = cleanup_state(state)

    new_detected = detect_phase(state)
    counts = schedule_phase(state)

    save_state(state)
    log(f"=== 完了:検知{new_detected} / 通知 検知{counts['detect']} エントリー{counts['entry']} 決済{counts['exit']} / 追跡中{len(state['scheduled'])} ===")


if __name__ == "__main__":
    main()
