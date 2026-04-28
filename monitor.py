"""
Altcoin Pump Monitor v3.6 — スマート通知版
GitHub Actions で 1時間ごとに実行

機能:
  Phase 1 急騰検出
    - top1000 取得 → v3.6条件
    - 急騰時刻 T_pump を特定(時間足から逆算)
    - 推奨エントリー時刻 T_entry = T_pump + 3h
    - state.json に schedule 登録

  Phase 2 スケジュール処理
    - 既存 schedule を巡回
    - 検知通知/エントリー通知/決済通知 を時刻に応じて送信

環境変数(GitHub Secrets / Variables):
  CG_API_KEY:        CoinGecko API キー
  DISCORD_WEBHOOK:   Discord webhook URL
  CG_PLAN:           "demo" or "pro"(デフォルト: demo)
  ACCOUNT_BALANCE:   口座残高(円、デフォルト: 100000)
  POSITION_PCT:      ポジションサイズ(0-1、デフォルト: 0.20)
  TEST_DISCORD:      "1" なら接続テストのみ実行
"""
import os
import sys
import json
import time
import re
import urllib.request
import urllib.parse
import urllib.error
from datetime import datetime, timezone, timedelta
from pathlib import Path

# ============= 設定 =============
CG_API_KEY = os.environ.get("CG_API_KEY", "").strip()
DISCORD_WEBHOOK = os.environ.get("DISCORD_WEBHOOK", "").strip()
CG_PLAN = os.environ.get("CG_PLAN", "demo").strip().lower()
ACCOUNT_BALANCE = float(os.environ.get("ACCOUNT_BALANCE", "100000"))
POSITION_PCT = float(os.environ.get("POSITION_PCT", "0.20"))

API_BASE = "https://pro-api.coingecko.com/api/v3" if CG_PLAN == "pro" else "https://api.coingecko.com/api/v3"
KEY_PARAM = "x_cg_pro_api_key" if CG_PLAN == "pro" else "x_cg_demo_api_key"
INTERVAL_SEC = 0.5 if CG_PLAN == "pro" else 2.5

# v3.6 戦略パラメータ
PUMP_THRESHOLD = 0.50
MAX_CH24 = 2.00
MIN_RANK = 250
MAX_RANK = 1000
MAX_30D_RATIO = 2.0
MIN_TURNOVER = 0.01
WAIT_HOURS = 3
HOLD_HOURS = 192
STOP_MULT = 1.60
TP_PCT = 0.50
DEDUP_HOURS = 48

# 通知タイミング
ENTRY_WINDOW_MIN = 60
EXIT_WINDOW_MIN = 60

STABLE_SYMBOLS = {
    'usdt','usdc','dai','busd','tusd','usde','usdp','usdd','gusd','fdusd',
    'pyusd','usdy','usds','usdt0','usdg','usdq','crvusd','frxusd','lusd','usdn',
    'usdx','eurc','eure','eurs','msusd','apxusd','usda','jchf','xsgd','xaut','paxg',
    'susd','buidl','usd1','usdm','usdb','xusd','rusd','usd0','usdz','gho','fxusd',
    'usdx0','usdl','flexusd','eusd','vchf','mim','tbtc','sbtc','cbbtc','wbtc','weth',
    'steth','cbeth','reth','btc','eth'
}
STABLE_NAME_RE = re.compile(
    r'stable|tether|\busd\b|usdc|dollar|\beur\b|\bgold\b|\bsilver\b|wrapped|staked|lido|bridged|restaked',
    re.IGNORECASE
)

STATE_FILE = Path(__file__).parent / "state.json"


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


def load_state():
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except Exception:
            pass
    return {"alerted": {}, "scheduled": []}


def save_state(state):
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2))


# ============= CoinGecko =============
def cg_get(path, params=None):
    params = dict(params or {})
    if CG_API_KEY:
        params[KEY_PARAM] = CG_API_KEY
    qs = urllib.parse.urlencode(params)
    url = f"{API_BASE}{path}?{qs}"
    req = urllib.request.Request(url, headers={"accept": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())


def fetch_top_coins(top_n=1000):
    coins = []
    pages = (top_n + 249) // 250
    for p in range(1, pages + 1):
        per_page = min(250, top_n - (p - 1) * 250)
        data = cg_get("/coins/markets", {
            "vs_currency": "usd", "order": "market_cap_desc",
            "per_page": per_page, "page": p, "sparkline": "false",
            "price_change_percentage": "24h"
        })
        coins.extend(data)
        if p < pages:
            time.sleep(INTERVAL_SEC)
    return coins


# ============= フィルタ =============
def basic_filter(coin):
    ch24 = coin.get("price_change_percentage_24h")
    if ch24 is None:
        return False, "ch24 None"
    sym = (coin.get("symbol") or "").lower()
    name = (coin.get("name") or "").lower()
    if sym in STABLE_SYMBOLS:
        return False, "stable symbol"
    if STABLE_NAME_RE.search(name):
        return False, "stable name"
    rank = coin.get("market_cap_rank")
    if rank is None:
        return False, "no rank"
    if rank < MIN_RANK or rank > MAX_RANK:
        return False, f"rank {rank}"
    if ch24 / 100 < PUMP_THRESHOLD:
        return False, f"ch24 {ch24:.1f}%"
    if ch24 / 100 > MAX_CH24:
        return False, f"ch24 {ch24:.1f}% > max"
    return True, "OK"


def deep_check(coin):
    """30日前比 + turnover + 急騰開始時刻"""
    try:
        data30 = cg_get(f"/coins/{urllib.parse.quote(coin['id'])}/market_chart", {
            "vs_currency": "usd", "days": 31
        })
        prices = data30.get("prices") or []
        volumes = data30.get("total_volumes") or []
        mcaps = data30.get("market_caps") or []
        if len(prices) < 100:
            return False, "データ不足", {}

        now_p = prices[-1][1]
        old_p = prices[0][1]
        ratio_30d = now_p / old_p if old_p > 0 else None
        if ratio_30d and ratio_30d > MAX_30D_RATIO:
            return False, f"30日前比 {ratio_30d:.2f}倍", {"ratio_30d": ratio_30d}

        recent_start = max(0, len(volumes) - 7 * 24)
        v_win = volumes[recent_start:]
        m_win = mcaps[recent_start:]
        avg_v = sum(v[1] for v in v_win) / max(1, len(v_win))
        avg_m = sum(m[1] for m in m_win) / max(1, len(m_win))
        turnover = avg_v / avg_m if avg_m > 0 else 0
        if turnover < MIN_TURNOVER:
            return False, f"turnover {turnover*100:.3f}%", {"ratio_30d": ratio_30d, "turnover": turnover}

        # 急騰開始時刻を特定
        pump_start = None
        for i in range(len(prices) - 1, 23, -1):
            cur = prices[i][1]
            ago = prices[i - 24][1]
            if ago <= 0:
                continue
            ch = cur / ago - 1
            if i - 1 < 24:
                if ch > 0.50:
                    pump_start = datetime.fromtimestamp(prices[i][0]/1000, tz=timezone.utc)
                continue
            prev_ago = prices[i - 1 - 24][1]
            if prev_ago <= 0:
                continue
            prev_ch = prices[i - 1][1] / prev_ago - 1
            if prev_ch <= 0.50 and ch > 0.50:
                pump_start = datetime.fromtimestamp(prices[i][0]/1000, tz=timezone.utc)
                break
        if pump_start is None:
            pump_start = datetime.fromtimestamp(prices[-1][0]/1000, tz=timezone.utc)

        return True, "OK", {
            "ratio_30d": ratio_30d, "turnover": turnover, "pump_start": pump_start
        }
    except Exception as e:
        return False, f"APIエラー: {e}", {}


def fetch_peak_and_price(coin_id, pump_start, entry_time):
    """pump_start ~ entry_time の最高値と現在価格を取得"""
    try:
        data = cg_get(f"/coins/{urllib.parse.quote(coin_id)}/market_chart", {
            "vs_currency": "usd", "days": 5
        })
        prices = data.get("prices") or []
        if not prices:
            return None, None
        cur_price = prices[-1][1]
        ps_ms = pump_start.timestamp() * 1000
        et_ms = entry_time.timestamp() * 1000
        in_window = [p[1] for p in prices if ps_ms <= p[0] <= et_ms]
        if not in_window:
            in_window = [cur_price]
        peak = max(in_window)
        return peak, cur_price
    except Exception as e:
        log(f"  fetch_peak_and_price エラー: {e}")
        return None, None


# ============= Discord 通知 =============
def discord_notify(content, embeds=None):
    if not DISCORD_WEBHOOK:
        log("WARN: DISCORD_WEBHOOK 未設定、通知スキップ")
        return False
    url = DISCORD_WEBHOOK.strip()
    log(f"DEBUG: Webhook URL長さ={len(url)}文字, 先頭={url[:45]}, 末尾={url[-15:]}")
    payload = {"content": content}
    if embeds:
        payload["embeds"] = embeds
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=body, headers={
        "Content-Type": "application/json",
        "User-Agent": "DiscordBot (https://github.com/kmd0704/altcoin-pump-monitor, 3.6)"
    })
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            log(f"DEBUG: HTTP {r.status} - 通知成功")
            return r.status in (200, 204)
    except urllib.error.HTTPError as e:
        try:
            err_body = e.read().decode("utf-8", errors="replace")[:200]
        except Exception:
            err_body = ""
        log(f"Discord通知失敗: HTTP {e.code} {e.reason} | body={err_body}")
        return False
    except Exception as e:
        log(f"Discord通知失敗: {type(e).__name__}: {e}")
        return False


def to_jst(dt):
    return dt.astimezone(timezone(timedelta(hours=9)))


def fmt_jst(dt):
    return to_jst(dt).strftime("%Y-%m-%d %H:%M JST")


def build_detection_embed(coin, deep_info, schedule):
    """検知時の通知"""
    sym = coin["symbol"].upper()
    cid = coin["id"]
    name = coin.get("name", "")
    rank = coin["market_cap_rank"]
    ch24 = coin["price_change_percentage_24h"]
    pump_start = from_iso(schedule["pump_time"])
    entry_time = from_iso(schedule["entry_time"])
    until_entry = (entry_time - now_utc()).total_seconds() / 60
    chart_url = f"https://www.coingecko.com/ja/coins/{cid}"
    embed = {
        "title": f"🟡 急騰検知 [{sym}] — クリックでチャート確認",
        "url": chart_url,
        "description": f"**{name}** (rank {rank}) — 現時点ではまだエントリーしません\n📊 [**CoinGecko でチャートを開く**]({chart_url})",
        "color": 0xf0b648,
        "fields": [
            {"name": "急騰開始時刻", "value": fmt_jst(pump_start), "inline": True},
            {"name": "★ エントリー予定時刻 ★", "value": fmt_jst(entry_time), "inline": True},
            {"name": "あと", "value": f"{int(until_entry)}分後", "inline": True},
            {"name": "24h急騰", "value": f"+{ch24:.1f}%", "inline": True},
            {"name": "30日前比", "value": f"{deep_info.get('ratio_30d',0):.2f}倍", "inline": True},
            {"name": "turnover", "value": f"{(deep_info.get('turnover') or 0)*100:.2f}%", "inline": True},
            {"name": "📌 行動",
             "value": (
                f"・**今すぐ何もしない**\n"
                f"・エントリー予定時刻が近づいたら自動で再通知\n"
                f"・[CoinGecko チャートを確認]({chart_url}) しておくと吉"
             ), "inline": False},
        ],
        "footer": {"text": "v3.6 / 検知 → 3時間様子見 → 推奨時刻でエントリー"},
        "timestamp": now_utc().isoformat()
    }
    return embed


def build_entry_embed(schedule, peak, cur_price):
    """エントリー時刻の通知(Phase 2 執行チェックリスト付き)"""
    sym = schedule["symbol"]
    cid = schedule["coin_id"]
    rank = schedule.get("rank")
    entry_time = from_iso(schedule["entry_time"])
    exit_time = from_iso(schedule["exit_time"])
    delta_min = (now_utc() - entry_time).total_seconds() / 60
    position_yen = ACCOUNT_BALANCE * POSITION_PCT
    position_usdt = position_yen / 150
    stop_price = peak * STOP_MULT
    tp_price = cur_price * (1 - TP_PCT)
    stop_dist = (stop_price / cur_price - 1) * 100
    chart_url = f"https://www.coingecko.com/ja/coins/{cid}"
    mexc_url = f"https://futures.mexc.com/exchange/{sym}_USDT"

    embed = {
        "title": f"🚨 エントリー時刻です [{sym}] — クリックでチャート確認",
        "url": chart_url,
        "description": (
            f"**いますぐ MEXC でショート発注** | 推奨時刻 {fmt_jst(entry_time)}({int(delta_min):+}分)\n"
            f"📊 [**CoinGecko でチャート確認**]({chart_url}) | "
            f"⚡ [**MEXC で発注画面**]({mexc_url})"
        ),
        "color": 0xe06c6c,
        "fields": [
            {"name": "🎯 銘柄", "value": f"`{sym}USDT` (Perpetual)", "inline": True},
            {"name": "ランク", "value": f"#{rank}", "inline": True},
            {"name": "ポジション", "value": f"{int(position_yen):,}円 ≒ **${position_usdt:.2f} USDT**", "inline": True},
            {"name": "🟢 Peak価格", "value": f"`${peak:.8f}`", "inline": True},
            {"name": "🟡 エントリー価格", "value": f"`${cur_price:.8f}`", "inline": True},
            {"name": "💥 利確-50%", "value": f"`${tp_price:.8f}`", "inline": True},
            {"name": "🛑 ストップ", "value": f"`${stop_price:.8f}` ({stop_dist:+.1f}%)", "inline": True},
            {"name": "⏰ 強制決済時刻", "value": fmt_jst(exit_time), "inline": True},
            {"name": " ", "value": " ", "inline": True},
            {"name": "✅ Phase 2 執行チェックリスト",
             "value": (
                f"**① MEXC で `{sym}USDT` を開く** [→ クリック]({mexc_url})\n"
                f"**② USDT-M Perpetual** を選択(現物ではない)\n"
                f"**③ ⚠ ショート(Short/Sell)を選択** ← 一番大事\n"
                f"**④ レバレッジを 1x に設定**(必須)\n"
                f"**⑤ 証拠金を `${position_usdt:.2f} USDT` 入力**\n"
                f"**⑥ 成行注文(Market)で発注**\n"
                f"**⑦ ストップロス: `${stop_price:.8f}`**\n"
                f"**⑧ 利確(TP): `${tp_price:.8f}`**\n"
                f"**⑨ カレンダーに {fmt_jst(exit_time)} を登録**"
             ), "inline": False},
            {"name": "🔗 取引所",
             "value": (
                f"⚡ [**MEXC(推奨)**]({mexc_url}) | "
                f"[Bybit](https://www.bybit.com/en/trade/usdt/{sym}USDT) | "
                f"[Binance](https://www.binance.com/en/futures/{sym}USDT)"
             ), "inline": False},
        ],
        "footer": {"text": "v3.6 / TP=50% / Stop=Peak×1.60 / Hold=192h / Phase2"},
        "timestamp": now_utc().isoformat()
    }
    return embed


def build_exit_embed(schedule):
    sym = schedule["symbol"]
    cid = schedule["coin_id"]
    chart_url = f"https://www.coingecko.com/ja/coins/{cid}"
    return {
        "title": f"⏰ 強制決済時刻 [{sym}] — クリックでチャート確認",
        "url": chart_url,
        "description": f"保有192時間経過。**成行で決済**してください。\n📊 [**CoinGecko でチャート確認**]({chart_url})",
        "color": 0x4fc3f7,
        "fields": [
            {"name": "銘柄", "value": f"{sym}USDT", "inline": True},
            {"name": "エントリー時刻", "value": fmt_jst(from_iso(schedule['entry_time'])), "inline": True},
            {"name": "決済時刻", "value": fmt_jst(now_utc()), "inline": True},
            {"name": "📊 チャート / 取引所",
             "value": (
                f"📈 [**CoinGecko チャート**]({chart_url})\n"
                f"💱 [Bybit](https://www.bybit.com/en/trade/usdt/{sym}USDT) | "
                f"[Binance](https://www.binance.com/en/futures/{sym}USDT) | "
                f"[MEXC](https://futures.mexc.com/exchange/{sym}_USDT)"
             ), "inline": False},
        ],
        "footer": {"text": "v3.6 / 192h 経過"}
    }


# ============= 状態管理 =============
def is_alerted_recently(coin_id, alerted):
    last = alerted.get(coin_id)
    if not last:
        return False
    return (now_utc() - from_iso(last)) < timedelta(hours=DEDUP_HOURS)


def cleanup_state(state):
    cutoff = now_utc() - timedelta(hours=DEDUP_HOURS * 2)
    state["alerted"] = {
        cid: ts for cid, ts in (state.get("alerted") or {}).items()
        if from_iso(ts) > cutoff
    }
    cutoff_sched = now_utc() - timedelta(hours=24)
    state["scheduled"] = [
        s for s in (state.get("scheduled") or [])
        if from_iso(s["exit_time"]) > cutoff_sched
    ]
    return state


# ============= メインフロー =============
def detect_phase(state):
    """Phase 1: 新規急騰検出"""
    log("Phase 1: top1000 取得...")
    coins = fetch_top_coins(1000)
    log(f"  {len(coins)} 銘柄取得")

    candidates = [c for c in coins if basic_filter(c)[0]]
    log(f"  急騰候補: {len(candidates)}件")

    new_count = 0
    for c in candidates:
        if is_alerted_recently(c["id"], state.get("alerted", {})):
            log(f"  {c['symbol'].upper()} - 既にアラート済み(スキップ)")
            continue
        time.sleep(INTERVAL_SEC)
        ok, reason, info = deep_check(c)
        if not ok:
            log(f"  ✗ {c['symbol'].upper()} 除外: {reason}")
            continue

        pump_start = info.get("pump_start", now_utc())
        entry_time = pump_start + timedelta(hours=WAIT_HOURS)
        exit_time = entry_time + timedelta(hours=HOLD_HOURS)
        sched = {
            "coin_id": c["id"],
            "symbol": c["symbol"].upper(),
            "name": c.get("name", ""),
            "rank": c.get("market_cap_rank"),
            "ch24_at_pump": c.get("price_change_percentage_24h"),
            "ratio_30d": info.get("ratio_30d"),
            "turnover": info.get("turnover"),
            "price_at_detection": c.get("current_price"),
            "pump_time": to_iso(pump_start),
            "entry_time": to_iso(entry_time),
            "exit_time": to_iso(exit_time),
            "detection_notified": False,
            "entry_notified": False,
            "exit_notified": False,
        }
        state["scheduled"].append(sched)
        state["alerted"][c["id"]] = to_iso(now_utc())
        log(f"  ✓ {c['symbol'].upper()} スケジュール登録: エントリー予定 {fmt_jst(entry_time)}")
        new_count += 1
    return new_count


def schedule_phase(state):
    """Phase 2: 既存スケジュールの処理"""
    log("Phase 2: スケジュール処理...")
    notified_count = {"detect": 0, "entry": 0, "exit": 0}
    now = now_utc()

    for s in state["scheduled"]:
        sym = s["symbol"]
        cid = s["coin_id"]
        entry_t = from_iso(s["entry_time"])
        exit_t = from_iso(s["exit_time"])

        # 検知通知
        if not s.get("detection_notified"):
            mins_until_entry = (entry_t - now).total_seconds() / 60
            if 0 <= mins_until_entry <= 30:
                s["detection_notified"] = True
            else:
                fake_coin = {"symbol": sym, "name": s["name"], "market_cap_rank": s["rank"],
                             "price_change_percentage_24h": s["ch24_at_pump"], "id": cid}
                deep = {"ratio_30d": s["ratio_30d"], "turnover": s["turnover"]}
                embed = build_detection_embed(fake_coin, deep, s)
                if mins_until_entry < 0:
                    msg = f"⚠ **検知 [{sym}]**(エントリー時刻 {abs(int(mins_until_entry))}分前に通過済)"
                else:
                    msg = f"🟡 **検知 [{sym}]** エントリー予定 {fmt_jst(entry_t)}"
                discord_notify(msg, embeds=[embed])
                s["detection_notified"] = True
                notified_count["detect"] += 1
                log(f"  📨 detection notify: {sym} (mins_until={int(mins_until_entry)})")

        # エントリー通知
        if not s.get("entry_notified"):
            mins_until = abs((entry_t - now).total_seconds() / 60)
            if mins_until <= ENTRY_WINDOW_MIN:
                pump_start = from_iso(s["pump_time"])
                peak, cur = fetch_peak_and_price(cid, pump_start, now)
                if peak is None or cur is None:
                    log(f"  ⚠ {sym} peak/price 取得失敗、entry通知保留")
                    continue
                embed = build_entry_embed(s, peak, cur)
                discord_notify(f"🚨 **エントリー時刻 [{sym}]** いますぐ発注!", embeds=[embed])
                s["entry_notified"] = True
                s["peak_price"] = peak
                s["entry_price"] = cur
                notified_count["entry"] += 1
                log(f"  ⚡ entry notify: {sym} peak=${peak:.8f} entry=${cur:.8f}")
                time.sleep(INTERVAL_SEC)

        # 決済通知
        if not s.get("exit_notified") and s.get("entry_notified"):
            mins_until = abs((exit_t - now).total_seconds() / 60)
            if mins_until <= EXIT_WINDOW_MIN:
                embed = build_exit_embed(s)
                discord_notify(f"⏰ **決済時刻 [{sym}]** 192h 経過、成行決済を!", embeds=[embed])
                s["exit_notified"] = True
                notified_count["exit"] += 1
                log(f"  🏁 exit notify: {sym}")

    return notified_count


def main():
    log(f"=== Altcoin Pump Monitor v3.6(スマート通知版)起動 / プラン={CG_PLAN} ===")
    if not CG_API_KEY:
        log("ERROR: CG_API_KEY 未設定")
        sys.exit(1)
    if not DISCORD_WEBHOOK:
        log("WARN: DISCORD_WEBHOOK 未設定(通知なし)")

    # Discord 接続テスト(TEST_DISCORD=1 を Variable に設定すると実行)
    if os.environ.get("TEST_DISCORD", "").strip() == "1":
        log("🧪 TEST_DISCORD モード:接続テスト送信中...")
        ok = discord_notify(
            "🧪 **Discord 接続テスト**\n"
            f"GitHub Actions から正常に到達しました。プラン={CG_PLAN}\n"
            "このメッセージが見えたら配線OK!\n"
            "確認後 GitHub Variable の TEST_DISCORD を削除してください。"
        )
        log(f"テスト結果: {'✅ 成功' if ok else '❌ 失敗'}")
        return

    state = load_state()
    state = cleanup_state(state)

    new_detected = detect_phase(state)
    counts = schedule_phase(state)

    save_state(state)
    log(f"=== 完了:新規検知 {new_detected}件 / 通知 検知{counts['detect']} エントリー{counts['entry']} 決済{counts['exit']} / 追跡中 {len(state['scheduled'])}件 ===")


if __name__ == "__main__":
    main()
