"""
Altcoin Pump Monitor — s-260517 専用 (独立BOT)
GitHub Actions で 1時間ごとに実行

【この BOT について】
即時スキャルプ戦略。検知瞬間にエントリーし、24h以内に決着させる。
train/test 整合性◎(差0.08のみ)で 3戦略中 最も信頼できる候補。

【戦略仕様】
  検知条件:
    - 24h変動率: 60%〜500%
    - 時価総額ランク: 250〜900 (m版相当の狭めレンジ)
    - 30日前比: ≤ 3.0倍 (= ret_30d ≤ 200%)
    - 7日 turnover: ≥ 1%/日
    - vol_z: ≥ 0 (NaN は通す)
    - 直近7日リターン: ≤ +50% (★ 持続上昇排除フィルタ★)
  挙動:
    - エントリー: pump_start と同時 (wait=0、即時)
    - 利確 TP: -20% (entry比)
    - 損切 SL: +5% (entry比、超タイト)
    - 保有時間: 24時間
  期待値 (バックテスト):
    - 月間 約 18件
    - pnl>0率 57.7% / TP-hit率 47.7% / SL-hit率 42.3% / 時間切れ 9.9%
    - PF 5.00
    - 平均PnL/件 +8.5%
    - Sharpe 0.71 (test) / 0.79 (train) ← train/test 整合性◎

【環境変数 (GitHub Secrets / Variables)】
  CG_API_KEY:                  CoinGecko API キー(共有 OK)
  DISCORD_WEBHOOK_S_260517:    Discord webhook URL (★ 新規・専用)
  CG_PLAN:                     "demo" or "pro" (デフォルト: demo)
  ACCOUNT_BALANCE:             口座残高(円、デフォルト: 100000)
  POSITION_PCT:                ポジションサイズ(0-1、デフォルト: 0.20)
  TEST_DISCORD_S_260517:       "1" なら接続テストのみ
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
DISCORD_WEBHOOK = os.environ.get("DISCORD_WEBHOOK_S_260517", "").strip()
CG_PLAN = os.environ.get("CG_PLAN", "demo").strip().lower()
ACCOUNT_BALANCE = float(os.environ.get("ACCOUNT_BALANCE", "100000"))
POSITION_PCT = float(os.environ.get("POSITION_PCT", "0.20"))

API_BASE = "https://pro-api.coingecko.com/api/v3" if CG_PLAN == "pro" else "https://api.coingecko.com/api/v3"
KEY_PARAM = "x_cg_pro_api_key" if CG_PLAN == "pro" else "x_cg_demo_api_key"
INTERVAL_SEC = 0.5 if CG_PLAN == "pro" else 2.5

# ============= 戦略パラメータ (s-260517) =============
STRATEGY_LABEL = "s-260517"
STRATEGY_NOTE = "即時スキャルプ / TP-20%/SL+5%/hold24h/wait0h"

# 検知フィルタ
CH24_MIN = 0.60                 # 24h変動率 ≥ 60%
CH24_MAX = 5.00                 # 24h変動率 ≤ 500%
RANK_MIN = 250                  # 時価総額ランク ≥ 250 (m版相当)
RANK_MAX = 900                  # 時価総額ランク ≤ 900 (m版相当)
RATIO_30D_MAX = 3.0             # 30日前比 ≤ 3.0倍 (ret_30d ≤ 200%)
TURNOVER_MIN = 0.01             # 7日 turnover ≥ 1%/日
VOL_Z_MIN = 0.0                 # vol_z ≥ 0 (NaN は通す)
RET7_MAX = 0.5                  # 直近7日リターン ≤ +50% (持続上昇排除)

# エントリー / 決済
WAIT_HOURS = 0                  # 即時エントリー (pump_start と同時)
HOLD_HOURS = 24                 # entry + 24h で決済
TP_PCT = 0.20                   # 利確 -20%
SL_PCT = 0.05                   # 損切 +5% (entry比、超タイト)

# その他
DEDUP_HOURS = 48                # 同銘柄の再アラート抑止
ENTRY_WINDOW_MIN = 60           # エントリー通知許容窓
EXIT_WINDOW_MIN = 60            # 決済通知許容窓
PUMP_DETECT_THRESHOLD = 0.50    # pump_start 特定用(50%超え始めた時点)

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

# ★ 独立 state file (既存BOTと混ざらない)
STATE_FILE = Path(__file__).parent / "monitor_s_260517_state.json"


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
    return {"alerted": {}, "scheduled": []}

def save_state(state):
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2))


# ============= CoinGecko =============
def cg_get(path, params=None, max_retries=4):
    """CoinGecko GET with retry + exponential backoff."""
    params = dict(params or {})
    if CG_API_KEY:
        params[KEY_PARAM] = CG_API_KEY
    qs = urllib.parse.urlencode(params)
    url = f"{API_BASE}{path}?{qs}"
    req = urllib.request.Request(url, headers={"accept": "application/json"})
    last_err = None
    for attempt in range(max_retries):
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                return json.loads(r.read())
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, ConnectionError) as e:
            last_err = e
            code = getattr(e, "code", None)
            wait = 30 if code == 429 else (2 ** attempt) * 3
            log(f"  cg_get {path} attempt {attempt+1}/{max_retries} failed: {e}  → retry in {wait}s")
            time.sleep(wait)
    raise last_err


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


# ============= 戦略フィルタ =============
def basic_filter(coin):
    """basic_filter: rank / ch24 / ステーブル除外 (s-260517 専用)"""
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
    if rank < RANK_MIN or rank > RANK_MAX:
        return False, f"rank {rank} out of [{RANK_MIN},{RANK_MAX}]"
    ch24_frac = ch24 / 100
    if ch24_frac < CH24_MIN:
        return False, f"ch24 {ch24:.1f}% < {CH24_MIN*100:.0f}%"
    if ch24_frac > CH24_MAX:
        return False, f"ch24 {ch24:.1f}% > {CH24_MAX*100:.0f}%"
    return True, "OK"


def deep_check(coin):
    """30日比 + turnover + vol_z + pump_start 特定 (s-260517 専用)"""
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
        if ratio_30d and ratio_30d > RATIO_30D_MAX:
            return False, f"30日前比 {ratio_30d:.2f}倍 > {RATIO_30D_MAX}", {"ratio_30d": ratio_30d}

        recent_start = max(0, len(volumes) - 7 * 24)
        v_win = volumes[recent_start:]
        m_win = mcaps[recent_start:]
        avg_v = sum(v[1] for v in v_win) / max(1, len(v_win))
        avg_m = sum(m[1] for m in m_win) / max(1, len(m_win))
        turnover = avg_v / avg_m if avg_m > 0 else 0
        if turnover < TURNOVER_MIN:
            return False, f"turnover {turnover*100:.3f}% < {TURNOVER_MIN*100:.1f}%", {"ratio_30d": ratio_30d, "turnover": turnover}

        # vol_z 計算
        vol_z = None
        if len(volumes) >= 24 * 7:
            pump24 = sum(v[1] for v in volumes[-24:])
            lookback_vols = [v[1] for v in volumes[:-24]]
            if len(lookback_vols) >= 24:
                rolling_sums = []
                for i in range(0, len(lookback_vols) - 24 + 1):
                    rolling_sums.append(sum(lookback_vols[i:i + 24]))
                if rolling_sums:
                    mean = sum(rolling_sums) / len(rolling_sums)
                    var = sum((x - mean) ** 2 for x in rolling_sums) / len(rolling_sums)
                    std = var ** 0.5
                    if std > 0:
                        vol_z = (pump24 - mean) / std

        # ★ 直近7日リターンチェック (s-260517 専用フィルタ)
        ret_7d = None
        if len(prices) >= 7*24 + 1:
            p_now = prices[-1][1]
            p_7d = prices[-1 - 7*24][1]
            if p_7d > 0:
                ret_7d = p_now / p_7d - 1
                if ret_7d > RET7_MAX:
                    return False, f"ret_7d {ret_7d*100:.1f}% > {RET7_MAX*100:.0f}%", {"ratio_30d": ratio_30d, "turnover": turnover, "ret_7d": ret_7d}

        # vol_z >= 0 チェック (NaNは通す)
        if vol_z is not None and vol_z < VOL_Z_MIN:
            return False, f"vol_z {vol_z:.2f} < {VOL_Z_MIN}", {"ratio_30d": ratio_30d, "turnover": turnover, "vol_z": vol_z, "ret_7d": ret_7d}

        # pump_start を特定(ch24が初めて 50% を越えた瞬間)
        pump_start = None
        for i in range(len(prices) - 1, 23, -1):
            cur = prices[i][1]
            ago = prices[i - 24][1]
            if ago <= 0:
                continue
            ch = cur / ago - 1
            if i - 1 < 24:
                if ch > PUMP_DETECT_THRESHOLD:
                    pump_start = datetime.fromtimestamp(prices[i][0]/1000, tz=timezone.utc)
                continue
            prev_ago = prices[i - 1 - 24][1]
            if prev_ago <= 0:
                continue
            prev_ch = prices[i - 1][1] / prev_ago - 1
            if prev_ch <= PUMP_DETECT_THRESHOLD and ch > PUMP_DETECT_THRESHOLD:
                pump_start = datetime.fromtimestamp(prices[i][0]/1000, tz=timezone.utc)
                break
        if pump_start is None:
            pump_start = datetime.fromtimestamp(prices[-1][0]/1000, tz=timezone.utc)

        return True, "OK", {
            "ratio_30d": ratio_30d, "turnover": turnover, "vol_z": vol_z, "ret_7d": ret_7d, "pump_start": pump_start
        }
    except Exception as e:
        return False, f"APIエラー: {e}", {}


def fetch_current_price(coin_id):
    """エントリー時の現在価格を取得"""
    try:
        data = cg_get(f"/coins/{urllib.parse.quote(coin_id)}/market_chart", {
            "vs_currency": "usd", "days": 1
        })
        prices = data.get("prices") or []
        if not prices:
            return None
        return prices[-1][1]
    except Exception as e:
        log(f"  fetch_current_price エラー: {e}")
        return None


# ============= Discord 通知 =============
def discord_notify(content, embeds=None):
    if not DISCORD_WEBHOOK:
        log("WARN: DISCORD_WEBHOOK_S_260517 未設定、通知スキップ")
        return False
    payload = {"content": content}
    if embeds:
        payload["embeds"] = embeds
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(DISCORD_WEBHOOK, data=body, headers={
        "Content-Type": "application/json",
        "User-Agent": "DiscordBot (s-260517, 1.0)"
    })
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            log(f"  HTTP {r.status} - 通知成功")
            return r.status in (200, 204)
    except urllib.error.HTTPError as e:
        try:
            err_body = e.read().decode("utf-8", errors="replace")[:200]
        except Exception:
            err_body = ""
        log(f"  Discord通知失敗: HTTP {e.code} {e.reason} | body={err_body}")
        return False
    except Exception as e:
        log(f"  Discord通知失敗: {type(e).__name__}: {e}")
        return False


def build_entry_embed(schedule, entry_price):
    """エントリー時刻の通知 — entry+SL%, TP%, hold時間を表示"""
    sym = schedule["symbol"]
    cid = schedule["coin_id"]
    rank = schedule.get("rank")
    pump_start = from_iso(schedule["pump_time"])
    entry_time = from_iso(schedule["entry_time"])
    exit_time_obj = entry_time + timedelta(hours=HOLD_HOURS)
    delta_min = (now_utc() - entry_time).total_seconds() / 60
    position_usd = ACCOUNT_BALANCE * POSITION_PCT

    # SL = entry * (1 + SL_PCT), TP = entry * (1 - TP_PCT)
    stop_price = entry_price * (1 + SL_PCT)
    tp_price = entry_price * (1 - TP_PCT)
    chart_url = f"https://www.coingecko.com/ja/coins/{cid}"
    mexc_url = f"https://futures.mexc.com/exchange/{sym}_USDT"

    market_cap = schedule.get("market_cap")
    mc_str = f"${market_cap/1e9:.2f}B" if market_cap and market_cap >= 1e9 else (f"${market_cap/1e6:.1f}M" if market_cap else "—")
    rank_label = f"#{rank}" if rank else "—"

    return {
        "title": f"⚡ s-260517 ENTRY [{sym}] — クリックでチャート確認",
        "url": chart_url,
        "description": (
            f"**いますぐ MEXC でショート発注** | {fmt_jst(entry_time)} ({int(delta_min):+}分)\n"
            f"📊 [**CoinGecko**]({chart_url}) | ⚡ [**MEXC**]({mexc_url})"
        ),
        "color": 0x1abc9c,  # 水色 (即時スキャルプ系)
        "fields": [
            {"name": "🎯 銘柄", "value": f"`{sym}USDT` (Perpetual)", "inline": True},
            {"name": "ランク", "value": rank_label, "inline": True},
            {"name": "時価総額", "value": mc_str, "inline": True},
            {"name": "ポジション", "value": f"**${position_usd:.2f} USDT** ({int(POSITION_PCT*100)}%)", "inline": True},
            {"name": "🟡 エントリー価格", "value": f"`${entry_price:.8f}`", "inline": True},
            {"name": "📈 24h変動率", "value": f"+{schedule['ch24_at_pump']:.1f}%", "inline": True},
            {"name": f"🛑 ストップロス(+{int(SL_PCT*100)}%)", "value": f"`${stop_price:.8f}`", "inline": True},
            {"name": f"💰 利確指値 (-{int(TP_PCT*100)}%)", "value": f"`${tp_price:.8f}`", "inline": True},
            {"name": f"⏰ 強制決済時刻 ({HOLD_HOURS}h後)", "value": fmt_jst(exit_time_obj), "inline": True},
            {"name": "✅ 執行チェックリスト",
             "value": (
                f"**① MEXC で `{sym}USDT` を開く** [→ クリック]({mexc_url})\n"
                f"**② USDT-M Perpetual** を選択\n"
                f"**③ ⚠ ショート(Short/Sell)を選択**\n"
                f"**④ レバレッジを 1x に設定**\n"
                f"**⑤ 証拠金 `${position_usd:.2f} USDT` を入力**\n"
                f"**⑥ 成行注文(Market)で発注**\n"
                f"**⑦ ストップロス指値: `${stop_price:.8f}` (+{int(SL_PCT*100)}%)**\n"
                f"**⑧ 利確指値: `${tp_price:.8f}` (-{int(TP_PCT*100)}%)**\n"
                f"**⑨ カレンダーに {fmt_jst(exit_time_obj)} を登録**"
             ), "inline": False},
        ],
        "footer": {"text": f"s-260517 / SL=entry+{int(SL_PCT*100)}% / TP=-{int(TP_PCT*100)}% / hold={HOLD_HOURS}h / wait={WAIT_HOURS}h"},
        "timestamp": now_utc().isoformat()
    }


def build_exit_embed(schedule):
    sym = schedule["symbol"]
    cid = schedule["coin_id"]
    chart_url = f"https://www.coingecko.com/ja/coins/{cid}"
    return {
        "title": f"⏰ s-260517 強制決済 [{sym}]",
        "url": chart_url,
        "description": f"保有 {HOLD_HOURS}時間経過。**成行で決済**してください。\n📊 [**CoinGecko**]({chart_url})",
        "color": 0x1abc9c,
        "fields": [
            {"name": "銘柄", "value": f"{sym}USDT", "inline": True},
            {"name": "エントリー時刻", "value": fmt_jst(from_iso(schedule['entry_time'])), "inline": True},
            {"name": "決済時刻", "value": fmt_jst(now_utc()), "inline": True},
        ],
        "footer": {"text": f"s-260517 / {HOLD_HOURS}h 経過"}
    }


# ============= 状態管理 =============
def is_alerted_recently(coin_id, alerted):
    last = alerted.get(coin_id)
    if not last:
        return False
    return (now_utc() - from_iso(last)) < timedelta(hours=DEDUP_HOURS)


def cleanup_state(state):
    cutoff = now_utc() - timedelta(hours=DEDUP_HOURS * 2)
    state["alerted"] = {cid: ts for cid, ts in (state.get("alerted") or {}).items() if from_iso(ts) > cutoff}
    cutoff_sched = now_utc() - timedelta(hours=24)
    state["scheduled"] = [s for s in (state.get("scheduled") or []) if from_iso(s["exit_time"]) > cutoff_sched]
    return state


# ============= メインフロー =============
def detect_phase(state):
    """Phase 1: s-260517 条件を満たす銘柄を検出"""
    log(f"Phase 1: top1000 取得...")
    coins = fetch_top_coins(1000)
    log(f"  {len(coins)} 銘柄取得")

    candidates = [c for c in coins if basic_filter(c)[0]]
    log(f"  候補(ch24/rank通過): {len(candidates)}件")

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
            "market_cap": c.get("market_cap"),
            "ch24_at_pump": c.get("price_change_percentage_24h"),
            "ratio_30d": info.get("ratio_30d"),
            "turnover": info.get("turnover"),
            "vol_z": info.get("vol_z"),
            "ret_7d": info.get("ret_7d"),
            "pump_time": to_iso(pump_start),
            "entry_time": to_iso(entry_time),
            "exit_time": to_iso(exit_time),
            "entry_notified": False,
            "exit_notified": False,
        }
        state["scheduled"].append(sched)
        state["alerted"][c["id"]] = to_iso(now_utc())
        log(f"  ✓ {c['symbol'].upper()} 登録: entry予定 {fmt_jst(entry_time)} / exit予定 {fmt_jst(exit_time)}")
        new_count += 1
    return new_count


def schedule_phase(state):
    """Phase 2: スケジュール処理 — エントリー通知と決済通知"""
    log("Phase 2: スケジュール処理...")
    counts = {"entry": 0, "exit": 0}
    now = now_utc()

    for s in state["scheduled"]:
        sym = s["symbol"]
        cid = s["coin_id"]
        entry_t = from_iso(s["entry_time"])
        exit_t = from_iso(s["exit_time"])

        # === エントリー通知 (wait後の窓内 or 既に過ぎてる)===
        if not s.get("entry_notified"):
            mins_until = (entry_t - now).total_seconds() / 60
            # entry_time から ±60分 以内 → 通知 (検知時点で既に過ぎてる場合も含む)
            if mins_until <= ENTRY_WINDOW_MIN:
                cur_price = fetch_current_price(cid)
                if cur_price is None:
                    log(f"  ⚠ {sym} 現在価格取得失敗、保留")
                    continue
                embed = build_entry_embed(s, cur_price)
                discord_notify(f"⚡ **s-260517 ENTRY [{sym}]** いますぐ発注!", embeds=[embed])
                s["entry_notified"] = True
                s["entry_price"] = cur_price
                counts["entry"] += 1
                log(f"  🆕 entry notify: {sym} price=${cur_price:.8f}")
                time.sleep(INTERVAL_SEC)

        # === 決済通知 ===
        if s.get("entry_notified") and not s.get("exit_notified"):
            mins_until = abs((exit_t - now).total_seconds() / 60)
            if mins_until <= EXIT_WINDOW_MIN:
                embed = build_exit_embed(s)
                discord_notify(f"⏰ **s-260517 決済 [{sym}]** {HOLD_HOURS}h 経過、成行決済を!", embeds=[embed])
                s["exit_notified"] = True
                counts["exit"] += 1
                log(f"  🏁 exit notify: {sym}")

    return counts


def main():
    log(f"=== {STRATEGY_LABEL} 監視BOT 起動 / プラン={CG_PLAN} ===")
    log(f"  戦略: {STRATEGY_NOTE}")
    log(f"  フィルタ: ch24 {int(CH24_MIN*100)}-{int(CH24_MAX*100)}%, rank {RANK_MIN}-{RANK_MAX}, 30d≤{RATIO_30D_MAX}倍, turnover≥{TURNOVER_MIN*100}%, vol_z≥{VOL_Z_MIN}")
    log(f"  挙動: TP=-{int(TP_PCT*100)}% / SL=+{int(SL_PCT*100)}% / hold={HOLD_HOURS}h / wait={WAIT_HOURS}h")

    if not CG_API_KEY:
        log("ERROR: CG_API_KEY 未設定")
        sys.exit(1)
    if not DISCORD_WEBHOOK:
        log("WARN: DISCORD_WEBHOOK_S_260517 未設定(通知なし)")

    # 接続テスト
    if os.environ.get("TEST_DISCORD_S_260517", "").strip() == "1":
        log("🧪 TEST_DISCORD モード:接続テスト送信中...")
        ok = discord_notify(
            f"🧪 **s-260517 接続テスト**\n"
            f"GitHub Actions から正常に到達。\n"
            f"戦略: {STRATEGY_NOTE}\n"
            f"検知条件: ch24 {int(CH24_MIN*100)}-{int(CH24_MAX*100)}% / rank {RANK_MIN}-{RANK_MAX}\n"
            f"挙動: TP=-{int(TP_PCT*100)}% / SL=+{int(SL_PCT*100)}% / hold={HOLD_HOURS}h / wait={WAIT_HOURS}h\n"
            f"確認後、Variable の TEST_DISCORD_S_260517 を削除してください。"
        )
        log(f"テスト結果: {'✅ 成功' if ok else '❌ 失敗'}")
        return

    state = load_state()
    state = cleanup_state(state)

    new_detected = detect_phase(state)
    counts = schedule_phase(state)

    save_state(state)
    log(f"=== 完了:新規検知 {new_detected}件 / エントリー通知 {counts['entry']} / 決済通知 {counts['exit']} / 追跡中 {len(state['scheduled'])}件 ===")


if __name__ == "__main__":
    main()
