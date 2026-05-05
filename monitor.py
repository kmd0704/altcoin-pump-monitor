"""
Altcoin Pump Monitor — 7戦略統合版
GitHub Actions で 1時間ごとに実行

戦略バージョン(全戦略を同時に評価):
  v3.4   — TP=30% / 全銘柄(rank 250-1000) / 3h待ち
  v3.4m  — TP=30% / モッピーさん版(rank 250-900) / 3h待ち
  v3.5   — TP=30% / vol_z≥0厳選 / 3h待ち
  v3.5m  — TP=30% / vol_z厳選+モッピーさん版 / 3h待ち
  v3.6 ★ — TP=50% / リターン最大化 / 3h待ち / 推奨デフォルト
  v3.6m  — TP=50% / モッピーさん版(rank 250-900) / 3h待ち
  s     — TP=20% / 即時エントリー / 0h待ち / 短命pump向け、勝率93%

通知設計:
  s版マッチ → ⚡ 即時エントリー通知(検知と同時)
  通常版マッチ → 🚨 3時間後エントリー通知
  192h後 → ⏰ 決済通知

機能:
  Phase 1 急騰検出
    - top1000 取得 → 24h+50%急騰候補抽出
    - deep_check で vol_z / 30日比 / turnover 取得
    - 各候補を 5戦略で評価 → 1つ以上マッチで schedule 登録
    - s版マッチ時点で即時エントリー通知
    - 通常版マッチは pump_start + 3h でエントリー通知

  Phase 2 スケジュール処理
    - 既存 schedule を巡回
    - 検知通知/エントリー通知/決済通知 を時刻に応じて送信(各コイン1通ずつ)

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

# ============= 7戦略の定義 =============
# 通常3つ(v3.4 / v3.5 / v3.6)+ それぞれの m(モッピーさん版)+ s版(即時統合)
# wait_hours: エントリーまでの待ち時間(0=即時, 3=従来)
STRATEGIES = [
    {"id": "v34",  "label": "v3.4",    "tp": 0.30, "wait_hours": 3, "max_rank": 1000, "vol_z_floor": None, "note": "TP30% / 全銘柄 / 3h待ち"},
    {"id": "v34m", "label": "v3.4m",   "tp": 0.30, "wait_hours": 3, "max_rank": 900,  "vol_z_floor": None, "note": "TP30% / モッピーさん版(rank 250-900)"},
    {"id": "v35",  "label": "v3.5",    "tp": 0.30, "wait_hours": 3, "max_rank": 1000, "vol_z_floor": 0.0,  "note": "TP30% / vol_z厳選 / 3h待ち"},
    {"id": "v35m", "label": "v3.5m",   "tp": 0.30, "wait_hours": 3, "max_rank": 900,  "vol_z_floor": 0.0,  "note": "TP30% / vol_z厳選+モッピーさん版"},
    {"id": "v36",  "label": "v3.6 ★", "tp": 0.50, "wait_hours": 3, "max_rank": 1000, "vol_z_floor": None, "note": "TP50% / リターン最大化(推奨)"},
    {"id": "v36m", "label": "v3.6m",   "tp": 0.50, "wait_hours": 3, "max_rank": 900,  "vol_z_floor": None, "note": "TP50% / モッピーさん版(rank 250-900)"},
    {"id": "s",    "label": "s",       "tp": 0.20, "wait_hours": 0, "max_rank": 1000, "vol_z_floor": None, "note": "TP20% / 即時エントリー(勝率93%)"},
]
STRATEGY_BY_ID = {s["id"]: s for s in STRATEGIES}

# 共通パラメータ
PUMP_THRESHOLD = 0.50         # 24h +50%
MAX_CH24 = 2.00                # 24h +200% 以下
MIN_RANK = 250                 # 全戦略共通の下限
WIDEST_MAX_RANK = 1000         # Phase1で広く取る上限(各戦略のmax_rankは別途判定)
MAX_30D_RATIO = 2.0            # 30日で2倍超は除外
MIN_TURNOVER = 0.01            # turnover ≥ 1%
WAIT_HOURS = 3                 # 急騰検知後 3h でエントリー
HOLD_HOURS = 192               # 保有 192h(8日)
STOP_MULT = 1.60               # ストップ = Peak × 1.60
DEDUP_HOURS = 48               # 同銘柄の再アラート抑止

# モッピーさん警告閾値(全戦略で表示)
MOPPY_WARNING_RANK = 900

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
    """共通の絞り込み(全戦略の最広上限 = rank 1000)"""
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
    if rank < MIN_RANK or rank > WIDEST_MAX_RANK:
        return False, f"rank {rank}"
    if ch24 / 100 < PUMP_THRESHOLD:
        return False, f"ch24 {ch24:.1f}%"
    if ch24 / 100 > MAX_CH24:
        return False, f"ch24 {ch24:.1f}% > max"
    return True, "OK"


def deep_check(coin):
    """30日前比 + turnover + vol_z + 急騰開始時刻"""
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

        # vol_z 計算 — 直近24h累計 vs 過去30日 rolling 24h sum の z-score
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
            "ratio_30d": ratio_30d, "turnover": turnover, "vol_z": vol_z, "pump_start": pump_start
        }
    except Exception as e:
        return False, f"APIエラー: {e}", {}


def evaluate_strategies(coin, deep_info):
    """各戦略について {match: bool, reason: str} を返す。
    リターン: [{id, label, tp, match, reason}, ...]
    """
    rank = coin.get("market_cap_rank")
    vol_z = deep_info.get("vol_z")
    results = []
    for s in STRATEGIES:
        match = True
        reason = "OK"
        if rank is None or rank > s["max_rank"]:
            match = False
            reason = f"rank {rank} > {s['max_rank']}"
        elif s["vol_z_floor"] is not None and vol_z is not None and vol_z < s["vol_z_floor"]:
            match = False
            reason = f"vol_z {vol_z:.2f} < {s['vol_z_floor']}"
        results.append({
            "id": s["id"], "label": s["label"], "tp": s["tp"],
            "match": match, "reason": reason, "note": s["note"],
        })
    return results


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
        "User-Agent": "DiscordBot (https://github.com/kmd0704/altcoin-pump-monitor, 4.0)"
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


def fmt_strategy_badges(evals, with_tp=False):
    """戦略マッチ状況のバッジテキスト生成"""
    lines = []
    for e in evals:
        if e["match"]:
            mark = "✅"
            tp_part = f" → 利確 -{int(e['tp']*100)}%" if with_tp else ""
            lines.append(f"{mark} **{e['label']}**({e['note']}){tp_part}")
        else:
            lines.append(f"❌ {e['label']} — {e['reason']}")
    return "\n".join(lines)


def build_detection_embed(coin, deep_info, schedule):
    """検知時の通知 — 4戦略のマッチ状況を1通にまとめて表示"""
    sym = coin["symbol"].upper()
    cid = coin["id"]
    name = coin.get("name", "")
    rank = coin["market_cap_rank"]
    ch24 = coin["price_change_percentage_24h"]
    market_cap = schedule.get("market_cap")
    pump_start = from_iso(schedule["pump_time"])
    entry_time = from_iso(schedule["entry_time"])
    until_entry = (entry_time - now_utc()).total_seconds() / 60
    chart_url = f"https://www.coingecko.com/ja/coins/{cid}"

    if market_cap:
        mc_str = f"${market_cap/1e9:.2f}B" if market_cap >= 1e9 else f"${market_cap/1e6:.1f}M"
    else:
        mc_str = "—"

    vol_z = deep_info.get("vol_z")
    vol_z_str = f"{vol_z:+.2f}" if vol_z is not None else "—"

    # schedule に保存された evals を取得(なければ再評価)
    evals = schedule.get("evals") or evaluate_strategies(coin, deep_info)
    matched_count = sum(1 for e in evals if e["match"])

    # rank≥900 モッピーさん警告
    warning_field = None
    if rank >= MOPPY_WARNING_RANK:
        warning_field = {
            "name": "⚠️ モッピーさん 警告ゾーン",
            "value": "rank 900以上は **継続pumpリスク** あり。チャートで戻り兆候を必ず目視確認してから判断推奨。\n(※ v3.8 ではそもそも対象外)",
            "inline": False
        }

    fields = [
        {"name": "急騰開始時刻", "value": fmt_jst(pump_start), "inline": True},
        {"name": "★ エントリー予定時刻 ★", "value": fmt_jst(entry_time), "inline": True},
        {"name": "あと", "value": f"{int(until_entry)}分後", "inline": True},
        {"name": "24h急騰", "value": f"+{ch24:.1f}%", "inline": True},
        {"name": "時価総額", "value": mc_str, "inline": True},
        {"name": "30日前比", "value": f"{deep_info.get('ratio_30d',0):.2f}倍", "inline": True},
        {"name": "turnover", "value": f"{(deep_info.get('turnover') or 0)*100:.2f}%", "inline": True},
        {"name": "vol_z", "value": vol_z_str, "inline": True},
        {"name": "ランク", "value": f"#{rank}" + (" ⚠️" if rank >= MOPPY_WARNING_RANK else ""), "inline": True},
        {"name": f"🎯 戦略マッチ状況 ({matched_count}/{len(STRATEGIES)})",
         "value": fmt_strategy_badges(evals, with_tp=False),
         "inline": False},
    ]
    if warning_field:
        fields.append(warning_field)
    fields.append({"name": "📌 行動",
         "value": (
            f"・**今すぐ何もしない**\n"
            f"・エントリー予定時刻が近づいたら自動で再通知(各戦略の TP価格を表示)\n"
            f"・[CoinGecko チャートを確認]({chart_url}) しておくと吉"
         ), "inline": False})

    embed = {
        "title": f"🟡 急騰検知 [{sym}] — クリックでチャート確認",
        "url": chart_url,
        "description": f"**{name}** (rank {rank}) — 現時点ではまだエントリーしません\n📊 [**CoinGecko でチャートを開く**]({chart_url})",
        "color": 0xf0b648,
        "fields": fields,
        "footer": {"text": f"{len(STRATEGIES)}戦略同時評価 / 検知 → 3時間様子見 → 推奨時刻でエントリー"},
        "timestamp": now_utc().isoformat()
    }
    return embed


def build_entry_embed(schedule, peak, cur_price, mode="immediate"):
    """エントリー時刻の通知。
    mode="immediate" → s版用(即時エントリー、TP-20%)
    mode="normal"    → 通常版用(3h待ち、TP-30%/-50%)
    """
    sym = schedule["symbol"]
    cid = schedule["coin_id"]
    rank = schedule.get("rank")
    pump_start = from_iso(schedule["pump_time"])
    if mode == "immediate":
        entry_time = pump_start
    else:
        entry_time = from_iso(schedule["entry_time_normal"])
    exit_time_obj = entry_time + timedelta(hours=HOLD_HOURS)
    delta_min = (now_utc() - entry_time).total_seconds() / 60
    position_usd = ACCOUNT_BALANCE * POSITION_PCT  # USD扱い

    stop_price = peak * STOP_MULT
    stop_dist = (stop_price / cur_price - 1) * 100
    chart_url = f"https://www.coingecko.com/ja/coins/{cid}"
    mexc_url = f"https://futures.mexc.com/exchange/{sym}_USDT"

    market_cap = schedule.get("market_cap")
    if market_cap:
        mc_str = f"${market_cap/1e9:.2f}B" if market_cap >= 1e9 else f"${market_cap/1e6:.1f}M"
    else:
        mc_str = "—"
    rank_label = f"#{rank}" + (" ⚠️" if rank and rank >= MOPPY_WARNING_RANK else "")

    # 該当戦略を mode で絞り込み(s版 = wait_hours=0、通常 = wait_hours=3)
    evals = schedule.get("evals") or []
    target_wait = 0 if mode == "immediate" else 3
    relevant_strats = [s for s in STRATEGIES if s["wait_hours"] == target_wait]
    relevant_ids = {s["id"] for s in relevant_strats}

    # 戦略別 TP価格(対象戦略のみ表示。他mode戦略はグレーアウトで参考表示)
    tp_lines = []
    matched_relevant = [e for e in evals if e["match"] and e["id"] in relevant_ids]
    for e in matched_relevant:
        tp_price = cur_price * (1 - e["tp"])
        tp_lines.append(f"✅ **{e['label']}** TP-{int(e['tp']*100)}% → 💰 `${tp_price:.8f}` (利確指値)")
    skipped_relevant = [e for e in evals if not e["match"] and e["id"] in relevant_ids]
    for e in skipped_relevant:
        tp_lines.append(f"❌ {e['label']} — {e['reason']}")
    tp_block = "\n".join(tp_lines) if tp_lines else "(該当戦略なし)"

    # 推奨デフォルト
    if mode == "immediate":
        default_id = "s"
        title_emoji = "⚡"
        title_word = "即時エントリー(s版)"
        color = 0x4fc3f7
    else:
        default_id = "v36"
        title_emoji = "🚨"
        title_word = "3時間後エントリー(通常版)"
        color = 0xe06c6c
    default_strat = next((e for e in matched_relevant if e["id"] == default_id), None) or (matched_relevant[0] if matched_relevant else None)
    default_tp = default_strat["tp"] if default_strat else 0.20
    default_tp_price = cur_price * (1 - default_tp)

    matched_count = len(matched_relevant)
    relevant_total = len(relevant_strats)

    embed = {
        "title": f"{title_emoji} {title_word} [{sym}] — クリックでチャート確認",
        "url": chart_url,
        "description": (
            f"**いますぐ MEXC でショート発注** | {fmt_jst(entry_time)}({int(delta_min):+}分)\n"
            + (f"⚠️ **rank {rank}(900+)はモッピーさん警告ゾーン**:継続pumpリスク、目視確認必須\n" if rank and rank >= MOPPY_WARNING_RANK else "")
            + f"📊 [**CoinGecko**]({chart_url}) | ⚡ [**MEXC**]({mexc_url})"
        ),
        "color": color,
        "fields": [
            {"name": "🎯 銘柄", "value": f"`{sym}USDT` (Perpetual)", "inline": True},
            {"name": "ランク", "value": rank_label, "inline": True},
            {"name": "時価総額", "value": mc_str, "inline": True},
            {"name": "ポジション", "value": f"**${position_usd:.2f} USDT** ({int(POSITION_PCT*100)}%)", "inline": True},
            {"name": "🟢 Peak価格", "value": f"`${peak:.8f}`", "inline": True},
            {"name": "🟡 エントリー価格", "value": f"`${cur_price:.8f}`", "inline": True},
            {"name": "🛑 ストップロス指値(共通)", "value": f"`${stop_price:.8f}` ({stop_dist:+.1f}%)", "inline": True},
            {"name": "⏰ 強制決済時刻(192h後)", "value": fmt_jst(exit_time_obj), "inline": True},
            {"name": f"💰 戦略別 利確指値(該当 {matched_count}/{relevant_total})",
             "value": tp_block, "inline": False},
            {"name": "✅ 執行チェックリスト",
             "value": (
                f"**① MEXC で `{sym}USDT` を開く** [→ クリック]({mexc_url})\n"
                f"**② USDT-M Perpetual** を選択(現物ではない)\n"
                f"**③ ⚠ ショート(Short/Sell)を選択** ← 一番大事\n"
                f"**④ レバレッジを 1x に設定**(必須)\n"
                f"**⑤ 証拠金 `${position_usd:.2f} USDT` を入力**\n"
                f"**⑥ 成行注文(Market)で発注**\n"
                f"**⑦ ストップロス指値: `${stop_price:.8f}`**\n"
                f"**⑧ 利確指値: `${default_tp_price:.8f}`** (推奨 `{default_id}`/TP-{int(default_tp*100)}%)\n"
                f"  └ 別戦略の値を使いたい時は上の一覧から選択\n"
                f"**⑨ カレンダーに {fmt_jst(exit_time_obj)} を登録**"
             ), "inline": False},
            {"name": "🔗 取引所",
             "value": (
                f"⚡ [**MEXC(推奨)**]({mexc_url}) | "
                f"[Bybit](https://www.bybit.com/en/trade/usdt/{sym}USDT) | "
                f"[Binance](https://www.binance.com/en/futures/{sym}USDT)"
             ), "inline": False},
        ],
        "footer": {"text": f"{title_word} / Stop=Peak×1.60 / Hold={HOLD_HOURS}h"},
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
        "description": f"保有{HOLD_HOURS}時間経過。**成行で決済**してください。\n📊 [**CoinGecko でチャート確認**]({chart_url})",
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
        "footer": {"text": f"{len(STRATEGIES)}戦略同時評価 / {HOLD_HOURS}h 経過"}
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
    """Phase 1: 新規急騰検出 — 4戦略を同時評価"""
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

        # 4戦略を同時評価
        evals = evaluate_strategies(c, info)
        matched = [e for e in evals if e["match"]]
        if not matched:
            log(f"  ✗ {c['symbol'].upper()} 全戦略不該当: {[e['reason'] for e in evals]}")
            continue

        pump_start = info.get("pump_start", now_utc())
        entry_time_normal = pump_start + timedelta(hours=3)   # 通常版用
        # exit_time は通常版基準(s版は entry が早いので少し早く出るが192h固定で扱う)
        exit_time = entry_time_normal + timedelta(hours=HOLD_HOURS)
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
            "price_at_detection": c.get("current_price"),
            "pump_time": to_iso(pump_start),
            "entry_time_normal": to_iso(entry_time_normal),
            "entry_time": to_iso(entry_time_normal),  # 後方互換
            "exit_time": to_iso(exit_time),
            "evals": evals,
            "detection_notified": False,
            "immediate_entry_notified": False,   # s版用エントリー通知
            "normal_entry_notified": False,      # 通常版エントリー通知(旧 entry_notified)
            "entry_notified": False,             # 後方互換
            "exit_notified": False,
        }
        state["scheduled"].append(sched)
        state["alerted"][c["id"]] = to_iso(now_utc())
        s_strats = [e for e in matched if e["id"] == "s"]
        n_strats = [e for e in matched if e["id"] != "s"]
        log(f"  ✓ {c['symbol'].upper()} 登録: s版マッチ {len(s_strats)} / 通常版マッチ {len(n_strats)} / 通常エントリー予定 {fmt_jst(entry_time_normal)}")
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
        entry_t_normal = from_iso(s.get("entry_time_normal") or s["entry_time"])
        exit_t = from_iso(s["exit_time"])
        evals = s.get("evals") or []
        # s版マッチ / 通常版マッチを判定
        has_s_match = any(e["match"] and STRATEGY_BY_ID.get(e["id"], {}).get("wait_hours") == 0 for e in evals)
        has_normal_match = any(e["match"] and STRATEGY_BY_ID.get(e["id"], {}).get("wait_hours") == 3 for e in evals)

        # === ⚡ 即時エントリー通知(s版用、検知と同時)===
        if has_s_match and not s.get("immediate_entry_notified"):
            pump_start = from_iso(s["pump_time"])
            peak, cur = fetch_peak_and_price(cid, pump_start, now)
            if peak is not None and cur is not None:
                embed = build_entry_embed(s, peak, cur, mode="immediate")
                discord_notify(f"⚡ **即時エントリー [{sym}]** s版用、いますぐ発注!", embeds=[embed])
                s["immediate_entry_notified"] = True
                s["s_peak_price"] = peak
                s["s_entry_price"] = cur
                notified_count["entry"] += 1
                log(f"  ⚡ s版 entry notify: {sym} peak=${peak:.8f} entry=${cur:.8f}")
                time.sleep(INTERVAL_SEC)
            else:
                log(f"  ⚠ {sym} s版 peak/price 取得失敗、保留")

        # === 🟡 検知通知(s版マッチが無い場合のみ。s版マッチがあれば即時エントリーで代替) ===
        if not has_s_match and not s.get("detection_notified"):
            mins_until_entry = (entry_t_normal - now).total_seconds() / 60
            if 0 <= mins_until_entry <= 30:
                s["detection_notified"] = True
            else:
                fake_coin = {"symbol": sym, "name": s["name"], "market_cap_rank": s["rank"],
                             "price_change_percentage_24h": s["ch24_at_pump"], "id": cid}
                deep = {"ratio_30d": s["ratio_30d"], "turnover": s["turnover"], "vol_z": s.get("vol_z")}
                embed = build_detection_embed(fake_coin, deep, s)
                if mins_until_entry < 0:
                    msg = f"⚠ **検知 [{sym}]**(エントリー時刻 {abs(int(mins_until_entry))}分前に通過済)"
                else:
                    msg = f"🟡 **検知 [{sym}]** 通常版エントリー予定 {fmt_jst(entry_t_normal)}"
                discord_notify(msg, embeds=[embed])
                s["detection_notified"] = True
                notified_count["detect"] += 1
                log(f"  📨 detection notify: {sym} (mins_until={int(mins_until_entry)})")
        elif has_s_match and not s.get("detection_notified"):
            # s版でカバー済みなので detection_notified=True にしておく
            s["detection_notified"] = True

        # === 🚨 通常版エントリー通知(3h待ち、通常版マッチがある場合のみ) ===
        if has_normal_match and not s.get("normal_entry_notified") and not s.get("entry_notified"):
            mins_until = abs((entry_t_normal - now).total_seconds() / 60)
            if mins_until <= ENTRY_WINDOW_MIN:
                pump_start = from_iso(s["pump_time"])
                peak, cur = fetch_peak_and_price(cid, pump_start, now)
                if peak is None or cur is None:
                    log(f"  ⚠ {sym} 通常版 peak/price 取得失敗、保留")
                    continue
                embed = build_entry_embed(s, peak, cur, mode="normal")
                discord_notify(f"🚨 **3時間後エントリー [{sym}]** 通常版用、いますぐ発注!", embeds=[embed])
                s["normal_entry_notified"] = True
                s["entry_notified"] = True   # 後方互換
                s["peak_price"] = peak
                s["entry_price"] = cur
                notified_count["entry"] += 1
                log(f"  🚨 通常版 entry notify: {sym} peak=${peak:.8f} entry=${cur:.8f}")
                time.sleep(INTERVAL_SEC)

        # === ⏰ 決済通知 ===
        already_entered = s.get("immediate_entry_notified") or s.get("normal_entry_notified") or s.get("entry_notified")
        if already_entered and not s.get("exit_notified"):
            mins_until = abs((exit_t - now).total_seconds() / 60)
            if mins_until <= EXIT_WINDOW_MIN:
                embed = build_exit_embed(s)
                discord_notify(f"⏰ **決済時刻 [{sym}]** {HOLD_HOURS}h 経過、成行決済を!", embeds=[embed])
                s["exit_notified"] = True
                notified_count["exit"] += 1
                log(f"  🏁 exit notify: {sym}")

    return notified_count


def main():
    strat_summary = " / ".join(s["label"] for s in STRATEGIES)
    log(f"=== Altcoin Pump Monitor 起動 / {len(STRATEGIES)}戦略同時評価: {strat_summary} / プラン={CG_PLAN} ===")
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
            f"GitHub Actions から正常に到達しました。\n"
            f"{len(STRATEGIES)}戦略同時評価モード: {strat_summary} / プラン={CG_PLAN}\n"
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
