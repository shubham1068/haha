"""
╔══════════════════════════════════════════════════════════════╗
║          NIFTY OPTIONS BACKTEST ENGINE  v3.0                ║
║          Industry-Grade | Bulk Fetch | Full Logging         ║
╚══════════════════════════════════════════════════════════════╝

Architecture:
  1. Config       — saari settings ek jagah
  2. Logger        — file + console dono
  3. DB Layer      — connection + bulk fetchers
  4. Strategy Zone — sirf yahan changes karo
  5. Engine        — trade loop, reentry, PnL
  6. Reporter      — summary + CSV save

CSV Format (trades_input.csv):
  Expiry Date
  2025-01-16
  2025-01-23
  ...
"""

# ── stdlib ────────────────────────────────────────────────────
import os
import sys
import logging
import traceback
from datetime import datetime, time, timedelta
from pathlib import Path

# ── third-party ───────────────────────────────────────────────
import pandas as pd
from pymongo import MongoClient
from urllib.parse import quote_plus


# ══════════════════════════════════════════════════════════════
# 1. CONFIG  ← sirf yahan changes karo
# ══════════════════════════════════════════════════════════════

class Config:
    # ── paths ──────────────────────────────────────────────────
    BASE_DIR      = Path(os.getcwd())
    CSV_INPUT     = BASE_DIR / "trades_input.csv"
    RESULT_CSV    = BASE_DIR / "result.csv"
    LOG_FILE      = BASE_DIR / "backtest.log"

    # ── trade params ───────────────────────────────────────────
    SLIPPAGE      = 0.008   # 0.8% per leg per transaction
    LOT_SIZE      = 25
    NUM_LOTS      = 1
    FIXED_SL_PERC = 1.20    # 20% over combined short premium

    # ── market hours ───────────────────────────────────────────
    MARKET_OPEN   = time(9, 20)
    MARKET_CLOSE  = time(15, 30)

    # ── MongoDB ────────────────────────────────────────────────
    MONGO_USER    = "global_readonly"
    MONGO_PASS    = "Strategy_reader@1234509"
    MONGO_HOST    = "192.168.12.160"
    MONGO_PORT    = 27017
    DB_OPTIONS    = "strategy_historical"
    DB_SPOT       = "new_strategy_historical"
    COL_OPTIONS   = "Option_NIFTY"
    COL_SPOT      = "SPOT_NIFTY_5g"


# ══════════════════════════════════════════════════════════════
# 2. LOGGER
# ══════════════════════════════════════════════════════════════

def setup_logger(log_file: Path) -> logging.Logger:
    logger = logging.getLogger("backtest")
    logger.setLevel(logging.DEBUG)

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Console handler — INFO aur upar
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)

    # File handler — DEBUG level — har cheez record hogi
    fh = logging.FileHandler(log_file, mode="a", encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)

    logger.addHandler(ch)
    logger.addHandler(fh)
    return logger


log = setup_logger(Config.LOG_FILE)


# ══════════════════════════════════════════════════════════════
# 3. DB LAYER
# ══════════════════════════════════════════════════════════════

class DBClient:
    """
    MongoDB connection + bulk fetch.
    Bulk fetch = ek expiry ka poora data ek baar mein — loop mein
    bar bar hit nahi karta.
    """

    def __init__(self, cfg: Config):
        uri = (
            f"mongodb://{quote_plus(cfg.MONGO_USER)}:{quote_plus(cfg.MONGO_PASS)}"
            f"@{cfg.MONGO_HOST}:{cfg.MONGO_PORT}/admin?authSource=admin"
        )
        try:
            self._client  = MongoClient(uri, serverSelectionTimeoutMS=5000)
            self._client.admin.command("ping")   # connection verify
            self.col_opt  = self._client[cfg.DB_OPTIONS][cfg.COL_OPTIONS]
            self.col_spot = self._client[cfg.DB_SPOT][cfg.COL_SPOT]
            log.info("MongoDB connected ✅")
        except Exception as e:
            log.critical(f"MongoDB connection FAILED: {e}")
            sys.exit(1)

    def close(self):
        self._client.close()
        log.info("MongoDB connection closed.")

    # ── datetime builder ────────────────────────────────────────
    @staticmethod
    def build_dt(expiry_str: str) -> dict:
        """
        Expiry string se teeno DTs automatically.
        weekday(): Mon=0 Tue=1 Wed=2 Thu=3 Fri=4
        week_monday = expiry - weekday() → koi bhi expiry day handle karta hai.
        """
        expiry_date    = datetime.strptime(expiry_str, "%Y-%m-%d").date()
        expiry_weekday = expiry_date.weekday()

        if expiry_weekday >= 5:
            log.warning(f"Expiry {expiry_str} weekend pe hai ({expiry_date.strftime('%A')}) — verify karo!")

        week_monday = expiry_date - timedelta(days=expiry_weekday)

        dt = {
            "expiry_str"  : expiry_str,
            "expiry_date" : expiry_date,
            "expiry_day"  : expiry_date.strftime("%A"),

            # dt1 — journey window
            "dt1_start"   : datetime.combine(week_monday,  time(9, 20, 0)),
            "dt1_end"     : datetime.combine(expiry_date,  time(15, 30, 0)),

            # dt2 — CandleTimeStamp range (same as dt1)
            "dt2_gte"     : datetime.combine(week_monday,  time(9, 20, 0)),
            "dt2_lte"     : datetime.combine(expiry_date,  time(15, 30, 0)),

            # dt3 — ExpiryDate exact match (midnight)
            "dt3_expiry"  : datetime.combine(expiry_date,  time(0, 0, 0)),
        }

        log.debug(
            f"DT built | expiry={expiry_str} ({dt['expiry_day']}) | "
            f"window={dt['dt1_start']} → {dt['dt1_end']} | "
            f"dt3={dt['dt3_expiry']}"
        )
        return dt

    # ── bulk spot fetch ─────────────────────────────────────────
    def fetch_spot_bulk(self, dt: dict) -> pd.DataFrame:
        """
        SPOT: dt2 range ka poora data ek baar.
        Returns DataFrame indexed by CandleTimeStamp.
        """
        try:
            cursor = self.col_spot.find(
                {"CandleTimeStamp": {"$gte": dt["dt2_gte"], "$lte": dt["dt2_lte"]}},
                {"CandleTimeStamp": 1, "ClosePrice": 1, "_id": 0}
            )
            rows = list(cursor)
            if not rows:
                log.warning(f"SPOT bulk fetch: 0 rows | expiry={dt['expiry_str']}")
                return pd.DataFrame()

            df = pd.DataFrame(rows)
            df["CandleTimeStamp"] = pd.to_datetime(df["CandleTimeStamp"])
            df = df.set_index("CandleTimeStamp").sort_index()
            log.info(f"SPOT fetched: {len(df)} candles | {df.index[0]} → {df.index[-1]}")
            return df

        except Exception as e:
            log.error(f"SPOT bulk fetch failed: {e}")
            return pd.DataFrame()

    # ── bulk options fetch ──────────────────────────────────────
    def fetch_options_bulk(self, dt: dict) -> pd.DataFrame:
        """
        OPTIONS: dt3 expiry + dt2 time range ka poora data.
        Returns DataFrame: CandleTimeStamp | StrikePrice | OptionType | ClosePrice
        OptionType: 1=CE  2=PE
        """
        try:
            cursor = self.col_opt.find(
                {
                    "ExpiryDate":      dt["dt3_expiry"],
                    "CandleTimeStamp": {"$gte": dt["dt2_gte"], "$lte": dt["dt2_lte"]},
                },
                {"CandleTimeStamp": 1, "StrikePrice": 1,
                 "OptionType": 1, "ClosePrice": 1, "_id": 0}
            )
            rows = list(cursor)
            if not rows:
                log.warning(f"OPTIONS bulk fetch: 0 rows | expiry={dt['expiry_str']}")
                return pd.DataFrame()

            df = pd.DataFrame(rows)
            df["CandleTimeStamp"] = pd.to_datetime(df["CandleTimeStamp"])
            df["StrikePrice"]     = df["StrikePrice"].astype(int)
            df = df.sort_values("CandleTimeStamp").reset_index(drop=True)
            log.info(
                f"OPTIONS fetched: {len(df)} rows | "
                f"strikes={sorted(df['StrikePrice'].unique())}"
            )
            return df

        except Exception as e:
            log.error(f"OPTIONS bulk fetch failed: {e}")
            return pd.DataFrame()


# ── in-memory lookups (no DB hit in loop) ──────────────────────

def spot_at(df_spot: pd.DataFrame, ts: datetime) -> float | None:
    """DataFrame se spot — exact match ya next fallback."""
    if ts in df_spot.index:
        return float(df_spot.loc[ts, "ClosePrice"])
    future = df_spot[df_spot.index >= ts]
    return float(future.iloc[0]["ClosePrice"]) if not future.empty else None


def premiums_at(df_opt: pd.DataFrame, ts: datetime, strikes: dict) -> dict:
    """
    DataFrame se 4 legs ke premiums — ek minute ka slice.
    strikes = {"CE_Main": 23100, "PE_Main": 22900, ...}
    """
    minute_df = df_opt[df_opt["CandleTimeStamp"] == ts]
    if minute_df.empty:
        return {}

    result = {}
    for leg, strike in strikes.items():
        opt_type = 1 if "CE" in leg else 2
        row = minute_df[
            (minute_df["StrikePrice"] == int(strike)) &
            (minute_df["OptionType"]  == opt_type)
        ]
        if not row.empty:
            result[leg] = float(row.iloc[0]["ClosePrice"])

    log.debug(f"premiums_at {ts.strftime('%H:%M')} | strikes={strikes} | got={result}")
    return result


# ══════════════════════════════════════════════════════════════
# 4. STRATEGY ZONE  ← sirf yahan changes karo
# ══════════════════════════════════════════════════════════════

MAIN_OFFSET  = 0.007   # Short legs: 0.7% OTM
HEDGE_OFFSET = 0.015   # Hedge legs: 1.5% OTM


def _round_strike(price: float) -> int:
    """Nearest 100 — remainder ≤50 → floor, >50 → ceil."""
    r = price % 100
    return int((price // 100) * 100) if r <= 50 else int((price // 100 + 1) * 100)


def get_strikes(spot: float) -> dict:
    """
    Strategy: Short Strangle + OTM Hedge wings.
    Naya strategy → sirf ye function badlo.
    """
    return {
        "CE_Main":  _round_strike(spot * (1 + MAIN_OFFSET)),
        "PE_Main":  _round_strike(spot * (1 - MAIN_OFFSET)),
        "CE_Hedge": _round_strike(spot * (1 + HEDGE_OFFSET)),
        "PE_Hedge": _round_strike(spot * (1 - HEDGE_OFFSET)),
    }


# ══════════════════════════════════════════════════════════════
# 5. ENGINE
# ══════════════════════════════════════════════════════════════

def _apply_slippage(price: float, is_entry: bool, is_short: bool) -> float:
    cfg = Config
    if is_short:
        return price * (1 - cfg.SLIPPAGE) if is_entry else price * (1 + cfg.SLIPPAGE)
    else:
        return price * (1 + cfg.SLIPPAGE) if is_entry else price * (1 - cfg.SLIPPAGE)


def _compute_pnl(sce, spe, bce, bpe, scx, spx, bcx, bpx) -> dict:
    cfg = Config
    mul = cfg.LOT_SIZE * cfg.NUM_LOTS
    return {
        "pnl_sh_ce": round((sce - scx) * mul, 2),
        "pnl_sh_pe": round((spe - spx) * mul, 2),
        "pnl_by_ce": round((bcx - bce) * mul, 2),
        "pnl_by_pe": round((bpx - bpe) * mul, 2),
        "net_pnl":   round(((sce-scx) + (spe-spx) + (bcx-bce) + (bpx-bpe)) * mul, 2),
    }


def run_expiry(expiry_str: str, entry_dates: list[str], db: DBClient) -> list[dict]:
    """
    Ek expiry ke saare entry dates process karta hai.
    Bulk fetch once → loop mein DataFrame se lookup.
    Returns list of result dicts.
    """
    log.info(f"{'='*60}")
    log.info(f"EXPIRY: {expiry_str} | Entries: {entry_dates}")

    dt = DBClient.build_dt(expiry_str)

    # ── bulk fetch (single DB call per expiry) ──────────────────
    df_spot = db.fetch_spot_bulk(dt)
    df_opt  = db.fetch_options_bulk(dt)

    if df_spot.empty or df_opt.empty:
        log.error(f"Expiry {expiry_str}: data missing — skipping all entries")
        return []

    cfg          = Config
    results      = []
    end_journey  = dt["dt1_end"]
    expiry_dt3   = dt["dt3_expiry"]

    for entry_date_str in entry_dates:
        try:
            entry_date = datetime.strptime(entry_date_str, "%Y-%m-%d").date()
            cursor     = datetime.combine(entry_date, cfg.MARKET_OPEN)
            log.info(f"  Entry date: {entry_date_str} | cursor start: {cursor}")

            # ── state ──────────────────────────────────────────
            active          = False
            strikes         = None
            combined_sl     = 0.0
            chain           = 1
            sce = spe = bce = bpe = 0.0
            scx = spx = bcx = bpx = 0.0
            entry_ts        = None
            entry_spot      = 0.0

            # ── minute loop ────────────────────────────────────
            while cursor <= end_journey:

                # market hours guard
                if cursor.time() < cfg.MARKET_OPEN:
                    cursor = datetime.combine(cursor.date(), cfg.MARKET_OPEN)
                    continue
                if cursor.time() > cfg.MARKET_CLOSE:
                    cursor = datetime.combine(
                        cursor.date() + timedelta(days=1), cfg.MARKET_OPEN)
                    continue

                # ── A: no position — try entry ──────────────────
                if not active:
                    spot = spot_at(df_spot, cursor)
                    if spot is None:
                        log.debug(f"    {cursor.strftime('%H:%M')} spot missing — skip")
                        cursor += timedelta(minutes=1)
                        continue

                    strikes     = get_strikes(spot)
                    entry_spot  = spot
                    p           = premiums_at(df_opt, cursor, strikes)

                    if not all(k in p for k in strikes):
                        log.debug(f"    {cursor.strftime('%H:%M')} premiums incomplete {p} — skip")
                        strikes = None
                        cursor += timedelta(minutes=1)
                        continue

                    sce = _apply_slippage(p["CE_Main"],  True, True)
                    spe = _apply_slippage(p["PE_Main"],  True, True)
                    bce = _apply_slippage(p["CE_Hedge"], True, False)
                    bpe = _apply_slippage(p["PE_Hedge"], True, False)
                    scx, spx, bcx, bpx = sce, spe, bce, bpe

                    entry_ts    = cursor
                    combined_sl = (sce + spe) * cfg.FIXED_SL_PERC
                    active      = True

                    log.info(
                        f"    ENTRY Chain#{chain} @ {cursor.strftime('%H:%M')} | "
                        f"spot={round(spot,1)} | strikes={strikes} | "
                        f"sl_level={round(combined_sl,2)}"
                    )
                    cursor += timedelta(minutes=1)
                    continue

                # ── B: position active — monitor ────────────────
                p = premiums_at(df_opt, cursor, strikes)

                if p.get("CE_Main") and p.get("PE_Main"):
                    scx = _apply_slippage(p["CE_Main"], False, True)
                    spx = _apply_slippage(p["PE_Main"], False, True)
                if p.get("CE_Hedge"):
                    bcx = _apply_slippage(p["CE_Hedge"], False, False)
                if p.get("PE_Hedge"):
                    bpx = _apply_slippage(p["PE_Hedge"], False, False)

                curr_premium  = scx + spx
                sl_hit        = curr_premium >= combined_sl
                expiry_end    = cursor >= end_journey

                if sl_hit or expiry_end:
                    reason = "SL_HIT" if sl_hit else "EXPIRY"
                    pnl    = _compute_pnl(sce, spe, bce, bpe, scx, spx, bcx, bpx)

                    row = {
                        "expiry_date"    : expiry_str,
                        "entry_date"     : entry_date_str,
                        "chain"          : chain,
                        "entry_time"     : entry_ts.strftime("%H:%M") if entry_ts else "",
                        "exit_time"      : cursor.strftime("%H:%M"),
                        "exit_reason"    : reason,
                        "entry_spot"     : round(entry_spot, 2),
                        "strike_ce_main" : strikes["CE_Main"],
                        "strike_pe_main" : strikes["PE_Main"],
                        "strike_ce_hedge": strikes["CE_Hedge"],
                        "strike_pe_hedge": strikes["PE_Hedge"],
                        "sh_ce_entry"    : round(sce, 2),
                        "sh_pe_entry"    : round(spe, 2),
                        "by_ce_entry"    : round(bce, 2),
                        "by_pe_entry"    : round(bpe, 2),
                        "sh_ce_exit"     : round(scx, 2),
                        "sh_pe_exit"     : round(spx, 2),
                        "by_ce_exit"     : round(bcx, 2),
                        "by_pe_exit"     : round(bpx, 2),
                        **pnl,
                    }
                    results.append(row)

                    log.info(
                        f"    EXIT  Chain#{chain} {reason} @ {cursor.strftime('%H:%M')} | "
                        f"net_pnl=₹{pnl['net_pnl']}"
                    )

                    # ── expiry end → done ───────────────────────
                    if expiry_end:
                        active  = False
                        strikes = None
                        break

                    # ── SL hit → reentry same candle ────────────
                    respot = spot_at(df_spot, cursor)
                    if respot is None:
                        log.warning(f"    Reentry spot missing @ {cursor.strftime('%H:%M')} — next candle")
                        active  = False
                        strikes = None
                        cursor += timedelta(minutes=1)
                        continue

                    new_strikes = get_strikes(respot)
                    p_re        = premiums_at(df_opt, cursor, new_strikes)

                    if not all(k in p_re for k in new_strikes):
                        log.warning(f"    Reentry premiums incomplete @ {cursor.strftime('%H:%M')} — next candle")
                        active  = False
                        strikes = None
                        cursor += timedelta(minutes=1)
                        continue

                    # fresh entry
                    strikes    = new_strikes
                    entry_spot = respot
                    sce = _apply_slippage(p_re["CE_Main"],  True, True)
                    spe = _apply_slippage(p_re["PE_Main"],  True, True)
                    bce = _apply_slippage(p_re["CE_Hedge"], True, False)
                    bpe = _apply_slippage(p_re["PE_Hedge"], True, False)
                    scx, spx, bcx, bpx = sce, spe, bce, bpe

                    entry_ts    = cursor
                    combined_sl = (sce + spe) * cfg.FIXED_SL_PERC
                    active      = True
                    chain      += 1

                    log.info(
                        f"    ↳ REENTRY Chain#{chain} @ {cursor.strftime('%H:%M')} | "
                        f"spot={round(respot,1)} | strikes={strikes}"
                    )
                    continue   # same minute — cursor advance mat karo

                cursor += timedelta(minutes=1)

        except Exception:
            log.error(
                f"Entry {entry_date_str} (expiry {expiry_str}) FAILED:\n"
                f"{traceback.format_exc()}"
            )
            continue   # ek entry fail → baaki chalta rahe

    return results


# ══════════════════════════════════════════════════════════════
# 6. REPORTER
# ══════════════════════════════════════════════════════════════

def save_and_report(results: list[dict], cfg: Config):
    if not results:
        log.warning("Koi trade complete nahi hua — CSV nahi banega.")
        return

    df = pd.DataFrame(results)
    df.to_csv(cfg.RESULT_CSV, index=False)

    # ── basic counts ───────────────────────────────────────────
    total    = len(df)
    wins     = int((df["net_pnl"] >= 0).sum())
    losses   = int((df["net_pnl"] <  0).sum())
    win_rate = round(wins / total * 100, 1) if total else 0

    # ── pnl stats ──────────────────────────────────────────────
    net_pnl     = round(df["net_pnl"].sum(), 2)
    avg_pnl     = round(df["net_pnl"].mean(), 2)
    avg_win     = round(df.loc[df["net_pnl"] >= 0, "net_pnl"].mean(), 2) if wins   else 0
    avg_loss    = round(df.loc[df["net_pnl"] <  0, "net_pnl"].mean(), 2) if losses else 0
    best_trade  = round(df["net_pnl"].max(), 2)
    worst_trade = round(df["net_pnl"].min(), 2)
    profit_factor = (
        round(df.loc[df["net_pnl"] > 0, "net_pnl"].sum() /
              abs(df.loc[df["net_pnl"] < 0, "net_pnl"].sum()), 2)
        if losses and df.loc[df["net_pnl"] < 0, "net_pnl"].sum() != 0 else "∞"
    )

    # ── reentry stats ──────────────────────────────────────────
    sl_trades     = df[df["exit_reason"] == "SL_HIT"]
    expiry_trades = df[df["exit_reason"] == "EXPIRY"]
    max_chain     = int(df["chain"].max()) if "chain" in df.columns else 1
    avg_chain     = round(df.groupby(["expiry_date", "entry_date"])["chain"].max().mean(), 2)

    # ── drawdown ───────────────────────────────────────────────
    cumulative   = df["net_pnl"].cumsum()
    rolling_max  = cumulative.cummax()
    drawdown     = cumulative - rolling_max
    max_drawdown = round(drawdown.min(), 2)
    max_drawdown_idx = drawdown.idxmin()
    max_dd_exit  = df.loc[max_drawdown_idx, "exit_time"] if max_drawdown_idx in df.index else "N/A"

    # ── streak ─────────────────────────────────────────────────
    streak_vals     = (df["net_pnl"] >= 0).astype(int).tolist()
    max_win_streak  = max_loss_streak = cur = 0
    cur_type        = None
    for v in streak_vals:
        if v == cur_type:
            cur += 1
        else:
            cur_type = v
            cur = 1
        if v == 1: max_win_streak  = max(max_win_streak,  cur)
        else:      max_loss_streak = max(max_loss_streak, cur)

    # ── per-expiry breakdown ───────────────────────────────────
    expiry_summary = (
        df.groupby("expiry_date")
          .agg(
              trades   = ("net_pnl", "count"),
              wins     = ("net_pnl", lambda x: (x >= 0).sum()),
              net_pnl  = ("net_pnl", "sum"),
              sl_hits  = ("exit_reason", lambda x: (x == "SL_HIT").sum()),
              max_chain= ("chain", "max"),
          )
          .reset_index()
    )

    # ── per entry-date breakdown ───────────────────────────────
    day_summary = (
        df.groupby("entry_date")
          .agg(
              chains   = ("net_pnl", "count"),
              net_pnl  = ("net_pnl", "sum"),
              sl_hits  = ("exit_reason", lambda x: (x == "SL_HIT").sum()),
          )
          .reset_index()
    )
    best_day  = day_summary.loc[day_summary["net_pnl"].idxmax()]
    worst_day = day_summary.loc[day_summary["net_pnl"].idxmin()]

    # ── save summary CSV ───────────────────────────────────────
    summary_path = cfg.BASE_DIR / "summary.csv"
    expiry_summary.to_csv(summary_path, index=False)

    # ── print full report ──────────────────────────────────────
    sep = "=" * 58

    log.info("")
    log.info(sep)
    log.info("  BACKTEST RESULTS — FULL REPORT")
    log.info(sep)

    log.info("  ── OVERALL ────────────────────────────────────────")
    log.info(f"  Total Chains     : {total}  (SL={len(sl_trades)}  EXPIRY={len(expiry_trades)})")
    log.info(f"  Wins / Losses    : {wins} / {losses}")
    log.info(f"  Win Rate         : {win_rate}%")
    log.info(f"  Max Reentry Chain: {max_chain}  |  Avg Chains/Day: {avg_chain}")

    log.info("  ── PNL ────────────────────────────────────────────")
    log.info(f"  Net PNL          : ₹{net_pnl}")
    log.info(f"  Avg PNL/Chain    : ₹{avg_pnl}")
    log.info(f"  Avg Win          : ₹{avg_win}")
    log.info(f"  Avg Loss         : ₹{avg_loss}")
    log.info(f"  Best Trade       : ₹{best_trade}")
    log.info(f"  Worst Trade      : ₹{worst_trade}")
    log.info(f"  Profit Factor    : {profit_factor}")

    log.info("  ── RISK ───────────────────────────────────────────")
    log.info(f"  Max Drawdown     : ₹{max_drawdown}")
    log.info(f"  Max Win Streak   : {max_win_streak}")
    log.info(f"  Max Loss Streak  : {max_loss_streak}")

    log.info("  ── BEST / WORST DAY ───────────────────────────────")
    log.info(f"  Best Day         : {best_day['entry_date']}  ₹{round(best_day['net_pnl'],2)}  ({int(best_day['chains'])} chains)")
    log.info(f"  Worst Day        : {worst_day['entry_date']}  ₹{round(worst_day['net_pnl'],2)}  ({int(worst_day['chains'])} chains)")

    log.info("  ── PER EXPIRY ─────────────────────────────────────")
    for _, r in expiry_summary.iterrows():
        wr = round(r["wins"] / r["trades"] * 100, 1) if r["trades"] else 0
        log.info(
            f"  {r['expiry_date']}  trades={int(r['trades'])}  "
            f"wins={int(r['wins'])}({wr}%)  "
            f"sl={int(r['sl_hits'])}  max_chain={int(r['max_chain'])}  "
            f"pnl=₹{round(r['net_pnl'],2)}"
        )

    log.info("  ── FILES ──────────────────────────────────────────")
    log.info(f"  Trades CSV       : {cfg.RESULT_CSV}")
    log.info(f"  Summary CSV      : {summary_path}")
    log.info(f"  Log File         : {cfg.LOG_FILE}")
    log.info(sep)


# ══════════════════════════════════════════════════════════════
# 7. MAIN
# ══════════════════════════════════════════════════════════════

def main():
    cfg = Config()

    log.info("Backtest engine starting...")
    log.info(f"Input CSV : {cfg.CSV_INPUT}")
    log.info(f"Log file  : {cfg.LOG_FILE}")

    # ── load CSV ───────────────────────────────────────────────
    if not cfg.CSV_INPUT.exists():
        log.critical(f"Input CSV nahi mila: {cfg.CSV_INPUT}")
        sys.exit(1)

    df_input = pd.read_csv(cfg.CSV_INPUT)
    df_input.columns = df_input.columns.str.strip()

    # CSV mein columns detect karo
    # Case A: sirf "Expiry Date" column → entry date = expiry date same
    # Case B: "Entry Date" + "Expiry Date" dono hain
    has_entry_col = "Entry Date" in df_input.columns

    log.info(f"CSV rows: {len(df_input)} | has_entry_col={has_entry_col}")

    # ── group by expiry ────────────────────────────────────────
    # Ek expiry ka ek bulk fetch → efficient
    expiry_map: dict[str, list[str]] = {}

    for _, row in df_input.iterrows():
        expiry = str(row["Expiry Date"]).strip()
        entry  = str(row["Entry Date"]).strip() if has_entry_col else expiry
        expiry_map.setdefault(expiry, []).append(entry)

    log.info(f"Expiries to process: {list(expiry_map.keys())}")

    # ── DB connect ─────────────────────────────────────────────
    db = DBClient(cfg)

    # ── process each expiry ────────────────────────────────────
    all_results = []
    for expiry_str, entry_dates in expiry_map.items():
        try:
            rows = run_expiry(expiry_str, entry_dates, db)
            all_results.extend(rows)
        except Exception:
            log.error(
                f"Expiry {expiry_str} FAILED completely:\n"
                f"{traceback.format_exc()}"
            )
            continue   # ek expiry fail → baaki chalta rahe

    db.close()

    # ── save + report ──────────────────────────────────────────
    save_and_report(all_results, cfg)
    log.info("Done.")


if __name__ == "__main__":
    main()
