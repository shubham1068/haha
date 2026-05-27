from pymongo import MongoClient
from urllib.parse import quote_plus
from datetime import datetime, time, timedelta
import pandas as pd
import os

# ==============================================================================
# CONFIGURABLE PARAMETERS & PATHS
# ==============================================================================

SLIPPAGE      = 0.008   # 0.8% execution drag per transaction
LOT_SIZE      = 25      # NIFTY lot size
NUM_LOTS      = 1
FIXED_SL_PERC = 1.20    # 20% Stop Loss on combined short premium

BASE_DIR      = os.getcwd()
CSV_FILE_PATH = os.path.join(BASE_DIR, "trades_input.csv")
RESULT_PATH   = os.path.join(BASE_DIR, "result_target_sl.csv")

# ==============================================================================
# *** STRATEGY ZONE — SIRF YAHAN CHANGES KARO ***
# Alag strategy test karni ho toh sirf ye section badlo.
# Baaki poora engine same rahega.
# ==============================================================================

MAIN_OFFSET  = 0.007   # Short legs: 0.7% OTM
HEDGE_OFFSET = 0.015   # Hedge legs: 1.5% OTM

def get_strikes(spot_price: float) -> dict:
    """
    Strategy: Short Straddle + OTM Hedge wings
    Spot se 4 strikes calculate karta hai.
    Naya strategy test karna ho toh sirf ye function badlo.
    """
    return {
        "CE_Main":  calculate_strike_price(spot_price * (1 + MAIN_OFFSET)),
        "PE_Main":  calculate_strike_price(spot_price * (1 - MAIN_OFFSET)),
        "CE_Hedge": calculate_strike_price(spot_price * (1 + HEDGE_OFFSET)),
        "PE_Hedge": calculate_strike_price(spot_price * (1 - HEDGE_OFFSET)),
    }

# ==============================================================================
# MONGODB CONNECTION
# ==============================================================================

_user = "global_readonly"
_pass = "Strategy_reader@1234509"

URI = (
    f"mongodb://{quote_plus(_user)}:{quote_plus(_pass)}"
    "@192.168.12.160:27017/admin?authSource=admin"
)

client   = MongoClient(URI)
COL_OPT  = client["strategy_historical"]["Option_NIFTY"]
COL_SPOT = client["new_strategy_historical"]["SPOT_NIFTY_5g"]

# ==============================================================================
# DB DATETIME FORMAT CHECKER
# Ye block pehli baar chalao — samajh lo DB mein kis format mein data hai.
# Sab theek lage toh is block ko comment out kar sakte ho.
# ==============================================================================

print("\n" + "="*60)
print("  DB DATETIME FORMAT CHECK")
print("="*60)

# --- SPOT collection ---
spot_sample = COL_SPOT.find_one({}, {"CandleTimeStamp": 1, "ClosePrice": 1, "_id": 0})
if spot_sample:
    ts = spot_sample["CandleTimeStamp"]
    print(f"\n[SPOT] CandleTimeStamp")
    print(f"  Value : {ts}")
    print(f"  Type  : {type(ts).__name__}")
    # Expected: <class 'datetime'>  |  2025-01-16 09:15:00
    if isinstance(ts, datetime):
        print(f"  ✅ datetime object — queries direct chalegi")
        print(f"  Seconds: {ts.second}  |  Microseconds: {ts.microsecond}")
        if ts.second != 0 or ts.microsecond != 0:
            print(f"  ⚠️  WARNING: Seconds/microseconds non-zero — time(9,20) se match nahi hoga!")
            print(f"     Fix: time(9, 20, ts.second) use karo ya DB clean karo.")
    else:
        print(f"  ❌ STRING — datetime.strptime se convert karna padega!")
        print(f"     Fix: datetime.strptime(ts, '%Y-%m-%d %H:%M:%S') use karo fetching_spot_price mein")
else:
    print("\n[SPOT] ❌ Koi document nahi mila — collection empty ya connection issue")

# --- OPTIONS collection ---
opt_sample = COL_OPT.find_one({}, {"CandleTimeStamp": 1, "ExpiryDate": 1,
                                    "StrikePrice": 1, "OptionType": 1, "_id": 0})
if opt_sample:
    ts2  = opt_sample["CandleTimeStamp"]
    exp  = opt_sample["ExpiryDate"]
    print(f"\n[OPTIONS] CandleTimeStamp")
    print(f"  Value : {ts2}")
    print(f"  Type  : {type(ts2).__name__}")
    if isinstance(ts2, datetime):
        print(f"  ✅ datetime object — OK")
        if ts2.second != 0 or ts2.microsecond != 0:
            print(f"  ⚠️  WARNING: Seconds/microseconds = {ts2.second}s {ts2.microsecond}µs — mismatch possible!")
    else:
        print(f"  ❌ STRING — convert karna padega")

    print(f"\n[OPTIONS] ExpiryDate")
    print(f"  Value : {exp}")
    print(f"  Type  : {type(exp).__name__}")
    if isinstance(exp, datetime):
        print(f"  ✅ datetime object")
        print(f"  Time part: {exp.hour}:{exp.minute}:{exp.second}")
        if exp.hour != 0 or exp.minute != 0 or exp.second != 0:
            print(f"  ⚠️  WARNING: Time part non-zero — time(0,0) se match nahi hoga!")
            print(f"     Fix: expiry_datetime = datetime.combine(expiry_date, time({exp.hour}, {exp.minute}, {exp.second}))")
        else:
            print(f"  ✅ Midnight (00:00:00) — expiry_datetime = datetime.combine(date, time(0,0)) CORRECT hai")
    else:
        print(f"  ❌ STRING — convert karna padega")

    print(f"\n[OPTIONS] StrikePrice sample : {opt_sample.get('StrikePrice')} ({type(opt_sample.get('StrikePrice')).__name__})")
    print(f"[OPTIONS] OptionType sample  : {opt_sample.get('OptionType')} ({type(opt_sample.get('OptionType')).__name__})")
else:
    print("\n[OPTIONS] ❌ Koi document nahi mila — collection empty ya connection issue")

# --- Code mein jo datetime ban raha hai uska preview ---
_test_entry_date  = datetime(2025, 1, 16).date()
_test_expiry_dt   = datetime.combine(_test_entry_date, time(0, 0))
_test_cursor      = datetime.combine(_test_entry_date, time(9, 20))
_test_end         = datetime.combine(_test_entry_date, time(15, 30))

print(f"\n[CODE] Is code se banne waale datetime objects:")
print(f"  expiry_datetime      = {_test_expiry_dt}  (ExpiryDate filter mein jaata hai)")
print(f"  current_time_cursor  = {_test_cursor}  (CandleTimeStamp filter mein jaata hai)")
print(f"  end_journey_dt       = {_test_end}  (loop stop — DB mein nahi jaata)")
print(f"\n  Agar DB ke values se match karte hain ✅ toh sab theek hai.")
print(f"  Agar nahi ✅ toh upar waali warnings padho aur fix karo.")
print("="*60 + "\n")

# ==============================================================================
# CORE HELPER FUNCTIONS  (in mein kuch mat badlo)
# ==============================================================================

def calculate_strike_price(price: float) -> int:
    """Nearest 100 pe round karta hai — ≤50 neeche, >50 upar."""
    remainder = price % 100
    if remainder <= 50:
        return int((price // 100) * 100)
    else:
        return int(((price // 100) + 1) * 100)


def fetching_spot_price(target_datetime) -> float | None:
    """
    SPOT_NIFTY_5g se exact minute ka ClosePrice laata hai.
    Nahi mila toh next available candle se fallback.
    """
    doc = COL_SPOT.find_one(
        {"CandleTimeStamp": target_datetime},
        {"ClosePrice": 1, "_id": 0}
    )
    if doc:
        return doc["ClosePrice"]

    # Fallback: us minute ke baad ka pehla available candle
    fallback = COL_SPOT.find_one(
        {"CandleTimeStamp": {"$gte": target_datetime}},
        sort=[("CandleTimeStamp", 1)]
    )
    return fallback["ClosePrice"] if fallback else None


def fetch_single_minute_premiums(strikes_dict: dict, target_dt, expiry_dt) -> dict:
    """
    Option_NIFTY collection se 4 legs ke ClosePrice laata hai.
    Filter: ExpiryDate + StrikePrice ($in all 4) + CandleTimeStamp
    OptionType 1 = CE, 2 = PE
    """
    query = {
        "ExpiryDate":      expiry_dt,
        "StrikePrice":     {"$in": [float(s) for s in strikes_dict.values()]},
        "CandleTimeStamp": target_dt,
    }
    cursor = COL_OPT.find(
        query,
        {"StrikePrice": 1, "OptionType": 1, "ClosePrice": 1, "_id": 0}
    )

    data = {}
    for doc in cursor:
        stk     = int(doc["StrikePrice"])
        op_type = doc["OptionType"]    # 1=CE, 2=PE
        price   = doc["ClosePrice"]

        if stk == int(strikes_dict["CE_Main"])  and op_type == 1: data["CE_Main"]  = price
        if stk == int(strikes_dict["PE_Main"])  and op_type == 2: data["PE_Main"]  = price
        if stk == int(strikes_dict["CE_Hedge"]) and op_type == 1: data["CE_Hedge"] = price
        if stk == int(strikes_dict["PE_Hedge"]) and op_type == 2: data["PE_Hedge"] = price

    return data


def apply_slippage(price: float, is_entry: bool, is_short: bool) -> float:
    """
    Entry mein thoda bura price milta hai, exit mein bhi.
    Short sell: entry pe neeche milo, exit pe upar dena padta hai.
    Buy:        entry pe upar milo, exit pe neeche milta hai.
    """
    if is_short:
        return price * (1 - SLIPPAGE) if is_entry else price * (1 + SLIPPAGE)
    else:
        return price * (1 + SLIPPAGE) if is_entry else price * (1 - SLIPPAGE)


def compute_pnl(sh_ce_entry, sh_pe_entry, by_ce_entry, by_pe_entry,
                sh_ce_exit,  sh_pe_exit,  by_ce_exit,  by_pe_exit) -> dict:
    """
    Per-lot PNL calculate karta hai, slippage already applied hoga entries/exits mein.
    Short legs: entry - exit (premium collect kiya tha)
    Hedge legs: exit - entry (premium diya tha, wapas milta hai agar value badhti hai)
    """
    pnl_sh_ce = (sh_ce_entry - sh_ce_exit) * LOT_SIZE * NUM_LOTS
    pnl_sh_pe = (sh_pe_entry - sh_pe_exit) * LOT_SIZE * NUM_LOTS
    pnl_by_ce = (by_ce_exit  - by_ce_entry) * LOT_SIZE * NUM_LOTS
    pnl_by_pe = (by_pe_exit  - by_pe_entry) * LOT_SIZE * NUM_LOTS

    net_pnl = pnl_sh_ce + pnl_sh_pe + pnl_by_ce + pnl_by_pe

    return {
        "pnl_sh_ce": round(pnl_sh_ce, 2),
        "pnl_sh_pe": round(pnl_sh_pe, 2),
        "pnl_by_ce": round(pnl_by_ce, 2),
        "pnl_by_pe": round(pnl_by_pe, 2),
        "net_pnl":   round(net_pnl, 2),
    }

# ==============================================================================
# MAIN ENGINE  (isko kabhi mat chhuo)
# ==============================================================================

if not os.path.exists(CSV_FILE_PATH):
    print(f"Error: Input CSV nahi mila: {CSV_FILE_PATH}")
    client.close()
    exit()

df_trades = pd.read_csv(CSV_FILE_PATH)

results          = []
total_count_win  = 0
total_count_loss = 0

for index, row in df_trades.iterrows():
    try:
        trade_id        = row["NUMBER"] if "NUMBER" in row else index + 1
        entry_date_val  = row["Entry Date"].strip()
        expiry_date_str = row["Expiry Date"].strip()

        entry_date       = datetime.strptime(entry_date_val,  "%Y-%m-%d").date()
        expiry_date      = datetime.strptime(expiry_date_str, "%Y-%m-%d").date()
        expiry_datetime  = datetime.combine(expiry_date, time(0, 0))

        current_time_cursor = datetime.combine(entry_date,  time(9, 20))
        end_journey_dt      = datetime.combine(expiry_date, time(15, 30))

        # --- state variables ---
        is_position_active = False
        active_strikes     = None
        combined_sl        = 0.0
        chain_counter      = 1

        sh_ce_entry = sh_pe_entry = by_ce_entry = by_pe_entry = 0.0
        sh_ce_exit  = sh_pe_exit  = by_ce_exit  = by_pe_exit  = 0.0
        row_entry_timestamp = None
        entry_spot_price    = 0.0
        exit_reason         = ""

        # ------------------------------------------------------------------
        # MINUTE LOOP
        # ------------------------------------------------------------------
        while current_time_cursor <= end_journey_dt:
            curr_date = current_time_cursor.date()
            curr_time = current_time_cursor.time()

            # Market hours guard
            if curr_time < time(9, 20):
                current_time_cursor = datetime.combine(curr_date, time(9, 20))
                continue
            if curr_time > time(15, 30):
                current_time_cursor = datetime.combine(
                    curr_date + timedelta(days=1), time(9, 20))
                continue

            # ---- SUB-FLOW A: Position open nahi hai ----
            if not is_position_active:
                spot_price = fetching_spot_price(current_time_cursor)
                if not spot_price:
                    current_time_cursor += timedelta(minutes=1)
                    continue

                entry_spot_price = spot_price
                active_strikes   = get_strikes(spot_price)   # <-- STRATEGY CALL

                p_data = fetch_single_minute_premiums(
                    active_strikes, current_time_cursor, expiry_datetime)

                if not all(k in p_data for k in ["CE_Main", "PE_Main", "CE_Hedge", "PE_Hedge"]):
                    active_strikes = None
                    current_time_cursor += timedelta(minutes=1)
                    continue

                # Slippage apply karo entry par
                sh_ce_entry = apply_slippage(p_data["CE_Main"],  is_entry=True, is_short=True)
                sh_pe_entry = apply_slippage(p_data["PE_Main"],  is_entry=True, is_short=True)
                by_ce_entry = apply_slippage(p_data["CE_Hedge"], is_entry=True, is_short=False)
                by_pe_entry = apply_slippage(p_data["PE_Hedge"], is_entry=True, is_short=False)

                # Exit initialize karo entry price se
                sh_ce_exit = sh_ce_entry
                sh_pe_exit = sh_pe_entry
                by_ce_exit = by_ce_entry
                by_pe_exit = by_pe_entry

                row_entry_timestamp = current_time_cursor
                combined_sl         = (sh_ce_entry + sh_pe_entry) * FIXED_SL_PERC
                is_position_active  = True

                current_time_cursor += timedelta(minutes=1)
                continue

            # ---- SUB-FLOW B: Position monitor kar ----
            if is_position_active:
                p_data = fetch_single_minute_premiums(
                    active_strikes, current_time_cursor, expiry_datetime)

                if p_data.get("CE_Main") and p_data.get("PE_Main"):
                    sh_ce_exit = apply_slippage(p_data["CE_Main"], is_entry=False, is_short=True)
                    sh_pe_exit = apply_slippage(p_data["PE_Main"], is_entry=False, is_short=True)
                if p_data.get("CE_Hedge"):
                    by_ce_exit = apply_slippage(p_data["CE_Hedge"], is_entry=False, is_short=False)
                if p_data.get("PE_Hedge"):
                    by_pe_exit = apply_slippage(p_data["PE_Hedge"], is_entry=False, is_short=False)

                current_combined_premium = sh_ce_exit + sh_pe_exit
                is_sl_breached           = current_combined_premium >= combined_sl
                is_expiry_end            = current_time_cursor >= end_journey_dt

                if is_sl_breached or is_expiry_end:
                    exit_reason = "SL_HIT" if is_sl_breached else "EXPIRY"

                    pnl = compute_pnl(
                        sh_ce_entry, sh_pe_entry, by_ce_entry, by_pe_entry,
                        sh_ce_exit,  sh_pe_exit,  by_ce_exit,  by_pe_exit
                    )

                    result_row = {
                        "trade_id":            trade_id,
                        "entry_date":          entry_date_val,
                        "expiry_date":         expiry_date_str,
                        "entry_time":          row_entry_timestamp.strftime("%H:%M") if row_entry_timestamp else "",
                        "exit_time":           current_time_cursor.strftime("%H:%M"),
                        "exit_reason":         exit_reason,
                        "entry_spot":          round(entry_spot_price, 2),
                        "strike_ce_main":      active_strikes["CE_Main"],
                        "strike_pe_main":      active_strikes["PE_Main"],
                        "strike_ce_hedge":     active_strikes["CE_Hedge"],
                        "strike_pe_hedge":     active_strikes["PE_Hedge"],
                        "sh_ce_entry":         round(sh_ce_entry, 2),
                        "sh_pe_entry":         round(sh_pe_entry, 2),
                        "by_ce_entry":         round(by_ce_entry, 2),
                        "by_pe_entry":         round(by_pe_entry, 2),
                        "sh_ce_exit":          round(sh_ce_exit,  2),
                        "sh_pe_exit":          round(sh_pe_exit,  2),
                        "by_ce_exit":          round(by_ce_exit,  2),
                        "by_pe_exit":          round(by_pe_exit,  2),
                        "pnl_sh_ce":           pnl["pnl_sh_ce"],
                        "pnl_sh_pe":           pnl["pnl_sh_pe"],
                        "pnl_by_ce":           pnl["pnl_by_ce"],
                        "pnl_by_pe":           pnl["pnl_by_pe"],
                        "net_pnl":             pnl["net_pnl"],
                    }
                    results.append(result_row)

                    if pnl["net_pnl"] >= 0:
                        total_count_win += 1
                    else:
                        total_count_loss += 1

                    print(
                        f"[{trade_id}] {entry_date_val} | "
                        f"Exit: {exit_reason} @ {current_time_cursor.strftime('%H:%M')} | "
                        f"Net PNL: ₹{pnl['net_pnl']}"
                    )

                    # Position reset — agli entry ke liye ready
                    is_position_active = False
                    active_strikes     = None
                    break  # is trade row ki journey khatam

                current_time_cursor += timedelta(minutes=1)

    except Exception as e:
        print(f"[ERROR] Trade {index} failed: {e}")
        continue

# ==============================================================================
# RESULTS SAVE KARO
# ==============================================================================

if results:
    df_result = pd.DataFrame(results)
    df_result.to_csv(RESULT_PATH, index=False)

    total_trades = total_count_win + total_count_loss
    total_pnl    = df_result["net_pnl"].sum()

    print("\n" + "="*55)
    print(f"  Total Trades : {total_trades}")
    print(f"  Wins         : {total_count_win}")
    print(f"  Losses       : {total_count_loss}")
    if total_trades > 0:
        print(f"  Win Rate     : {round(total_count_win / total_trades * 100, 1)}%")
    print(f"  Net PNL      : ₹{round(total_pnl, 2)}")
    print(f"  Result saved : {RESULT_PATH}")
    print("="*55)
else:
    print("Koi trade complete nahi hua.")

client.close()
