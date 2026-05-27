from pymongo import MongoClient
from urllib.parse import quote_plus
from datetime import datetime, time, timedelta
import pandas as pd

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
# SIRF YE BADLO — EXPIRY DATE DO
# ==============================================================================

EXPIRY_DATE_STR = "2025-01-16"   # <-- bas ye do, baaki sab automatic

# ==============================================================================
# DT BUILDER — koi bhi expiry day handle kar lega
# Thursday (NIFTY), Wednesday (BankNifty), holiday-shifted Tuesday — sab
# ==============================================================================

def build_dt(expiry_str: str) -> dict:
    """
    Input  : "2025-01-16"  (expiry date string — koi bhi weekday)

    Logic:
      weekday() → Mon=0, Tue=1, Wed=2, Thu=3, Fri=4
      week_monday = expiry_date - timedelta(days=weekday())
      Ye formula har expiry day pe sahi Monday deta hai:
        Thursday expiry (3) → expiry - 3 = Monday ✅
        Wednesday expiry (2) → expiry - 2 = Monday ✅
        Tuesday expiry (1)  → expiry - 1 = Monday ✅  (holiday shift)
        Monday expiry (0)   → expiry - 0 = Monday ✅  (very rare)

    dt1_range_start  → datetime  | Week Monday 09:20  — data window start
    dt1_range_end    → datetime  | Expiry day 15:30   — data window end
    dt2_candle_start → datetime  | CandleTimeStamp $gte
    dt2_candle_end   → datetime  | CandleTimeStamp $lte
    dt3_expiry       → datetime  | ExpiryDate exact match (midnight 00:00:00)
    """
    expiry_date    = datetime.strptime(expiry_str, "%Y-%m-%d").date()
    expiry_weekday = expiry_date.weekday()   # Mon=0 ... Fri=4

    # Weekend check
    if expiry_weekday >= 5:
        print(f"  ⚠️  WARNING: Expiry {expiry_str} weekend pe hai "
              f"({expiry_date.strftime('%A')}) — manually verify karo!")

    # Monday of that week — works for ANY expiry weekday
    week_monday = expiry_date - timedelta(days=expiry_weekday)

    dt1_range_start  = datetime.combine(week_monday,  time(9, 20, 0))
    dt1_range_end    = datetime.combine(expiry_date,  time(15, 30, 0))
    dt2_candle_start = dt1_range_start
    dt2_candle_end   = dt1_range_end
    dt3_expiry       = datetime.combine(expiry_date,  time(0, 0, 0))

    return {
        "expiry_str"       : expiry_str,
        "expiry_date"      : expiry_date,
        "expiry_weekday"   : expiry_date.strftime("%A"),   # human readable
        "week_monday"      : week_monday,
        "dt1_range_start"  : dt1_range_start,
        "dt1_range_end"    : dt1_range_end,
        "dt2_candle_start" : dt2_candle_start,
        "dt2_candle_end"   : dt2_candle_end,
        "dt3_expiry"       : dt3_expiry,
    }


def print_dt_summary(dt: dict):
    print("\n" + "="*60)
    print("  DT SUMMARY")
    print("="*60)
    print(f"  Expiry Input     : {dt['expiry_str']}  ({dt['expiry_weekday']})")
    print(f"  Week Monday      : {dt['week_monday']}")
    print()
    print(f"  dt1_range_start  : {dt['dt1_range_start']}   ← journey window start")
    print(f"  dt1_range_end    : {dt['dt1_range_end']}   ← journey window end")
    print()
    print(f"  dt2_candle_start : {dt['dt2_candle_start']}   ← CandleTimeStamp $gte")
    print(f"  dt2_candle_end   : {dt['dt2_candle_end']}   ← CandleTimeStamp $lte")
    print()
    print(f"  dt3_expiry       : {dt['dt3_expiry']}   ← ExpiryDate exact match")
    print("="*60 + "\n")

    # Sanity table — sab dikhao ek saath
    print("  Sanity check — expiry day vs Monday mapping:")
    print(f"  {'Expiry Date':<15} {'Expiry Day':<12} {'→ Monday'}")
    print(f"  {'-'*45}")
    print(f"  {str(dt['expiry_date']):<15} {dt['expiry_weekday']:<12} {str(dt['week_monday'])}")
    print()


# ==============================================================================
# BULK FETCHER — ek baar mein poora week ka data
# ==============================================================================

def fetch_spot_bulk(dt: dict) -> pd.DataFrame:
    """
    SPOT data — dt2 range ke saare candles ek baar mein
    Returns DataFrame indexed by CandleTimeStamp
    """
    cursor = COL_SPOT.find(
        {
            "CandleTimeStamp": {
                "$gte": dt["dt2_candle_start"],
                "$lte": dt["dt2_candle_end"],
            }
        },
        {"CandleTimeStamp": 1, "ClosePrice": 1, "_id": 0}
    )

    rows = list(cursor)
    if not rows:
        print("  ⚠️  SPOT: Koi data nahi mila — dt2 range check karo")
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df["CandleTimeStamp"] = pd.to_datetime(df["CandleTimeStamp"])
    df = df.set_index("CandleTimeStamp").sort_index()
    print(f"  ✅ SPOT fetched    : {len(df)} candles  |  "
          f"{df.index[0].strftime('%a %Y-%m-%d %H:%M')}  →  "
          f"{df.index[-1].strftime('%a %Y-%m-%d %H:%M')}")
    return df


def fetch_options_bulk(dt: dict, strikes: list = None) -> pd.DataFrame:
    """
    OPTIONS data — dt3 expiry + dt2 time range
    strikes = [23000, 23100, ...]  optional, None = saare strikes fetch
    Returns DataFrame: CandleTimeStamp, StrikePrice, OptionType, ClosePrice
    """
    query = {
        "ExpiryDate":      dt["dt3_expiry"],
        "CandleTimeStamp": {
            "$gte": dt["dt2_candle_start"],
            "$lte": dt["dt2_candle_end"],
        }
    }
    if strikes:
        query["StrikePrice"] = {"$in": [float(s) for s in strikes]}

    cursor = COL_OPT.find(
        query,
        {"CandleTimeStamp": 1, "StrikePrice": 1, "OptionType": 1, "ClosePrice": 1, "_id": 0}
    )

    rows = list(cursor)
    if not rows:
        print("  ⚠️  OPTIONS: Koi data nahi mila — dt3 expiry ya strikes check karo")
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df["CandleTimeStamp"] = pd.to_datetime(df["CandleTimeStamp"])
    df["StrikePrice"]     = df["StrikePrice"].astype(int)
    df = df.sort_values("CandleTimeStamp").reset_index(drop=True)
    print(f"  ✅ OPTIONS fetched : {len(df)} rows  |  "
          f"Strikes: {sorted(df['StrikePrice'].unique())}")
    return df


# ==============================================================================
# LOOKUP HELPERS — minute loop mein DataFrame se instant lookup
# ==============================================================================

def get_spot_at(df_spot: pd.DataFrame, ts: datetime) -> float | None:
    if ts in df_spot.index:
        return df_spot.loc[ts, "ClosePrice"]
    future = df_spot[df_spot.index >= ts]
    return future.iloc[0]["ClosePrice"] if not future.empty else None


def get_option_premiums_at(df_opt: pd.DataFrame, ts: datetime, strikes_dict: dict) -> dict:
    """
    strikes_dict = {"CE_Main": 23100, "PE_Main": 22900, "CE_Hedge": 23200, "PE_Hedge": 22800}
    """
    minute_df = df_opt[df_opt["CandleTimeStamp"] == ts]
    if minute_df.empty:
        return {}

    result = {}
    for leg, strike in strikes_dict.items():
        opt_type = 1 if "CE" in leg else 2
        row = minute_df[
            (minute_df["StrikePrice"] == int(strike)) &
            (minute_df["OptionType"]  == opt_type)
        ]
        if not row.empty:
            result[leg] = row.iloc[0]["ClosePrice"]
    return result


# ==============================================================================
# MAIN
# ==============================================================================

if __name__ == "__main__":

    dt = build_dt(EXPIRY_DATE_STR)
    print_dt_summary(dt)

    print("Fetching data from MongoDB...")
    df_spot = fetch_spot_bulk(dt)
    df_opt  = fetch_options_bulk(dt)

    print(f"\n  df_spot shape  : {df_spot.shape}")
    print(f"  df_opt  shape  : {df_opt.shape}")

    # Sample lookup
    if not df_spot.empty:
        test_ts = dt["dt2_candle_start"]
        print(f"\n  Sample SPOT @ {test_ts} : {get_spot_at(df_spot, test_ts)}")

    print("\nDone.")
    client.close()
