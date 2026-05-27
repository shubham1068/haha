"""
mongo_data_loader.py
====================
MongoDB se spot aur options data fetch karta hai.
CSV DataLoader ka drop-in replacement — same interface.

MongoDB Collections:
  DB      : nifty
  Spot    : spot      (fields: CandleTimeStamp, ClosePrice)
  Options : options   (fields: CandleDate, CandleTime, ExpiryDate,
                               StrikePrice, OptionType, OpenPrice,
                               HighPrice, LowPrice, ClosePrice,
                               Vol, OpenInterest, CandleTimeStamp,
                               Symbol, Ticker)

Key Feature:
  - Given a CSV row (entry_date, entry_time, exit_time, ce_strike, pe_strike)
  - Fetch all 1-min candles between entry_time and exit_time
  - For each candle: combined_premium = CE close + PE close
  - Store all premiums in array → return the LOWEST combined premium candle
"""

from pymongo import MongoClient
from datetime import datetime, date, time, timedelta
from typing import Optional, List, Dict, Any
import pandas as pd


# ─────────────────────────────────────────────────────────────
# CONFIG — sirf yahan change karo
# ─────────────────────────────────────────────────────────────
MONGO_URI       = "mongodb://localhost:27017/"
DB_NAME         = "nifty"
SPOT_COLLECTION = "spot"
OPT_COLLECTION  = "options"


# ─────────────────────────────────────────────────────────────
# MongoDataLoader
# ─────────────────────────────────────────────────────────────

class MongoDataLoader:
    """
    MongoDB se data fetch karta hai.
    CSV DataLoader ki jagah use karo — same get_spot() aur get_option_price() methods.

    Extra method:
        find_lowest_combined_premium(...)
            → Entry-exit window mein sabse kam CE+PE premium ka candle deta hai.
    """

    def __init__(self,
                 uri: str = MONGO_URI,
                 db_name: str = DB_NAME,
                 spot_col: str = SPOT_COLLECTION,
                 opt_col: str = OPT_COLLECTION):

        print(f"MongoDataLoader: Connecting to {uri} ...")
        self.client    = MongoClient(uri, serverSelectionTimeoutMS=5000)
        self.db        = self.client[db_name]
        self.spot_col  = self.db[spot_col]
        self.opt_col   = self.db[opt_col]

        # Test connection
        self.client.admin.command("ping")
        print(f"MongoDataLoader: ✓ Connected! DB='{db_name}'\n")

    # ─────────────────────────────────────────
    # GET SPOT
    # ─────────────────────────────────────────

    def get_spot(self, candle_dt: datetime) -> Optional[float]:
        """
        Spot price fetch karo given datetime.
        candle_dt example: datetime(2023, 1, 4, 9, 20)
        """
        doc = self.spot_col.find_one(
            {"CandleTimeStamp": candle_dt},
            {"ClosePrice": 1, "_id": 0}
        )
        return float(doc["ClosePrice"]) if doc else None

    # ─────────────────────────────────────────
    # GET OPTION PRICE (single candle)
    # ─────────────────────────────────────────

    def get_option_price(self,
                         trade_date : date,
                         expiry_date: date,
                         strike     : float,
                         opt_type   : int,       # 1=CE, 2=PE
                         candle_time: time) -> Optional[float]:
        """
        Single candle ka option close price fetch karo.
        """
        candle_dt = datetime.combine(trade_date, candle_time)
        expiry_dt = datetime.combine(expiry_date, time(0, 0, 0))

        doc = self.opt_col.find_one(
            {
                "CandleTimeStamp": candle_dt,
                "ExpiryDate"     : expiry_dt,
                "StrikePrice"    : float(strike),
                "OptionType"     : int(opt_type),
            },
            {"ClosePrice": 1, "_id": 0}
        )
        return float(doc["ClosePrice"]) if doc else None

    # ─────────────────────────────────────────
    # CORE: FIND LOWEST COMBINED PREMIUM
    # ─────────────────────────────────────────

    def find_lowest_combined_premium(self,
                                     trade_date  : date,
                                     expiry_date : date,
                                     ce_strike   : float,
                                     pe_strike   : float,
                                     entry_time  : time,
                                     exit_time   : time) -> Dict[str, Any]:
        """
        Entry time se exit time ke beech mein har 1-min candle ke liye
        CE + PE = combined_premium calculate karo.

        Sabse LOWEST combined premium wala candle return karo.

        Returns:
            {
                "min_premium_time"     : datetime,
                "ce_price"             : float,
                "pe_price"             : float,
                "combined_premium"     : float,
                "all_premiums"         : [ {time, ce, pe, combined}, ... ],  ← sorted ascending
                "total_candles_found"  : int,
            }
        """
        entry_dt = datetime.combine(trade_date,  entry_time)
        exit_dt  = datetime.combine(trade_date,  exit_time)
        expiry_dt = datetime.combine(expiry_date, time(0, 0, 0))

        # ── Fetch all CE candles in window ──────────────────
        ce_cursor = self.opt_col.find(
            {
                "CandleTimeStamp": {"$gte": entry_dt, "$lte": exit_dt},
                "ExpiryDate"     : expiry_dt,
                "StrikePrice"    : float(ce_strike),
                "OptionType"     : 1,   # CE
            },
            {"CandleTimeStamp": 1, "ClosePrice": 1, "_id": 0}
        ).sort("CandleTimeStamp", 1)

        # ── Fetch all PE candles in window ──────────────────
        pe_cursor = self.opt_col.find(
            {
                "CandleTimeStamp": {"$gte": entry_dt, "$lte": exit_dt},
                "ExpiryDate"     : expiry_dt,
                "StrikePrice"    : float(pe_strike),
                "OptionType"     : 2,   # PE
            },
            {"CandleTimeStamp": 1, "ClosePrice": 1, "_id": 0}
        ).sort("CandleTimeStamp", 1)

        # ── Build lookup maps ────────────────────────────────
        ce_map = {doc["CandleTimeStamp"]: float(doc["ClosePrice"]) for doc in ce_cursor}
        pe_map = {doc["CandleTimeStamp"]: float(doc["ClosePrice"]) for doc in pe_cursor}

        # ── Combine on matching timestamps ───────────────────
        common_times = sorted(set(ce_map.keys()) & set(pe_map.keys()))

        if not common_times:
            return {
                "min_premium_time"    : None,
                "ce_price"            : None,
                "pe_price"            : None,
                "combined_premium"    : None,
                "all_premiums"        : [],
                "total_candles_found" : 0,
                "error"               : f"No matching candles found for "
                                        f"CE={ce_strike} PE={pe_strike} "
                                        f"on {trade_date} [{entry_time}–{exit_time}]"
            }

        # ── Build all_premiums array ─────────────────────────
        all_premiums = []
        for ts in common_times:
            ce_p = ce_map[ts]
            pe_p = pe_map[ts]
            all_premiums.append({
                "time"             : ts,
                "ce_price"         : ce_p,
                "pe_price"         : pe_p,
                "combined_premium" : round(ce_p + pe_p, 2),
            })

        # ── Sort by combined premium ascending ───────────────
        all_premiums.sort(key=lambda x: x["combined_premium"])

        # ── Lowest is first after sort ───────────────────────
        lowest = all_premiums[0]

        return {
            "min_premium_time"    : lowest["time"],
            "ce_price"            : lowest["ce_price"],
            "pe_price"            : lowest["pe_price"],
            "combined_premium"    : lowest["combined_premium"],
            "all_premiums"        : all_premiums,          # full sorted array
            "total_candles_found" : len(all_premiums),
        }

    # ─────────────────────────────────────────
    # PROCESS ENTIRE CSV
    # ─────────────────────────────────────────

    def process_csv(self, csv_path: str, output_csv: str = "output/lowest_premium_results.csv"):
        """
        CSV ki har row ke liye lowest combined premium find karo.
        Results CSV mein save karo.

        CSV expected columns:
            Entry Date, Exit-date, Expiry, RE ENTRY, Spot,
            Entry Time, Exit Time, ce_strike, pe_strike
        """
        print(f"Reading CSV: {csv_path}")
        df = pd.read_csv(csv_path)
        print(f"  Total rows : {len(df)}\n")

        results = []

        for idx, row in df.iterrows():
            try:
                # Parse dates
                entry_date  = pd.to_datetime(row["Entry Date"]).date()
                expiry_date = pd.to_datetime(row["Expiry"]).date()

                # Parse times — handles "9.3" → 09:18, "15" → 15:00
                entry_time = _parse_time(row["Entry Time"])
                exit_time  = _parse_time(row["Exit Time"])

                ce_strike = float(row["ce_strike"])
                pe_strike = float(row["pe_strike"])

                print(f"Row {idx+2:>3} | {entry_date} | CE={ce_strike} PE={pe_strike} "
                      f"| {entry_time}–{exit_time} ", end="")

                result = self.find_lowest_combined_premium(
                    trade_date  = entry_date,
                    expiry_date = expiry_date,
                    ce_strike   = ce_strike,
                    pe_strike   = pe_strike,
                    entry_time  = entry_time,
                    exit_time   = exit_time,
                )

                if result["combined_premium"] is None:
                    print(f"⚠  No data found")
                    results.append({
                        **row.to_dict(),
                        "lowest_combined_premium" : None,
                        "lowest_premium_time"     : None,
                        "ce_price_at_lowest"      : None,
                        "pe_price_at_lowest"      : None,
                        "total_candles_checked"   : 0,
                        "all_premiums_array"      : "[]",
                    })
                else:
                    print(f"✓  Lowest={result['combined_premium']} "
                          f"@ {result['min_premium_time'].strftime('%H:%M')} "
                          f"| Candles={result['total_candles_found']}")

                    # Serialize all_premiums as string for CSV
                    premiums_str = str([
                        round(p["combined_premium"], 2)
                        for p in result["all_premiums"]
                    ])

                    results.append({
                        **row.to_dict(),
                        "lowest_combined_premium" : result["combined_premium"],
                        "lowest_premium_time"     : result["min_premium_time"].strftime("%H:%M"),
                        "ce_price_at_lowest"      : result["ce_price"],
                        "pe_price_at_lowest"      : result["pe_price"],
                        "total_candles_checked"   : result["total_candles_found"],
                        "all_premiums_array"      : premiums_str,
                    })

            except Exception as e:
                print(f"✗  ERROR: {e}")
                results.append({**row.to_dict(), "error": str(e)})

        # ── Save output ──────────────────────────────────────
        import os
        os.makedirs(os.path.dirname(output_csv), exist_ok=True)
        out_df = pd.DataFrame(results)
        out_df.to_csv(output_csv, index=False)
        print(f"\n✓ Results saved → {output_csv}")
        print(f"  Total rows processed : {len(results)}")

        return out_df

    # ─────────────────────────────────────────
    # UTILITY — same as DataLoader interface
    # ─────────────────────────────────────────

    def get_all_expiries(self) -> list:
        """MongoDB se saari unique expiry dates."""
        pipeline = [
            {"$group": {"_id": "$ExpiryDate"}},
            {"$sort": {"_id": 1}}
        ]
        docs = self.opt_col.aggregate(pipeline)
        return [doc["_id"].date() for doc in docs]

    def get_date_range(self):
        """Options data ka min/max CandleTimeStamp."""
        first = self.opt_col.find_one(sort=[("CandleTimeStamp", 1)])
        last  = self.opt_col.find_one(sort=[("CandleTimeStamp", -1)])
        return first["CandleTimeStamp"], last["CandleTimeStamp"]

    def summary(self):
        """Quick summary."""
        start, end = self.get_date_range()
        expiries   = self.get_all_expiries()
        spot_count = self.spot_col.count_documents({})
        opt_count  = self.opt_col.count_documents({})
        print(f"\nMongoDataLoader Summary:")
        print(f"  Spot docs    : {spot_count:,}")
        print(f"  Option docs  : {opt_count:,}")
        print(f"  Date range   : {start.date()} → {end.date()}")
        print(f"  Expiries     : {len(expiries)}\n")

    def close(self):
        self.client.close()
        print("MongoDataLoader: Connection closed.")


# ─────────────────────────────────────────────────────────────
# HELPER: parse time from CSV
# ─────────────────────────────────────────────────────────────

def _parse_time(val) -> time:
    """
    CSV mein time alag formats mein ho sakta hai:
      "9.3"  → 09:18  (Excel decimal — NOT minutes, it's fraction of hour? 
                        Actually from CSV image it looks like "9.3" = 9:18 — 
                        treating as HH:MM with dot separator)
      "9:20" → 09:20
      "15"   → 15:00
      "15.3" → 15:18
    
    From your CSV: Entry Time = 9.3 means 09:03 or 09:18?
    → Assuming "9.3" = hour 9, minute 30 (i.e. dot = colon shorthand).
    → Change _parse_time if your format differs.
    """
    s = str(val).strip()

    if ":" in s:
        parts = s.split(":")
        return time(int(parts[0]), int(parts[1]))

    if "." in s:
        parts = s.split(".")
        hour = int(parts[0])
        # Treat decimal part as minutes directly: "9.3" → 9:03, "9.20" → 9:20
        minute_str = parts[1].ljust(2, "0")  # "3" → "30", "20" → "20"
        minute = int(minute_str)
        return time(hour, minute)

    # Plain number = hour only
    return time(int(float(s)), 0)


# ─────────────────────────────────────────────────────────────
# STANDALONE RUN
# ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    csv_path   = sys.argv[1] if len(sys.argv) > 1 else "result_lowest_premium.csv"
    output_csv = sys.argv[2] if len(sys.argv) > 2 else "output/lowest_premium_results.csv"

    loader = MongoDataLoader()
    loader.summary()
    loader.process_csv(csv_path, output_csv)
    loader.close()
