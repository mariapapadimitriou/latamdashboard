"""
3-step median aggregation for both chart types.

Step 1  per ticker / date / time-bucket  → compute the metric
Step 2  per date / bucket               → median across tickers
Step 3  per bucket                      → median across dates

Public API
----------
prepare_dataframe(df, country)              → adds local_time + season columns
compute_pct_vol(trade_df, season_filter)    → {'Summer': df, 'Winter': df}
compute_book_spread(bid_df, ask_df, sf)     → {'Summer': df, 'Winter': df}
"""

import numpy as np
import pandas as pd

from trading_hours import utc_to_local_time_str, get_season


# ── Data preparation ──────────────────────────────────────────────────────────

def prepare_dataframe(df: pd.DataFrame, country: str) -> pd.DataFrame:
    """
    Add two derived columns to a raw intraday cache DataFrame:
      local_time  – HH:MM in the exchange's local timezone
      season      – 'Summer' or 'Winter' based on NY DST on that date
    """
    if df is None or df.empty:
        return pd.DataFrame(columns=["ticker", "date", "time", "close", "volume",
                                     "local_time", "season"])
    df = df.copy()
    df["close"]  = pd.to_numeric(df["close"],  errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce")

    # Vectorised UTC → local time conversion
    df["local_time"] = df.apply(
        lambda r: utc_to_local_time_str(r["date"], r["time"], country), axis=1
    )
    df["season"] = df["date"].apply(lambda d: get_season(pd.Timestamp(d).date()))
    return df


# ── Chart 1: % of daily trading volume ───────────────────────────────────────

def _pct_vol_inner(df: pd.DataFrame) -> pd.DataFrame:
    """
    Core 3-step aggregation for a (possibly season-filtered) trade DataFrame.
    Returns columns: local_time, pct_vol
    """
    if df.empty:
        return pd.DataFrame(columns=["local_time", "pct_vol"])

    df = df.dropna(subset=["volume", "local_time"])
    if df.empty:
        return pd.DataFrame(columns=["local_time", "pct_vol"])

    # Step 1 – pct_vol per ticker / date / bucket
    totals = (
        df.groupby(["ticker", "date"])["volume"]
        .sum()
        .rename("daily_vol")
        .reset_index()
    )
    df = df.merge(totals, on=["ticker", "date"], how="left")
    df = df[df["daily_vol"] > 0].copy()
    df["pct_vol"] = df["volume"] / df["daily_vol"] * 100

    # Step 2 – median across tickers per date / bucket
    step2 = (
        df.groupby(["date", "local_time"], as_index=False)["pct_vol"].median()
    )

    # Step 3 – median across dates per bucket
    step3 = (
        step2.groupby("local_time", as_index=False)["pct_vol"].median()
        .sort_values("local_time")
        .reset_index(drop=True)
    )
    return step3


def compute_pct_vol(trade_df: pd.DataFrame, season_filter: str = "All") -> dict:
    """
    Returns {'Summer': df, 'Winter': df} where each df has [local_time, pct_vol].
    season_filter = 'All' computes both seasons independently.
    """
    if trade_df is None or trade_df.empty:
        empty = pd.DataFrame(columns=["local_time", "pct_vol"])
        return {"Summer": empty, "Winter": empty}

    if season_filter in ("Summer", "Winter"):
        sub = trade_df[trade_df["season"] == season_filter]
        return {season_filter: _pct_vol_inner(sub)}

    return {
        "Summer": _pct_vol_inner(trade_df[trade_df["season"] == "Summer"]),
        "Winter": _pct_vol_inner(trade_df[trade_df["season"] == "Winter"]),
    }


# ── Chart 2: Top of Book Value & Spread ──────────────────────────────────────

def _book_spread_inner(bid_df: pd.DataFrame, ask_df: pd.DataFrame) -> pd.DataFrame:
    """
    Core 3-step aggregation for book value and spread.
    Returns columns: local_time, top_book_value, spread_bps
    """
    if bid_df.empty or ask_df.empty:
        return pd.DataFrame(columns=["local_time", "top_book_value", "spread_bps"])

    bid = bid_df.dropna(subset=["close", "volume", "local_time"]).copy()
    ask = ask_df.dropna(subset=["close", "volume", "local_time"]).copy()

    bid["bid_value"] = bid["close"] * bid["volume"]
    ask["ask_value"] = ask["close"] * ask["volume"]

    # Step 1 – per-ticker / date / bucket metrics
    merged = pd.merge(
        bid[["ticker", "date", "local_time", "bid_value", "close"]].rename(
            columns={"close": "bid_close"}
        ),
        ask[["ticker", "date", "local_time", "ask_value", "close"]].rename(
            columns={"close": "ask_close"}
        ),
        on=["ticker", "date", "local_time"],
        how="inner",
    )
    if merged.empty:
        return pd.DataFrame(columns=["local_time", "top_book_value", "spread_bps"])

    merged["top_book_value"] = (merged["bid_value"] + merged["ask_value"]) / 2
    merged["spread_bps"] = (
        (merged["ask_close"] - merged["bid_close"])
        / merged["bid_close"].replace(0, np.nan)
        * 10_000
    )

    # Step 2 – median across tickers per date / bucket
    step2 = merged.groupby(["date", "local_time"], as_index=False).agg(
        top_book_value=("top_book_value", "median"),
        spread_bps=("spread_bps",        "median"),
    )

    # Step 3 – median across dates per bucket
    step3 = (
        step2.groupby("local_time", as_index=False).agg(
            top_book_value=("top_book_value", "median"),
            spread_bps=("spread_bps",        "median"),
        )
        .sort_values("local_time")
        .reset_index(drop=True)
    )
    return step3


def compute_book_spread(
    bid_df: pd.DataFrame,
    ask_df: pd.DataFrame,
    season_filter: str = "All",
) -> dict:
    """
    Returns {'Summer': df, 'Winter': df} where each df has
    [local_time, top_book_value, spread_bps].
    """
    empty = pd.DataFrame(columns=["local_time", "top_book_value", "spread_bps"])
    if (bid_df is None or bid_df.empty) or (ask_df is None or ask_df.empty):
        return {"Summer": empty, "Winter": empty}

    if season_filter in ("Summer", "Winter"):
        b = bid_df[bid_df["season"] == season_filter]
        a = ask_df[ask_df["season"] == season_filter]
        return {season_filter: _book_spread_inner(b, a)}

    return {
        "Summer": _book_spread_inner(
            bid_df[bid_df["season"] == "Summer"],
            ask_df[ask_df["season"] == "Summer"],
        ),
        "Winter": _book_spread_inner(
            bid_df[bid_df["season"] == "Winter"],
            ask_df[ask_df["season"] == "Winter"],
        ),
    }
