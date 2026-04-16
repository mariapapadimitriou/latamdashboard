"""
Bloomberg / BQL data fetching with SQLite cache integration.

Public API
----------
get_holdings(country, force_refresh=False) -> list[str]
load_country_data(country, dates, progress_cb=None) -> {'trade': df, 'bid': df, 'ask': df}
"""

import logging
from datetime import date as date_type

import pandas as pd

from cache import (
    load_holdings, save_holdings,
    get_fetched_dates, save_intraday, load_intraday,
)
from trading_hours import COUNTRY_CONFIG, get_session_utc

logger = logging.getLogger(__name__)


# ── Holdings ─────────────────────────────────────────────────────────────────

def get_holdings(country: str, force_refresh: bool = False) -> list:
    """
    Return equity constituent tickers for the country ETF.
    Uses cached holdings unless force_refresh=True.
    """
    etf = COUNTRY_CONFIG[country]["etf"]
    if not force_refresh:
        cached = load_holdings(etf)
        if not cached.empty:
            return cached["constituent"].tolist()
    return _fetch_holdings_bql(etf)


def _fetch_holdings_bql(etf_ticker: str) -> list:
    from bql import BQLRunner  # noqa: PLC0415 – import deferred intentionally

    query = f"get(id()) for (holdings('{etf_ticker}'))"
    df = BQLRunner.execute(query)

    # Locate the id column (BQL returns it as 'id()')
    id_col = next(
        (c for c in df.columns if c.lower().startswith("id")), None
    )
    if id_col is None:
        logger.error("Cannot find id column in BQL response. Columns: %s", df.columns.tolist())
        return []

    tickers = df[id_col].dropna().astype(str).str.strip().tolist()
    # Keep only rows whose last token is 'Equity' (case-insensitive)
    equities = [t for t in tickers if t.split()[-1].lower() == "equity"]
    logger.info("Fetched %d equity constituents for %s", len(equities), etf_ticker)

    save_holdings(etf_ticker, equities, date_type.today().isoformat())
    return equities


# ── Intraday data ─────────────────────────────────────────────────────────────

def load_country_data(country: str, dates: list, progress_cb=None) -> dict:
    """
    Load TRADE, BID, ASK bars for all ETF constituents across the given dates.

    Only calls Bloomberg for (ticker, date, event) combinations not already
    in the cache (fetch_log). Already-fetched combinations—including those
    that returned no data—are skipped.

    Parameters
    ----------
    country      : 'Brazil', 'Mexico', or 'Chile'
    dates        : list of datetime.date or 'YYYY-MM-DD' strings
    progress_cb  : optional callable(done, total, ticker) for progress updates

    Returns
    -------
    dict with keys 'trade', 'bid', 'ask', each a pd.DataFrame with columns
    ticker, date, time (UTC HH:MM), close, volume.
    """
    tickers = get_holdings(country)
    if not tickers:
        logger.warning("No holdings found for %s", country)
        return _empty()

    date_strs = [
        d.isoformat() if hasattr(d, "isoformat") else str(d) for d in dates
    ]
    event_types = ["TRADE", "BID", "ASK"]
    total = len(tickers) * len(date_strs) * len(event_types)
    done = 0

    for ticker in tickers:
        for event_type in event_types:
            already = get_fetched_dates(ticker, event_type)
            to_fetch = [d for d in date_strs if d not in already]
            for date_str in to_fetch:
                _fetch_and_cache(ticker, date_str, country, event_type)
            done += len(to_fetch)
            if progress_cb:
                progress_cb(done, total, ticker)

    return {
        et.lower(): load_intraday(et, tickers, date_strs)
        for et in event_types
    }


def _fetch_and_cache(ticker: str, date_str: str, country: str, event_type: str):
    """Fetch one (ticker, date, event_type) bar set from Bloomberg and cache it."""
    from bbg import Bbg  # noqa: PLC0415

    try:
        trade_date = pd.Timestamp(date_str).date()
        start_utc, end_utc = get_session_utc(country, trade_date)

        raw = Bbg.intraday(
            security=ticker,
            fields=["CLOSE", "VOLUME"],
            startDateTime=start_utc,
            endDateTime=end_utc,
            eventType=event_type,
            interval=20,
        )
    except Exception as exc:
        logger.warning("Bloomberg fetch failed – %s %s %s: %s", ticker, date_str, event_type, exc)
        save_intraday(event_type, ticker, date_str, None)
        return

    if raw is None or (hasattr(raw, "empty") and raw.empty):
        save_intraday(event_type, ticker, date_str, None)
        return

    df = raw.copy()
    df.columns = [c.lower() for c in df.columns]

    # Extract UTC time string from the DatetimeIndex Bloomberg returns
    if isinstance(df.index, pd.DatetimeIndex):
        if df.index.tzinfo is None:
            df.index = df.index.tz_localize("UTC")
        df["time"] = df.index.strftime("%H:%M")
        df = df.reset_index(drop=True)
    elif "time" not in df.columns:
        logger.warning("No datetime index for %s/%s/%s", ticker, date_str, event_type)
        save_intraday(event_type, ticker, date_str, None)
        return

    save_intraday(event_type, ticker, date_str, df[["time", "close", "volume"]])


def _empty() -> dict:
    return {"trade": pd.DataFrame(), "bid": pd.DataFrame(), "ask": pd.DataFrame()}
