"""
SQLite cache layer.

Tables
------
holdings        – ETF -> constituent tickers (refreshed on demand)
intraday_trade  – TRADE bars  (ticker, date, time UTC, close, volume)
intraday_bid    – BID bars
intraday_ask    – ASK bars
fetch_log       – one row per (ticker, date, event_type); status 'ok'|'empty'
                  prevents redundant Bloomberg calls for dates already tried
"""

import sqlite3
import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent / "equity_cache.db"

_EVENT_TABLE = {
    "TRADE": "intraday_trade",
    "BID":   "intraday_bid",
    "ASK":   "intraday_ask",
}


def _conn():
    c = sqlite3.connect(DB_PATH, check_same_thread=False)
    c.execute("PRAGMA journal_mode=WAL")
    return c


def init_db():
    """Create all tables if they don't exist."""
    with _conn() as c:
        c.execute("""
            CREATE TABLE IF NOT EXISTS holdings (
                etf_ticker   TEXT NOT NULL,
                constituent  TEXT NOT NULL,
                date_pulled  TEXT NOT NULL,
                PRIMARY KEY (etf_ticker, constituent)
            )
        """)
        for tbl in _EVENT_TABLE.values():
            c.execute(f"""
                CREATE TABLE IF NOT EXISTS {tbl} (
                    ticker  TEXT NOT NULL,
                    date    TEXT NOT NULL,
                    time    TEXT NOT NULL,
                    close   REAL,
                    volume  REAL,
                    PRIMARY KEY (ticker, date, time)
                )
            """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS fetch_log (
                ticker      TEXT NOT NULL,
                date        TEXT NOT NULL,
                event_type  TEXT NOT NULL,
                status      TEXT NOT NULL,
                PRIMARY KEY (ticker, date, event_type)
            )
        """)
        c.commit()


# ── Holdings ─────────────────────────────────────────────────────────────────

def load_holdings(etf_ticker: str) -> pd.DataFrame:
    with _conn() as c:
        return pd.read_sql(
            "SELECT constituent, date_pulled FROM holdings WHERE etf_ticker = ?",
            c, params=(etf_ticker,),
        )


def save_holdings(etf_ticker: str, constituents: list, date_pulled_str: str):
    with _conn() as c:
        c.execute("DELETE FROM holdings WHERE etf_ticker = ?", (etf_ticker,))
        c.executemany(
            "INSERT OR REPLACE INTO holdings (etf_ticker, constituent, date_pulled) "
            "VALUES (?, ?, ?)",
            [(etf_ticker, t, date_pulled_str) for t in constituents],
        )
        c.commit()


# ── Fetch log ─────────────────────────────────────────────────────────────────

def get_fetched_dates(ticker: str, event_type: str) -> set:
    """Return the set of date strings already logged in fetch_log for this ticker/event."""
    with _conn() as c:
        df = pd.read_sql(
            "SELECT date FROM fetch_log WHERE ticker = ? AND event_type = ?",
            c, params=(ticker, event_type),
        )
    return set(df["date"].tolist()) if not df.empty else set()


def log_fetch(ticker: str, date_str: str, event_type: str, status: str):
    with _conn() as c:
        c.execute(
            "INSERT OR REPLACE INTO fetch_log (ticker, date, event_type, status) "
            "VALUES (?, ?, ?, ?)",
            (ticker, date_str, event_type, status),
        )
        c.commit()


# ── Intraday bars ─────────────────────────────────────────────────────────────

def save_intraday(event_type: str, ticker: str, date_str: str, df: pd.DataFrame | None):
    """
    Persist intraday bars and update fetch_log.
    Pass df=None (or empty) to record an 'empty' fetch attempt.
    """
    tbl = _EVENT_TABLE[event_type]
    with _conn() as c:
        if df is not None and not df.empty:
            records = []
            for _, row in df.iterrows():
                records.append((
                    ticker, date_str, str(row["time"]),
                    float(row["close"])  if pd.notna(row.get("close"))  else None,
                    float(row["volume"]) if pd.notna(row.get("volume")) else None,
                ))
            c.executemany(
                f"INSERT OR REPLACE INTO {tbl} (ticker, date, time, close, volume) "
                "VALUES (?, ?, ?, ?, ?)",
                records,
            )
            c.execute(
                "INSERT OR REPLACE INTO fetch_log "
                "(ticker, date, event_type, status) VALUES (?, ?, ?, 'ok')",
                (ticker, date_str, event_type),
            )
        else:
            c.execute(
                "INSERT OR REPLACE INTO fetch_log "
                "(ticker, date, event_type, status) VALUES (?, ?, ?, 'empty')",
                (ticker, date_str, event_type),
            )
        c.commit()


def load_intraday(event_type: str, tickers: list, date_strs: list) -> pd.DataFrame:
    """Load all cached bars matching the given tickers and dates."""
    if not tickers or not date_strs:
        return pd.DataFrame()
    tbl = _EVENT_TABLE[event_type]
    t_ph = ",".join(["?"] * len(tickers))
    d_ph = ",".join(["?"] * len(date_strs))
    with _conn() as c:
        return pd.read_sql(
            f"SELECT * FROM {tbl} WHERE ticker IN ({t_ph}) AND date IN ({d_ph})",
            c, params=list(tickers) + list(date_strs),
        )


# ── Settings / info ───────────────────────────────────────────────────────────

def get_cache_summary() -> dict:
    """Aggregate stats for the Settings tab."""
    result = {}
    with _conn() as c:
        result["holdings"] = pd.read_sql(
            "SELECT etf_ticker, COUNT(*) AS constituents, MAX(date_pulled) AS last_updated "
            "FROM holdings GROUP BY etf_ticker",
            c,
        )
        for tbl in _EVENT_TABLE.values():
            result[tbl] = pd.read_sql(
                f"SELECT ticker, COUNT(DISTINCT date) AS n_dates, "
                f"MIN(date) AS first_date, MAX(date) AS last_date "
                f"FROM {tbl} GROUP BY ticker ORDER BY ticker",
                c,
            )
    return result
