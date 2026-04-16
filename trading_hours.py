"""
Session times, NY-DST-based season logic, and UTC <-> local time conversions.

'Summer' = NY DST is active (Mar-Nov)
'Winter' = NY DST is not active (Nov-Mar)
All Bloomberg calls must use UTC datetimes; charts display local time.
"""

import pytz
from datetime import datetime, date as date_type
import pandas as pd

NY_TZ = pytz.timezone("America/New_York")
UTC   = pytz.utc

# Per-country config: local timezone, session times keyed by NY-DST season.
COUNTRY_CONFIG = {
    "Brazil": {
        "tz":  pytz.timezone("America/Sao_Paulo"),
        "etf": "EWZ US Equity",
        "summer_session": ("10:00", "17:00"),
        "winter_session": ("10:00", "18:00"),
    },
    "Mexico": {
        "tz":  pytz.timezone("America/Mexico_City"),
        "etf": "EWW US Equity",
        "summer_session": ("07:30", "14:00"),
        "winter_session": ("08:30", "15:00"),
    },
    "Chile": {
        "tz":  pytz.timezone("America/Santiago"),
        "etf": "ECH US Equity",
        "summer_session": ("09:30", "16:00"),
        "winter_session": ("09:30", "16:00"),
    },
}


def _to_plain_datetime(dt) -> datetime:
    """Coerce dates/Timestamps/strings to a plain Python datetime."""
    if isinstance(dt, str):
        dt = pd.Timestamp(dt).to_pydatetime()
    elif isinstance(dt, pd.Timestamp):
        dt = dt.to_pydatetime()
    if isinstance(dt, date_type) and not isinstance(dt, datetime):
        dt = datetime(dt.year, dt.month, dt.day, 12, 0)
    return dt.replace(tzinfo=None)


def is_ny_summer(dt) -> bool:
    """Return True if New York DST is active on the given date (any type)."""
    dt = _to_plain_datetime(dt)
    return bool(NY_TZ.localize(dt).dst())


def get_season(dt) -> str:
    """Return 'Summer' or 'Winter' based on NY DST status."""
    return "Summer" if is_ny_summer(dt) else "Winter"


def get_session_utc(country: str, trade_date) -> tuple:
    """
    Return (start_utc, end_utc) as tz-aware UTC datetimes for a Bloomberg call.

    trade_date can be datetime.date, pd.Timestamp, or 'YYYY-MM-DD' string.
    """
    cfg = COUNTRY_CONFIG[country]
    local_tz = cfg["tz"]

    if isinstance(trade_date, str):
        trade_date = pd.Timestamp(trade_date).date()
    elif isinstance(trade_date, pd.Timestamp):
        trade_date = trade_date.date()

    season_key = "summer_session" if is_ny_summer(trade_date) else "winter_session"
    start_str, end_str = cfg[season_key]

    def _local_dt(t_str):
        h, m = map(int, t_str.split(":"))
        return local_tz.localize(
            datetime(trade_date.year, trade_date.month, trade_date.day, h, m)
        )

    return (
        _local_dt(start_str).astimezone(UTC),
        _local_dt(end_str).astimezone(UTC),
    )


def utc_to_local_time_str(date_str: str, time_str: str, country: str) -> str:
    """
    Convert a UTC date + time (HH:MM) stored in the cache to a local 'HH:MM' string.
    """
    local_tz = COUNTRY_CONFIG[country]["tz"]
    dt_utc = pd.Timestamp(f"{date_str} {time_str}", tz="UTC")
    return dt_utc.tz_convert(local_tz).strftime("%H:%M")


def get_business_dates(start_date=None, end_date=None, n_days: int = 60) -> list:
    """
    Return a list of datetime.date objects covering n_days business days
    ending at end_date (defaults to last business day; never includes today).
    """
    today = pd.Timestamp.now().normalize()
    if end_date is None:
        end_date = today - pd.offsets.BDay(1)
    else:
        end_date = pd.Timestamp(end_date)
        if end_date >= today:
            end_date = today - pd.offsets.BDay(1)

    if start_date is None:
        start_date = end_date - pd.offsets.BDay(n_days - 1)
    else:
        start_date = pd.Timestamp(start_date)

    return pd.bdate_range(start=start_date, end=end_date).date.tolist()
