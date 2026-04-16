"""
LatAm Equity Intraday Dashboard
================================
Two tabs:
  Dashboard – date picker, run button, 3 country panels (Brazil / Mexico / Chile)
  Settings  – cache summary, holdings refresh

Run:  python app.py
"""

import json
import logging
from datetime import date

import pandas as pd
import dash
import dash_bootstrap_components as dbc
from dash import Input, Output, State, callback_context, dash_table, dcc, html

from aggregation import compute_book_spread, compute_pct_vol, prepare_dataframe
from cache import get_cache_summary, init_db
from charts import CHART_CONFIG, build_book_spread_chart, build_vol_chart, empty_figure
from data import get_holdings, load_country_data
from trading_hours import COUNTRY_CONFIG, get_business_dates

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)

# Initialise SQLite schema on startup
init_db()

COUNTRIES = ["Brazil", "Mexico", "Chile"]


# ── Date helpers ──────────────────────────────────────────────────────────────

def _last_biz_day() -> date:
    return (pd.Timestamp.now().normalize() - pd.offsets.BDay(1)).date()


def _default_range() -> tuple:
    end   = _last_biz_day()
    start = (pd.Timestamp(end) - pd.offsets.BDay(59)).date()
    return start, end


# ── Re-usable UI components ───────────────────────────────────────────────────

def _season_toggle(country_id: str):
    return dbc.RadioItems(
        id=f"season-{country_id}",
        options=[
            {"label": "All",    "value": "All"},
            {"label": "Summer", "value": "Summer"},
            {"label": "Winter", "value": "Winter"},
        ],
        value="All",
        inline=True,
        className="mb-2 small",
    )


def _country_panel(country: str) -> dbc.Card:
    cid = country.lower()
    etf_label = COUNTRY_CONFIG[country]["etf"].replace(" Equity", "")

    return dbc.Card([
        dbc.CardHeader(
            dbc.Row([
                dbc.Col(html.H6(f"{country}  ·  {etf_label}", className="mb-0 fw-bold"), width="auto"),
                dbc.Col(_season_toggle(cid), width="auto", className="ms-auto"),
            ], align="center"),
        ),
        dbc.CardBody([
            dbc.Row([
                dbc.Col(
                    dcc.Loading(
                        dcc.Graph(
                            id=f"chart-vol-{cid}",
                            figure=empty_figure("Click Run to load data"),
                            config=dict(**CHART_CONFIG,
                                        toImageButtonOptions=dict(
                                            format="png", scale=2,
                                            filename=f"{cid}_volume")),
                        ),
                        type="circle",
                    ),
                    md=6,
                ),
                dbc.Col(
                    dcc.Loading(
                        dcc.Graph(
                            id=f"chart-book-{cid}",
                            figure=empty_figure("Click Run to load data"),
                            config=dict(**CHART_CONFIG,
                                        toImageButtonOptions=dict(
                                            format="png", scale=2,
                                            filename=f"{cid}_book_spread")),
                        ),
                        type="circle",
                    ),
                    md=6,
                ),
            ]),
            dbc.Row(dbc.Col([
                dbc.Button(
                    "Show / Hide Data", id=f"btn-show-{cid}",
                    size="sm", color="secondary", outline=True, className="mt-2",
                ),
                dbc.Collapse(
                    dbc.Card(dbc.CardBody(
                        id=f"data-body-{cid}",
                        children=html.P("No data.", className="text-muted small mb-0"),
                    ), className="mt-2 border-0 shadow-sm"),
                    id=f"collapse-{cid}",
                    is_open=False,
                ),
            ])),
        ]),
    ], className="mb-3 shadow-sm")


# ── Dashboard tab ─────────────────────────────────────────────────────────────

start_default, end_default = _default_range()

_dashboard = dbc.Container([
    dbc.Row([
        dbc.Col(html.H4("LatAm Equity Intraday Dashboard",
                        className="text-primary mb-0"), width="auto"),
    ], className="mt-3 mb-3"),

    dbc.Row([
        dbc.Col(
            dcc.DatePickerRange(
                id="date-picker",
                min_date_allowed=date(2015, 1, 1),
                max_date_allowed=_last_biz_day(),
                start_date=start_default,
                end_date=end_default,
                display_format="YYYY-MM-DD",
                clearable=False,
            ),
            width="auto",
        ),
        dbc.Col(
            dbc.Button("Run", id="btn-run", color="primary", className="ms-2"),
            width="auto", className="d-flex align-items-center",
        ),
        dbc.Col(
            dcc.Loading(
                html.Div(id="run-status", className="text-muted small"),
                type="dot",
            ),
            width="auto", className="d-flex align-items-center ms-2",
        ),
    ], className="mb-3 align-items-center"),

    # Stores aggregated chart data between callbacks
    dcc.Store(id="store-aggregated"),

    *[_country_panel(c) for c in COUNTRIES],
], fluid=True)


# ── Settings tab ─────────────────────────────────────────────────────────────

def _settings_layout() -> dbc.Container:
    """Build the settings layout fresh each time (reads live cache state)."""
    summary = get_cache_summary()

    def _table(df: pd.DataFrame, page_size: int = 12) -> html.Div:
        if df.empty:
            return html.P("Nothing cached yet.", className="text-muted small")
        return dash_table.DataTable(
            data=df.to_dict("records"),
            columns=[{"name": c, "id": c} for c in df.columns],
            page_size=page_size,
            style_table={"overflowX": "auto", "maxHeight": "280px", "overflowY": "auto"},
            style_cell={"textAlign": "left", "padding": "3px 8px", "fontSize": "12px"},
            style_header={"fontWeight": "bold", "backgroundColor": "#f0f4fa"},
            filter_action="native",
            sort_action="native",
        )

    return dbc.Container([
        html.H5("Cache Summary", className="mt-3 mb-3"),

        html.H6("ETF Holdings"),
        _table(summary["holdings"]),

        html.H6("Intraday Trade Bars", className="mt-3"),
        _table(summary["intraday_trade"]),

        html.H6("Intraday Bid Bars", className="mt-3"),
        _table(summary["intraday_bid"]),

        html.H6("Intraday Ask Bars", className="mt-3"),
        _table(summary["intraday_ask"]),

        html.Hr(className="mt-4"),
        html.H6("Refresh ETF Holdings"),
        dbc.Row([
            dbc.Col(
                dbc.Select(
                    id="settings-country",
                    options=[{"label": c, "value": c} for c in COUNTRIES],
                    placeholder="Select country…",
                ),
                md=3,
            ),
            dbc.Col(
                dbc.Button(
                    "Refresh Holdings", id="btn-refresh-holdings",
                    color="warning", outline=True, size="sm",
                ),
                width="auto",
            ),
        ], className="mb-2 align-items-end"),
        html.Div(id="settings-msg", className="text-muted small"),
    ], fluid=True)


# ── App layout ────────────────────────────────────────────────────────────────

app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    suppress_callback_exceptions=True,
    title="LatAm Intraday",
)

app.layout = html.Div([
    dcc.Tabs(
        id="main-tabs",
        value="dashboard",
        children=[
            dcc.Tab(label="Dashboard", value="dashboard", children=[_dashboard]),
            dcc.Tab(label="Settings",  value="settings",
                    children=[html.Div(id="settings-content")]),
        ],
    )
])


# ── Callbacks ─────────────────────────────────────────────────────────────────

@app.callback(
    Output("settings-content", "children"),
    Input("main-tabs", "value"),
)
def _render_settings(tab: str):
    return _settings_layout() if tab == "settings" else dash.no_update


@app.callback(
    Output("settings-msg", "children"),
    Input("btn-refresh-holdings", "n_clicks"),
    State("settings-country", "value"),
    prevent_initial_call=True,
)
def _refresh_holdings(_, country: str):
    if not country:
        return "Please select a country first."
    try:
        tickers = get_holdings(country, force_refresh=True)
        return f"Refreshed {country}: {len(tickers)} equity constituents loaded."
    except Exception as exc:
        return f"Error: {exc}"


@app.callback(
    Output("store-aggregated", "data"),
    Output("run-status", "children"),
    Input("btn-run", "n_clicks"),
    State("date-picker", "start_date"),
    State("date-picker", "end_date"),
    prevent_initial_call=True,
)
def _run(_, start_date: str, end_date: str):
    if not start_date or not end_date:
        return dash.no_update, "Please select a date range."

    today = date.today()
    start = pd.Timestamp(start_date).date()
    end   = pd.Timestamp(end_date).date()

    # Never include today
    if end >= today:
        end = _last_biz_day()

    biz_dates = pd.bdate_range(start=start, end=end).date.tolist()
    if not biz_dates:
        return dash.no_update, "No business dates in the selected range."

    agg: dict = {}
    errors: list = []

    for country in COUNTRIES:
        try:
            raw      = load_country_data(country, biz_dates)
            trade_df = prepare_dataframe(raw.get("trade", pd.DataFrame()), country)
            bid_df   = prepare_dataframe(raw.get("bid",   pd.DataFrame()), country)
            ask_df   = prepare_dataframe(raw.get("ask",   pd.DataFrame()), country)

            vol_res  = compute_pct_vol(trade_df)
            book_res = compute_book_spread(bid_df, ask_df)

            agg[country] = {
                "vol":  {s: df.to_dict("records") for s, df in vol_res.items()},
                "book": {s: df.to_dict("records") for s, df in book_res.items()},
            }
        except Exception as exc:
            logger.error("Error loading %s: %s", country, exc)
            errors.append(country)
            agg[country] = {"vol": {}, "book": {}}

    n    = len(biz_dates)
    msg  = f"Loaded {n} business days  ({start}  →  {end})"
    if errors:
        msg += f"  ⚠ errors: {', '.join(errors)}"

    return json.dumps(agg), msg


# One pair of chart callbacks per country (defined in a loop with closure capture)
def _register_country_callbacks(country: str):
    cid = country.lower()

    @app.callback(
        Output(f"chart-vol-{cid}",  "figure"),
        Output(f"chart-book-{cid}", "figure"),
        Input("store-aggregated",   "data"),
        Input(f"season-{cid}",      "value"),
    )
    def _update_charts(store_data: str, season: str, _c=country):
        empty = empty_figure("Click Run to load data")
        if not store_data:
            return empty, empty

        data   = json.loads(store_data)
        c_data = data.get(_c, {})

        vol_dict  = {s: pd.DataFrame(v) for s, v in c_data.get("vol",  {}).items()}
        book_dict = {s: pd.DataFrame(v) for s, v in c_data.get("book", {}).items()}

        vol_fig  = build_vol_chart(vol_dict,  _c, season) if vol_dict  else empty
        book_fig = build_book_spread_chart(book_dict, _c, season) if book_dict else empty
        return vol_fig, book_fig

    @app.callback(
        Output(f"collapse-{cid}",  "is_open"),
        Output(f"data-body-{cid}", "children"),
        Input(f"btn-show-{cid}",   "n_clicks"),
        State(f"collapse-{cid}",   "is_open"),
        State("store-aggregated",  "data"),
        State(f"season-{cid}",     "value"),
        prevent_initial_call=True,
    )
    def _toggle_data(_, is_open: bool, store_data: str, season: str, _c=country):
        # Toggle visibility
        new_open = not is_open
        if not new_open:
            return False, dash.no_update

        if not store_data:
            return True, html.P("No data loaded.", className="text-muted small")

        data   = json.loads(store_data)
        c_data = data.get(_c, {})
        tables = []

        for label, key, cols in [
            ("% of Daily Volume",       "vol",  ["local_time", "pct_vol"]),
            ("Top of Book Value & Spread", "book", ["local_time", "top_book_value", "spread_bps"]),
        ]:
            tables.append(html.H6(label, className="mt-2"))
            season_data = c_data.get(key, {})
            rows_found  = False

            for s, rows in season_data.items():
                if rows:
                    rows_found = True
                    sub = pd.DataFrame(rows)
                    numeric_cols = [c for c in cols if c in sub.columns and c != "local_time"]
                    for nc in numeric_cols:
                        sub[nc] = pd.to_numeric(sub[nc], errors="coerce").round(4)

                    tables.append(html.P(f"Season: {s}", className="small mb-1 fw-bold text-secondary"))
                    tables.append(dash_table.DataTable(
                        data=sub[cols].to_dict("records"),
                        columns=[{"name": c, "id": c} for c in cols],
                        page_size=20,
                        export_format="csv",
                        style_table={"overflowX": "auto"},
                        style_cell={"fontSize": "11px", "padding": "3px 8px"},
                        style_header={"fontWeight": "bold", "backgroundColor": "#f0f4fa"},
                    ))

            if not rows_found:
                tables.append(html.P("No data for this chart.", className="text-muted small"))

        return True, tables


for _country in COUNTRIES:
    _register_country_callbacks(_country)


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    app.run(debug=True, port=8050)
