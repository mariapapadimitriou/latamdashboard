"""
Plotly figure builders for the six dashboard charts (2 per country).

Chart 1 – % of Daily Trading Volume   (bar, navy, season-aware)
Chart 2 – Top of Book Value & Spread  (dual-axis bars + line, season-aware)

Brazil  'All' → clustered Summer/Winter bars + two spread lines
Mexico/Chile 'All' → aggregate all seasons, single bar + single line
"""

import pandas as pd
import plotly.graph_objects as go

# ── Colour palette ────────────────────────────────────────────────────────────
NAVY      = "#1f3864"
MID_BLUE  = "#4472c4"   # Winter counterpart for bars
DARK_RED  = "#c00000"
LIGHT_RED = "#ff6b6b"   # Winter counterpart for spread lines

_BAR_COLORS  = {"Summer": NAVY,     "Winter": MID_BLUE,  "All": NAVY}
_LINE_COLORS = {"Summer": DARK_RED, "Winter": LIGHT_RED, "All": DARK_RED}

# Standard Plotly config – adds a high-res PNG download button
CHART_CONFIG = dict(
    toImageButtonOptions=dict(format="png", scale=2),
    displayModeBar=True,
    modeBarButtonsToRemove=["select2d", "lasso2d"],
)


def _base_layout(**extra) -> dict:
    return dict(
        plot_bgcolor="white",
        paper_bgcolor="white",
        font=dict(family="Arial", size=11, color="#333"),
        margin=dict(l=55, r=65, t=45, b=70),
        legend=dict(orientation="h", yanchor="bottom", y=1.02,
                    xanchor="right", x=1, font=dict(size=10)),
        **extra,
    )


# ── Chart 1: % Daily Volume ───────────────────────────────────────────────────

def build_vol_chart(
    vol_dict: dict,
    country: str,
    season_toggle: str,
) -> go.Figure:
    """
    Parameters
    ----------
    vol_dict      : {'Summer': df[local_time, pct_vol], 'Winter': df[…]}
    country       : 'Brazil' | 'Mexico' | 'Chile'
    season_toggle : 'All' | 'Summer' | 'Winter'
    """
    fig = go.Figure()

    if season_toggle == "All" and country == "Brazil":
        # Clustered bars – Summer and Winter side by side
        for season in ("Summer", "Winter"):
            df = vol_dict.get(season, pd.DataFrame())
            if not df.empty:
                fig.add_trace(go.Bar(
                    x=df["local_time"], y=df["pct_vol"],
                    name=season,
                    marker_color=_BAR_COLORS[season],
                ))
        fig.update_layout(barmode="group")

    elif season_toggle == "All":
        # Mexico / Chile: pool both seasons, re-take median
        frames = [v for v in vol_dict.values() if not v.empty]
        if frames:
            combined = pd.concat(frames)
            agg = (
                combined.groupby("local_time", as_index=False)["pct_vol"]
                .median()
                .sort_values("local_time")
            )
            fig.add_trace(go.Bar(
                x=agg["local_time"], y=agg["pct_vol"],
                name="All Seasons",
                marker_color=NAVY,
            ))

    else:
        df = vol_dict.get(season_toggle, pd.DataFrame())
        if not df.empty:
            fig.add_trace(go.Bar(
                x=df["local_time"], y=df["pct_vol"],
                name=season_toggle,
                marker_color=_BAR_COLORS.get(season_toggle, NAVY),
            ))

    fig.update_layout(
        **_base_layout(barmode="group"),
        title=dict(text="% of Daily Trading Volume", font=dict(size=13, color="#1f3864")),
        xaxis=dict(title="Time (Local)", tickangle=-45, showgrid=False),
        yaxis=dict(title="% of Daily Volume", tickformat=".1f",
                   gridcolor="#e8e8e8", zeroline=False),
    )
    return fig


# ── Chart 2: Top of Book Value & Spread ──────────────────────────────────────

def build_book_spread_chart(
    book_dict: dict,
    country: str,
    season_toggle: str,
) -> go.Figure:
    """
    Dual-axis chart:
      Left  axis (y)  – navy bars   for top-of-book value
      Right axis (y2) – red lines   for spread in bps

    Parameters
    ----------
    book_dict     : {'Summer': df[local_time, top_book_value, spread_bps], …}
    country       : 'Brazil' | 'Mexico' | 'Chile'
    season_toggle : 'All' | 'Summer' | 'Winter'
    """
    fig = go.Figure()

    def _add(season: str, df: pd.DataFrame):
        if df.empty:
            return
        bar_col  = _BAR_COLORS.get(season,  NAVY)
        line_col = _LINE_COLORS.get(season, DARK_RED)
        label    = season if season != "All" else "All Seasons"

        fig.add_trace(go.Bar(
            x=df["local_time"], y=df["top_book_value"],
            name=f"{label} – Value",
            marker_color=bar_col,
            yaxis="y",
            offsetgroup=season,
        ))
        fig.add_trace(go.Scatter(
            x=df["local_time"], y=df["spread_bps"],
            name=f"{label} – Spread (bps)",
            mode="lines+markers",
            line=dict(color=line_col, width=2),
            marker=dict(size=4),
            yaxis="y2",
        ))

    if season_toggle == "All" and country == "Brazil":
        for season in ("Summer", "Winter"):
            df = book_dict.get(season, pd.DataFrame())
            _add(season, df)

    elif season_toggle == "All":
        frames = [v for v in book_dict.values() if not v.empty]
        if frames:
            combined = pd.concat(frames)
            agg = (
                combined.groupby("local_time", as_index=False)
                .agg(top_book_value=("top_book_value", "median"),
                     spread_bps    =("spread_bps",     "median"))
                .sort_values("local_time")
            )
            _add("All", agg)

    else:
        df = book_dict.get(season_toggle, pd.DataFrame())
        _add(season_toggle, df)

    fig.update_layout(
        **_base_layout(),
        title=dict(text="Top of Book Value & Spread", font=dict(size=13, color="#1f3864")),
        xaxis=dict(title="Time (Local)", tickangle=-45, showgrid=False),
        yaxis=dict(
            title="Top of Book Value",
            tickformat=",.0f",
            gridcolor="#e8e8e8",
            zeroline=False,
        ),
        yaxis2=dict(
            title="Spread (bps)",
            overlaying="y",
            side="right",
            showgrid=False,
            tickformat=".1f",
            zeroline=False,
        ),
        barmode="group",
    )
    return fig


# ── Placeholder ───────────────────────────────────────────────────────────────

def empty_figure(message: str = "No data loaded") -> go.Figure:
    fig = go.Figure()
    fig.add_annotation(
        text=message,
        xref="paper", yref="paper",
        x=0.5, y=0.5,
        showarrow=False,
        font=dict(size=14, color="#888"),
    )
    fig.update_layout(
        **_base_layout(),
        xaxis=dict(visible=False),
        yaxis=dict(visible=False),
    )
    return fig
