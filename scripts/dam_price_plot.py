#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Interactive DAM price plot for Ukraine (OREE).

Reads UA_OREE_DAM_hourly_prices_YYYY_more_info.xlsx from data/<period>/dam_prices
and outputs a Plotly HTML to plots/<period>/ua_dam_prices.html.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go


BASE_DIR = Path(__file__).resolve().parent.parent


def find_input_xlsx(period_dir: Path) -> Path:
    candidates = sorted(period_dir.glob("UA_OREE_DAM_hourly_prices_*_more_info.xlsx"))
    if not candidates:
        raise FileNotFoundError(
            f"No DAM price input found in {period_dir}. "
            "Expected UA_OREE_DAM_hourly_prices_*_more_info.xlsx"
        )
    return candidates[-1]


def build_figure(df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=df["ts_kyiv"],
            y=df["price_eur_mwh"],
            mode="lines",
            line=dict(color="rgba(15, 111, 122, 0.35)", width=2),
            hovertemplate=(
                "Kyiv time: %{x|%Y-%m-%d %H:%M}<br>"
                "Price: %{y:.2f} EUR/MWh<extra></extra>"
            ),
            name="DAM price",
        )
    )

    daily = (
        df.set_index("ts_kyiv")["price_eur_mwh"]
        .resample("D")
        .mean()
        .dropna()
        .reset_index()
    )
    fig.add_trace(
        go.Scatter(
            x=daily["ts_kyiv"],
            y=daily["price_eur_mwh"],
            mode="lines",
            line=dict(color="#E29A2D", width=3),
            hovertemplate=(
                "Date: %{x|%Y-%m-%d}<br>"
                "Daily avg: %{y:.2f} EUR/MWh<extra></extra>"
            ),
            name="Daily average",
        )
    )

    fig.update_layout(
        xaxis_title="",
        yaxis_title="Price (EUR/MWh)",
        hovermode="x",
        margin=dict(l=60, r=20, t=60, b=60),
    )

    # Use fewer ticks: one every few days
    fig.update_xaxes(nticks=10)
    return fig


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build DAM price plot")
    parser.add_argument("period", help="Data/output subfolder name, e.g. Jan_2026")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    period_dir = BASE_DIR / "data" / args.period / "dam_prices"
    plots_dir = BASE_DIR / "plots" / args.period
    plots_dir.mkdir(parents=True, exist_ok=True)

    input_xlsx = find_input_xlsx(period_dir)
    df = pd.read_excel(input_xlsx, engine="openpyxl")

    if "ts_kyiv" not in df.columns or "price_eur_mwh" not in df.columns:
        raise ValueError("Expected columns ts_kyiv and price_eur_mwh in input file.")

    df["ts_kyiv"] = pd.to_datetime(df["ts_kyiv"], errors="coerce")
    df["price_eur_mwh"] = pd.to_numeric(df["price_eur_mwh"], errors="coerce")
    df = df.dropna(subset=["ts_kyiv", "price_eur_mwh"]).sort_values("ts_kyiv")

    fig = build_figure(df)
    out_html = plots_dir / "ua_dam_prices.html"
    fig.write_html(out_html, include_plotlyjs=True, full_html=True)


if __name__ == "__main__":
    main()
