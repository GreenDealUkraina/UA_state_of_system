#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Interactive outage map for Ukraine (ADM1) with a dropdown by attribute.

- Base layers: Ukraine outline (light grey), contested territory (transparent red fill + red outline)
- Main layer: ADM1 polygons colored by outage bins
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
import argparse
from typing import Dict, List, Tuple

import plotly.graph_objects as go


BASE_DIR = Path(__file__).resolve().parent.parent
SHAPE_DIR = BASE_DIR / "shapefiles"

ADM1_SHP = SHAPE_DIR / "ukr_admbnda_adm1_sspe_20240416.shp"
UKR_SHP = SHAPE_DIR / "Ukraine.shp"
CONTESTED_SHP = SHAPE_DIR / "contested_territory.shp"

OUT_HTML_NAME = "ua_outage_map.html"

# (attribute_name, label)
MAP_ATTRS = [
    ("Outage_w1", "Late October 2025"),
    ("outage_w2", "Early November 2025"),
    ("outage_w3", "First week of January 2026"),
    ("O_13_jan", "13 January 2026"),
]

BIN_EDGES = [0.0, 0.1, 2.0, 4.0, 8.0, 12.0]
BIN_LABELS = [
    "No scheduled outages",
    "0.1 - 2",
    "2 - 4",
    "4 - 8",
    "8 - 12",
    "12 +",
]
BIN_COLORS = [
    "#B6E77A",
    "#FFF3A0",
    "#FFD060",
    "#FFA53A",
    "#FF3B30",
    "#C8001A",
]


def load_geojson(path: Path) -> Dict:
    cmd = [
        "ogr2ogr",
        "-t_srs",
        "EPSG:4326",
        "-f",
        "GeoJSON",
        "/vsistdout/",
        str(path),
    ]
    result = subprocess.run(cmd, check=True, capture_output=True, text=True)
    return json.loads(result.stdout)


def geo_bounds(geojson: Dict) -> Tuple[float, float, float, float]:
    min_lon = 180.0
    max_lon = -180.0
    min_lat = 90.0
    max_lat = -90.0

    def walk_coords(coords: List) -> None:
        if not coords:
            return
        if isinstance(coords[0], (float, int)):
            lon, lat = coords[0], coords[1]
            nonlocal min_lon, max_lon, min_lat, max_lat
            min_lon = min(min_lon, lon)
            max_lon = max(max_lon, lon)
            min_lat = min(min_lat, lat)
            max_lat = max(max_lat, lat)
            return
        for item in coords:
            walk_coords(item)

    for feature in geojson.get("features", []):
        geom = feature.get("geometry", {})
        coords = geom.get("coordinates", [])
        walk_coords(coords)

    return min_lon, min_lat, max_lon, max_lat


def add_feature_ids(geojson: Dict, id_field: str | None = None) -> None:
    for i, feature in enumerate(geojson.get("features", [])):
        if id_field:
            feature_id = feature.get("properties", {}).get(id_field)
        else:
            feature_id = None
        feature["id"] = feature_id if feature_id is not None else str(i)


def get_prop(props: Dict, key: str) -> float | None:
    if key in props:
        return props.get(key)
    for k, v in props.items():
        if k.lower() == key.lower():
            return v
    return None


def to_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def bin_index(value: float) -> int:
    if value <= BIN_EDGES[1]:
        return 0
    for i in range(1, len(BIN_EDGES) - 1):
        if BIN_EDGES[i] < value <= BIN_EDGES[i + 1]:
            return i
    return len(BIN_EDGES) - 1


def build_bins(geojson: Dict, attr: str) -> Tuple[List[str], List[int], List[List[object]]]:
    locations: List[str] = []
    zvals: List[int] = []
    custom: List[List[object]] = []
    for feature in geojson.get("features", []):
        props = feature.get("properties", {})
        loc = feature.get("id")
        raw = get_prop(props, attr)
        val = to_float(raw)
        if val is None:
            continue
        locations.append(loc)
        zvals.append(bin_index(val))
        custom.append([props.get("ADM1_EN") or props.get("ADM1_UA") or "", val])
    return locations, zvals, custom


def discrete_colorscale() -> List[List[object]]:
    scale: List[List[object]] = []
    n = len(BIN_COLORS)
    if n == 1:
        return [[0, BIN_COLORS[0]], [1, BIN_COLORS[0]]]
    for i, color in enumerate(BIN_COLORS):
        t0 = i / (n - 1)
        t1 = min((i + 1) / (n - 1), 1.0)
        scale.append([t0, color])
        scale.append([t1, color])
    return scale


def build_figure() -> go.Figure:
    adm1_geo = load_geojson(ADM1_SHP)
    ukr_geo = load_geojson(UKR_SHP)
    contested_geo = load_geojson(CONTESTED_SHP)

    add_feature_ids(adm1_geo, "ADM1_PCODE")
    add_feature_ids(ukr_geo)
    add_feature_ids(contested_geo)

    attr = MAP_ATTRS[0][0]
    locations, zvals, custom = build_bins(adm1_geo, attr)
    min_lon, min_lat, max_lon, max_lat = geo_bounds(adm1_geo)
    center_lon = (min_lon + max_lon) / 2
    center_lat = (min_lat + max_lat) / 2

    fig = go.Figure()

    fig.add_trace(
        go.Choropleth(
            geojson=ukr_geo,
            locations=[f["id"] for f in ukr_geo.get("features", [])],
            z=[0] * len(ukr_geo.get("features", [])),
            colorscale=[[0, "#D9D9D9"], [1, "#D9D9D9"]],
            showscale=False,
            marker_line=dict(color="#B0B0B0", width=0.8),
            hoverinfo="skip",
            name="Ukraine",
        )
    )

    fig.add_trace(
        go.Choropleth(
            geojson=adm1_geo,
            locations=locations,
            z=zvals,
            zmin=0,
            zmax=len(BIN_COLORS) - 1,
            colorscale=discrete_colorscale(),
            showscale=False,
            marker_line=dict(color="#666", width=0.6),
            customdata=custom,
            hovertemplate=(
                "Region: %{customdata[0]}<br>"
                "Hours: %{customdata[1]:.2f}<extra></extra>"
            ),
            name=MAP_ATTRS[0][1],
            showlegend=False,
        )
    )
    main_trace_index = len(fig.data) - 1

    fig.add_trace(
        go.Choropleth(
            geojson=contested_geo,
            locations=[f["id"] for f in contested_geo.get("features", [])],
            z=[0] * len(contested_geo.get("features", [])),
            colorscale=[[0, "rgba(200,0,0,0.25)"], [1, "rgba(200,0,0,0.25)"]],
            showscale=False,
            marker_line=dict(color="rgba(200,0,0,0.6)", width=1.2),
            hoverinfo="skip",
            name="Contested territory",
        )
    )

    buttons = []
    for attr_name, label in MAP_ATTRS:
        locs, z, customdata = build_bins(adm1_geo, attr_name)
        trace_payload = {
            "locations": [locs],
            "z": [z],
            "customdata": [customdata],
            "hovertemplate": [
                "Region: %{customdata[0]}<br>"
                "Hours: %{customdata[1]:.2f}<extra></extra>"
            ],
        }
        buttons.append(
            dict(
                label=label,
                method="restyle",
                args=[trace_payload, [main_trace_index]],
            )
        )

    fig.update_layout(
        dragmode=False,
        legend=dict(itemclick=False, itemdoubleclick=False, title_text="Hours:"),
        margin=dict(l=20, r=20, t=50, b=20),
        updatemenus=[
            dict(
                buttons=buttons,
                direction="down",
                x=0.02,
                xanchor="left",
                y=0.98,
                yanchor="top",
                showactive=True,
            )
        ],
    )
    fig.update_geos(
        fitbounds="locations",
        visible=False,
        projection_type="natural earth",
        center=dict(lon=center_lon, lat=center_lat),
        lonaxis=dict(range=[min_lon, max_lon]),
        lataxis=dict(range=[min_lat, max_lat]),
    )

    for label, color in zip(BIN_LABELS, BIN_COLORS):
        fig.add_trace(
            go.Scattergeo(
                lon=[None],
                lat=[None],
                mode="markers",
                marker=dict(size=12, color=color, symbol="square"),
                name=label,
                showlegend=True,
                hoverinfo="skip",
            )
        )

    fig.add_trace(
        go.Scattergeo(
            lon=[None],
            lat=[None],
            mode="markers",
            marker=dict(size=12, color="#D9D9D9", symbol="square"),
            name="No data",
            showlegend=True,
            hoverinfo="skip",
        )
    )
    fig.add_trace(
        go.Scattergeo(
            lon=[None],
            lat=[None],
            mode="markers",
            marker=dict(size=12, color="rgba(200,0,0,0.25)", line=dict(color="rgba(200,0,0,0.6)", width=1.5), symbol="square"),
            name="Contested territory",
            showlegend=True,
            hoverinfo="skip",
        )
    )

    return fig


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build outage map")
    parser.add_argument("period", help="Output subfolder name, e.g. Jan_2026")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    plots_dir = BASE_DIR / "plots" / args.period
    plots_dir.mkdir(parents=True, exist_ok=True)
    out_html = plots_dir / OUT_HTML_NAME
    fig = build_figure()
    fig.write_html(
        out_html,
        include_plotlyjs=True,
        full_html=True,
        config={
            "scrollZoom": False,
            "displayModeBar": False,
            "doubleClick": False,
        },
    )


if __name__ == "__main__":
    main()
