#!/usr/bin/env python
"""
Pedestrian & cyclist flow simulation for Zürich – from counting stations to edge flows.

This script is a batch version of your notebook:
- Loads daily values for pedestrians & cyclists
- Aggregates mean daily flows per station
- Builds station GeoDataFrames (LV95 → WGS84)
- Builds separate OSMnx networks for bike & walk
- Snaps stations to nearest edges
- Propagates flows along the networks with distance decay & comfort/direction weights
- Aggregates flows per edge for bike and pedestrian networks
- Writes two output files (bike edges + ped edges) in EPSG:2056

Environment variables
---------------------

I/O mode (local vs S3)
~~~~~~~~~~~~~~~~~~~~~~
If all of these are set, we run in S3 mode:
- INPUT_BUCKET      : S3 bucket for input CSV
- INPUT_KEY         : S3 key for input CSV
- OUTPUT_BUCKET     : S3 bucket for output files (prefix)
- OUTPUT_KEY        : "base" path for outputs, e.g. "silver/zurich_pedbike_flows.gpkg"

Otherwise, we run in local mode:
- LOCAL_BUCKET_ROOT : root folder inside container (default: "/data")
- INPUT_KEY         : path to CSV relative to LOCAL_BUCKET_ROOT
                      e.g. "bronze/2025_verkehrszaehlungen_werte_fussgaenger_velo.csv"
- OUTPUT_KEY        : base path for outputs relative to LOCAL_BUCKET_ROOT
                      e.g. "silver/zurich_pedbike_flows.gpkg"

The script then writes:
- <base>_bike.<ext>  (bike flows per edge)
- <base>_ped.<ext>   (ped flows per edge)

Other config
~~~~~~~~~~~~
- OUTPUT_FORMAT     : "gpkg" (default) or "parquet"
- LOG_LEVEL         : "INFO" (default), "DEBUG", ...
- PEDESTRIAN_VELO_CSV_FILE : default input file name if INPUT_KEY is not set
                              (default: "2025_verkehrszaehlungen_werte_fussgaenger_velo.csv")

OSM / model parameters (hard-coded for now, can be env-ified later)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
- place_name        : "Zürich, Switzerland"
- BIKE_MODEL_PARAMS
- PED_MODEL_PARAMS

Typical local run
-----------------
docker run --rm \
  -v "$(pwd)/local_bucket:/data" \
  -e LOCAL_BUCKET_ROOT=/data \
  -e INPUT_KEY="bronze/2025_verkehrszaehlungen_werte_fussgaenger_velo.csv" \
  -e OUTPUT_KEY="silver/zurich_pedbike_flows.gpkg" \
  -e OUTPUT_FORMAT="gpkg" \
  pedbike-flows:latest
"""

import os
import math
import logging
import tempfile
from collections import defaultdict

import boto3
import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from pyproj import Transformer
import osmnx as ox
import networkx as nx

# -------------------------------------------------------------------
# Logging
# -------------------------------------------------------------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

# -------------------------------------------------------------------
# Env helpers
# -------------------------------------------------------------------
def get_env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None:
        return default
    try:
        return int(v)
    except ValueError:
        logger.warning("Invalid int for %s=%r, using default %d", name, v, default)
        return default


def get_env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None:
        return default
    try:
        return float(v)
    except ValueError:
        logger.warning("Invalid float for %s=%r, using default %f", name, v, default)
        return default

# -------------------------------------------------------------------
# Generic helpers
# -------------------------------------------------------------------
def highway_is_any(highway_value, target_types):
    """
    Utility to test if an OSM 'highway' tag (string or list) matches any of the given types.
    """
    if isinstance(highway_value, list):
        return any(h in target_types for h in highway_value)
    return highway_value in target_types


def bearing_diff_deg(b1, b2):
    """
    Smallest absolute difference between two bearings in degrees (0–180).
    """
    if b1 is None or b2 is None:
        return np.nan
    d = abs((b1 - b2 + 180) % 360 - 180)
    return d


def direction_weight(seed_bearing, edge_bearing, max_diff, exponent):
    """
    Direction weight in [0, 1] for an edge given a seed bearing.
    - If bearings identical -> near 1.
    - If difference >= max_diff -> 0.
    Otherwise decays smoothly with exponent.
    """
    d = bearing_diff_deg(seed_bearing, edge_bearing)
    if np.isnan(d) or d >= max_diff:
        return 0.0
    base = 1.0 - (d / max_diff)
    return float(base ** exponent)


def distance_decay(flow_value, distance_m, alpha):
    """
    Exponential distance decay:
      flow(distance) = flow0 * exp(-alpha * distance_m)
    """
    if distance_m <= 0:
        return float(flow_value)
    return float(flow_value * np.exp(-alpha * distance_m))

# -------------------------------------------------------------------
# I/O helpers (same conventions as other models)
# -------------------------------------------------------------------
def load_csv_local(root: str, rel_path: str) -> pd.DataFrame:
    path = os.path.join(root, rel_path)
    logger.info("Reading local CSV: %s", path)
    if not os.path.exists(path):
        raise FileNotFoundError(f"Input file not found: {path}")
    return pd.read_csv(path, sep=",", dtype=str)


def load_csv_s3(bucket: str, key: str) -> pd.DataFrame:
    logger.info("Reading CSV from s3://%s/%s", bucket, key)
    s3 = boto3.client("s3")
    with tempfile.NamedTemporaryFile(suffix=".csv") as tmp:
        s3.download_fileobj(bucket, key, tmp)
        tmp.flush()
        tmp.seek(0)
        return pd.read_csv(tmp.name, sep=",", dtype=str)


def derive_output_paths(base_key: str, output_format: str):
    """
    Derive bike & ped output keys from a base OUTPUT_KEY.
    Example:
      base_key = "silver/zurich_pedbike_flows.gpkg"
      -> bike: "silver/zurich_pedbike_flows_bike.gpkg"
         ped : "silver/zurich_pedbike_flows_ped.gpkg"
    """
    if output_format == "gpkg":
        ext = ".gpkg"
    else:
        ext = ".parquet"

    if base_key.lower().endswith(ext):
        base = base_key[:-len(ext)]
    else:
        base = base_key

    bike_key = base + "_bike" + ext
    ped_key = base + "_ped" + ext
    return bike_key, ped_key


def save_geodataframe(
    gdf: gpd.GeoDataFrame,
    output_format: str,
    local_root: str | None,
    output_path_or_key: str,
    use_s3: bool,
    output_bucket: str | None,
):
    """
    Save GeoDataFrame either locally or to S3 as one file.
    """
    if use_s3:
        assert output_bucket is not None
        s3 = boto3.client("s3")
        with tempfile.TemporaryDirectory() as tmpdir:
            if output_format == "gpkg":
                tmp_name = "pedbike_flow.gpkg"
                tmp_path = os.path.join(tmpdir, tmp_name)
                gdf.to_file(tmp_path, driver="GPKG")
            else:
                tmp_name = "pedbike_flow.parquet"
                tmp_path = os.path.join(tmpdir, tmp_name)
                gdf.to_parquet(tmp_path, index=False)

            size_bytes = os.path.getsize(tmp_path)
            logger.info("Temporary output %s (size=%d bytes)", tmp_path, size_bytes)
            s3.upload_file(tmp_path, output_bucket, output_path_or_key)
        logger.info("Uploaded output to s3://%s/%s", output_bucket, output_path_or_key)
    else:
        assert local_root is not None
        out_path = os.path.join(local_root, output_path_or_key)
        out_dir = os.path.dirname(out_path)
        if out_dir:
            os.makedirs(out_dir, exist_ok=True)
        if output_format == "gpkg":
            gdf.to_file(out_path, driver="GPKG")
        else:
            gdf.to_parquet(out_path, index=False)
        logger.info("Output written locally to %s", out_path)

# -------------------------------------------------------------------
# Propagation models
# -------------------------------------------------------------------
BIKE_MODEL_PARAMS = {
    "max_distance_m": 3000.0,
    "min_flow": 5.0,
    "decay_alpha": 0.0015,
    "max_bearing_diff": 90.0,
    "bearing_weight_exp": 2.0,
}

PED_MODEL_PARAMS = {
    "max_distance_m": 800.0,
    "min_flow": 3.0,
    "decay_alpha": 0.003,
    "max_bearing_diff": 120.0,
    "bearing_weight_exp": 1.0,
}


def propagate_bike_flows(G: nx.MultiDiGraph, seeds_gdf: gpd.GeoDataFrame, params: dict):
    """
    Propagate daily cyclist flows from seed edges through the bike network.

    seeds_gdf: FK_STANDORT, VELO_DAY, VELO_IN_SHARE, VELO_OUT_SHARE, u, v, key
    """
    max_distance_m = params["max_distance_m"]
    min_flow = params["min_flow"]
    decay_alpha = params["decay_alpha"]
    max_bearing_diff = params["max_bearing_diff"]
    bearing_exp = params["bearing_weight_exp"]

    edge_flow = defaultdict(float)

    for idx, seed in seeds_gdf.iterrows():
        base_flow = float(seed["VELO_DAY"])
        if base_flow <= 0:
            continue

        u0 = seed["u"]
        v0 = seed["v"]
        k0 = seed["key"]

        if not G.has_edge(u0, v0, k0):
            continue

        edge_data = G[u0][v0][k0]
        bearing_fwd = edge_data.get("bearing", None)

        flow_fwd = base_flow * float(seed["VELO_IN_SHARE"])
        flow_back = base_flow * float(seed["VELO_OUT_SHARE"])

        def propagate_from_edge(u_start, v_start, key_start, initial_flow, seed_bearing):
            if initial_flow <= 0:
                return

            stack = [(u_start, v_start, key_start, 0.0, float(initial_flow))]

            while stack:
                u, v, k, dist, flow_here = stack.pop()
                edge_flow[(u, v, k)] += flow_here

                if dist >= max_distance_m or flow_here < min_flow:
                    continue

                out_edges = list(G.out_edges(v, keys=True, data=True))
                if not out_edges:
                    continue

                weights = []
                meta = []

                for _, w, k2, data2 in out_edges:
                    length = float(data2.get("length", 0.0) or 0.0)
                    if length <= 0:
                        continue

                    new_dist = dist + length
                    if new_dist > max_distance_m:
                        continue

                    edge_bearing = data2.get("bearing", None)

                    if seed_bearing is None or edge_bearing is None:
                        dir_w = 1.0
                    else:
                        dir_w = direction_weight(
                            seed_bearing,
                            edge_bearing,
                            max_bearing_diff,
                            bearing_exp,
                        )

                    highway = data2.get("highway")
                    comfort = 1.0
                    if highway_is_any(highway, ["cycleway", "path"]):
                        comfort += 2.0
                    if highway_is_any(highway, ["residential", "living_street"]):
                        comfort += 1.0
                    if highway_is_any(highway, ["primary", "secondary", "tertiary"]):
                        comfort -= 0.5

                    weight = max(dir_w * comfort, 0.0)
                    if weight <= 0:
                        continue

                    weights.append(weight)
                    meta.append((v, w, k2, length, new_dist))

                if not weights:
                    continue

                sum_w = sum(weights)
                if sum_w <= 0:
                    continue

                for (v_curr, w_curr, k_curr, length, new_dist), w in zip(meta, weights):
                    share = w / sum_w
                    flow_candidate = flow_here * share
                    flow_next = distance_decay(flow_candidate, length, decay_alpha)
                    if flow_next < min_flow:
                        continue
                    stack.append((v_curr, w_curr, k_curr, new_dist, flow_next))

        # Forward (IN share)
        if flow_fwd > 0:
            propagate_from_edge(u0, v0, k0, flow_fwd, bearing_fwd)

        # Backward (OUT share, opposite direction if exists)
        if flow_back > 0 and G.has_edge(v0, u0):
            key_back = list(G[v0][u0].keys())[0]
            edge_data_back = G[v0][u0][key_back]
            bearing_back = edge_data_back.get("bearing", None)
            propagate_from_edge(v0, u0, key_back, flow_back, bearing_back)

    return edge_flow


def propagate_ped_flows(G: nx.MultiDiGraph, seeds_gdf: gpd.GeoDataFrame, params: dict):
    """
    Propagate daily pedestrian flows from seed edges through the pedestrian network.

    seeds_gdf: FK_STANDORT, FUSS_DAY, FUSS_IN_SHARE, FUSS_OUT_SHARE, u, v, key
    """
    max_distance_m = params["max_distance_m"]
    min_flow = params["min_flow"]
    decay_alpha = params["decay_alpha"]
    max_bearing_diff = params["max_bearing_diff"]
    bearing_exp = params["bearing_weight_exp"]

    edge_flow = defaultdict(float)

    for idx, seed in seeds_gdf.iterrows():
        base_flow = float(seed["FUSS_DAY"])
        if base_flow <= 0:
            continue

        u0 = seed["u"]
        v0 = seed["v"]
        k0 = seed["key"]

        if not G.has_edge(u0, v0, k0):
            continue

        edge_data = G[u0][v0][k0]
        bearing_fwd = edge_data.get("bearing", None)

        flow_fwd = base_flow * float(seed["FUSS_IN_SHARE"])
        flow_back = base_flow * float(seed["FUSS_OUT_SHARE"])

        def propagate_from_edge(u_start, v_start, key_start, initial_flow, seed_bearing):
            if initial_flow <= 0:
                return

            stack = [(u_start, v_start, key_start, 0.0, float(initial_flow))]

            while stack:
                u, v, k, dist, flow_here = stack.pop()
                edge_flow[(u, v, k)] += flow_here

                if dist >= max_distance_m or flow_here < min_flow:
                    continue

                out_edges = list(G.out_edges(v, keys=True, data=True))
                if not out_edges:
                    continue

                weights = []
                meta = []

                for _, w, k2, data2 in out_edges:
                    length = float(data2.get("length", 0.0) or 0.0)
                    if length <= 0:
                        continue

                    new_dist = dist + length
                    if new_dist > max_distance_m:
                        continue

                    edge_bearing = data2.get("bearing", None)

                    if seed_bearing is None or edge_bearing is None:
                        dir_w = 1.0
                    else:
                        dir_w = direction_weight(
                            seed_bearing,
                            edge_bearing,
                            max_bearing_diff,
                            bearing_exp,
                        )

                    highway = data2.get("highway")
                    comfort = 1.0
                    if highway_is_any(highway, ["pedestrian", "living_street"]):
                        comfort += 2.0
                    if highway_is_any(highway, ["footway", "path", "steps"]):
                        comfort += 1.0
                    if highway_is_any(highway, ["primary", "secondary", "tertiary"]):
                        comfort -= 0.5

                    weight = max(dir_w * comfort, 0.0)
                    if weight <= 0:
                        continue

                    weights.append(weight)
                    meta.append((v, w, k2, length, new_dist))

                if not weights:
                    continue

                sum_w = sum(weights)
                if sum_w <= 0:
                    continue

                for (v_curr, w_curr, k_curr, length, new_dist), w in zip(meta, weights):
                    share = w / sum_w
                    flow_candidate = flow_here * share
                    flow_next = distance_decay(flow_candidate, length, decay_alpha)
                    if flow_next < min_flow:
                        continue
                    stack.append((v_curr, w_curr, k_curr, new_dist, flow_next))

        if flow_fwd > 0:
            propagate_from_edge(u0, v0, k0, flow_fwd, bearing_fwd)

        if flow_back > 0 and G.has_edge(v0, u0):
            key_back = list(G[v0][u0].keys())[0]
            edge_data_back = G[v0][u0][key_back]
            bearing_back = edge_data_back.get("bearing", None)
            propagate_from_edge(v0, u0, key_back, flow_back, bearing_back)

    return edge_flow

# -------------------------------------------------------------------
# MAIN
# -------------------------------------------------------------------
def main():
    # --------------------------------------------------------------
    # I/O config (same pattern as other models)
    # --------------------------------------------------------------
    input_bucket = os.getenv("INPUT_BUCKET")
    input_key = os.getenv("INPUT_KEY")
    output_bucket = os.getenv("OUTPUT_BUCKET")
    output_key = os.getenv("OUTPUT_KEY")

    local_root = os.getenv("LOCAL_BUCKET_ROOT", "/data")
    default_csv_name = os.getenv(
        "PEDESTRIAN_VELO_CSV_FILE",
        "2025_verkehrszaehlungen_werte_fussgaenger_velo.csv",
    )

    use_s3 = all([input_bucket, input_key, output_bucket, output_key])

    if not output_key:
        output_key = "pedbike_flows.gpkg"

    if not input_key:
        input_key = default_csv_name

    output_format_raw = os.getenv("OUTPUT_FORMAT", "gpkg").strip().lower()
    if output_format_raw in ("gpkg", "geopackage"):
        output_format = "gpkg"
    elif output_format_raw in ("parquet", "geoparquet"):
        output_format = "parquet"
    else:
        logger.warning("Unknown OUTPUT_FORMAT=%r, defaulting to 'gpkg'", output_format_raw)
        output_format = "gpkg"

    if use_s3:
        logger.info("Running in S3 mode.")
        logger.info("Input:  s3://%s/%s", input_bucket, input_key)
        logger.info("Output base: s3://%s/%s", output_bucket, output_key)
    else:
        logger.info("Running in local mode.")
        logger.info("Local root: %s", local_root)
        logger.info("Input:  %s", os.path.join(local_root, input_key))
        logger.info("Output base: %s", os.path.join(local_root, output_key))

    logger.info("Output format: %s", output_format)

    bike_output_key, ped_output_key = derive_output_paths(output_key, output_format)
    logger.info("Bike output key: %s", bike_output_key)
    logger.info("Ped  output key: %s", ped_output_key)

    # --------------------------------------------------------------
    # 1. Load input CSV
    # --------------------------------------------------------------
    if use_s3:
        df_raw = load_csv_s3(input_bucket, input_key)
    else:
        df_raw = load_csv_local(local_root, input_key)

    logger.info("Loaded raw counts: %d rows, %d columns", len(df_raw), len(df_raw.columns))

    # --------------------------------------------------------------
    # 2. Date parsing & daily aggregation
    # --------------------------------------------------------------
    df_raw["DATUM"] = pd.to_datetime(df_raw["DATUM"], errors="coerce")
    df_raw["DATE"] = df_raw["DATUM"].dt.date

    agg_cols = ["VELO_IN", "VELO_OUT", "FUSS_IN", "FUSS_OUT"]
    for col in agg_cols:
        df_raw[col] = pd.to_numeric(df_raw[col], errors="coerce").fillna(0)

    df_daily = (
        df_raw
        .groupby(["FK_STANDORT", "DATE"], as_index=False)[agg_cols]
        .sum()
    )

    df_daily["VELO_DAY"] = df_daily["VELO_IN"] + df_daily["VELO_OUT"]
    df_daily["FUSS_DAY"] = df_daily["FUSS_IN"] + df_daily["FUSS_OUT"]

    df_daily["VELO_IN_SHARE"] = df_daily["VELO_IN"].div(df_daily["VELO_DAY"]).fillna(0.0)
    df_daily["VELO_OUT_SHARE"] = df_daily["VELO_OUT"].div(df_daily["VELO_DAY"]).fillna(0.0)

    df_daily["FUSS_IN_SHARE"] = df_daily["FUSS_IN"].div(df_daily["FUSS_DAY"]).fillna(0.0)
    df_daily["FUSS_OUT_SHARE"] = df_daily["FUSS_OUT"].div(df_daily["FUSS_DAY"]).fillna(0.0)

    df_daily["DATE"] = pd.to_datetime(df_daily["DATE"])

    flow_cols = [
        "VELO_DAY", "FUSS_DAY",
        "VELO_IN", "VELO_OUT",
        "FUSS_IN", "FUSS_OUT",
        "VELO_IN_SHARE", "VELO_OUT_SHARE",
        "FUSS_IN_SHARE", "FUSS_OUT_SHARE",
    ]

    df_station_daily = (
        df_daily
        .groupby("FK_STANDORT", as_index=False)[flow_cols]
        .mean()
    )

    df_coords = (
        df_raw
        .groupby("FK_STANDORT", as_index=False)[["OST", "NORD"]]
        .first()
    )

    df_coords["OST"] = pd.to_numeric(df_coords["OST"], errors="coerce")
    df_coords["NORD"] = pd.to_numeric(df_coords["NORD"], errors="coerce")

    df_station_daily = df_station_daily.merge(df_coords, on="FK_STANDORT", how="left")
    logger.info("Stations after merging coords: %d", len(df_station_daily))

    # --------------------------------------------------------------
    # 3. Build station GeoDataFrames (LV95 -> WGS84)
    # --------------------------------------------------------------
    gdf_stations = gpd.GeoDataFrame(
        df_station_daily.copy(),
        geometry=gpd.points_from_xy(df_station_daily["OST"], df_station_daily["NORD"]),
        crs="EPSG:2056",  # LV95
    )

    # Keep original LV95 for output if needed, but for network ops we go to 4326
    gdf_stations_4326 = gdf_stations.to_crs(epsg=4326)

    gdf_bike_stations = gdf_stations_4326[gdf_stations_4326["VELO_DAY"] > 0].copy()
    gdf_ped_stations = gdf_stations_4326[gdf_stations_4326["FUSS_DAY"] > 0].copy()

    logger.info("Total stations: %d", len(gdf_stations_4326))
    logger.info("Bike stations : %d", len(gdf_bike_stations))
    logger.info("Ped stations  : %d", len(gdf_ped_stations))

    # --------------------------------------------------------------
    # 4. OSM networks for bike & walk
    # --------------------------------------------------------------
    ox.settings.use_cache = True
    ox.settings.log_console = False

    place_name = os.getenv("OSM_PLACE", "Zürich, Switzerland")

    logger.info("Downloading bike network for %s ...", place_name)
    G_bike = ox.graph_from_place(place_name, network_type="bike", simplify=True)
    G_bike = ox.add_edge_speeds(G_bike)
    G_bike = ox.add_edge_travel_times(G_bike)
    G_bike = ox.add_edge_bearings(G_bike)

    logger.info("Bike graph: %d nodes, %d edges", len(G_bike.nodes), len(G_bike.edges))

    edges_bike_gdf = ox.graph_to_gdfs(G_bike, nodes=False, edges=True)
    logger.info("Bike edges: %d", len(edges_bike_gdf))

    logger.info("Downloading pedestrian network for %s ...", place_name)
    G_ped = ox.graph_from_place(place_name, network_type="walk", simplify=True)
    G_ped = ox.add_edge_speeds(G_ped)
    G_ped = ox.add_edge_travel_times(G_ped)
    G_ped = ox.add_edge_bearings(G_ped)

    logger.info("Ped graph: %d nodes, %d edges", len(G_ped.nodes), len(G_ped.edges))

    edges_ped_gdf = ox.graph_to_gdfs(G_ped, nodes=False, edges=True)
    logger.info("Ped edges: %d", len(edges_ped_gdf))

    # --------------------------------------------------------------
    # 5. Snap bike and ped stations to nearest edges (in LV95)
    # --------------------------------------------------------------
    # Bike
    gdf_bike_proj = gdf_bike_stations.to_crs(epsg=2056)
    edges_bike_proj = edges_bike_gdf.to_crs(epsg=2056).reset_index()
    edges_bike_snap = edges_bike_proj[["u", "v", "key", "geometry"]].copy()

    bike_station_edges = gpd.sjoin_nearest(
        gdf_bike_proj,
        edges_bike_snap,
        how="left",
        distance_col="snap_distance_m",
    ).to_crs(epsg=4326)

    bike_seeds = bike_station_edges[
        [
            "FK_STANDORT",
            "VELO_DAY",
            "VELO_IN_SHARE",
            "VELO_OUT_SHARE",
            "u",
            "v",
            "key",
            "snap_distance_m",
            "geometry",
        ]
    ].copy()

    logger.info(
        "Bike seeds: %d (snap distance min=%.2f m, max=%.2f m)",
        len(bike_seeds),
        float(bike_seeds["snap_distance_m"].min()),
        float(bike_seeds["snap_distance_m"].max()),
    )

    # Ped
    gdf_ped_proj = gdf_ped_stations.to_crs(epsg=2056)
    edges_ped_proj = edges_ped_gdf.to_crs(epsg=2056).reset_index()
    edges_ped_snap = edges_ped_proj[["u", "v", "key", "geometry"]].copy()

    ped_station_edges = gpd.sjoin_nearest(
        gdf_ped_proj,
        edges_ped_snap,
        how="left",
        distance_col="snap_distance_m",
    ).to_crs(epsg=4326)

    ped_seeds = ped_station_edges[
        [
            "FK_STANDORT",
            "FUSS_DAY",
            "FUSS_IN_SHARE",
            "FUSS_OUT_SHARE",
            "u",
            "v",
            "key",
            "snap_distance_m",
            "geometry",
        ]
    ].copy()

    logger.info(
        "Ped seeds: %d (snap distance min=%.2f m, max=%.2f m)",
        len(ped_seeds),
        float(ped_seeds["snap_distance_m"].min()),
        float(ped_seeds["snap_distance_m"].max()),
    )

    # --------------------------------------------------------------
    # 6. Propagate flows (bike + ped)
    # --------------------------------------------------------------
    logger.info("Propagating bike flows ...")
    bike_edge_flows = propagate_bike_flows(G_bike, bike_seeds, BIKE_MODEL_PARAMS)

    df_bike_flows = pd.DataFrame(
        [
            {"u": u, "v": v, "key": k, "bike_flow_daily": flow}
            for (u, v, k), flow in bike_edge_flows.items()
        ]
    )

    logger.info("Bike edge flows records: %d", len(df_bike_flows))

    logger.info("Propagating pedestrian flows ...")
    ped_edge_flows = propagate_ped_flows(G_ped, ped_seeds, PED_MODEL_PARAMS)

    df_ped_flows = pd.DataFrame(
        [
            {"u": u, "v": v, "key": k, "ped_flow_daily": flow}
            for (u, v, k), flow in ped_edge_flows.items()
        ]
    )

    logger.info("Ped edge flows records: %d", len(df_ped_flows))

    # --------------------------------------------------------------
    # 7. Join flows back to edges, normalize, reproject to EPSG:2056
    # --------------------------------------------------------------
    # Bike
    edges_bike_with_index = edges_bike_gdf.reset_index()
    edges_bike_with_flow = edges_bike_with_index.merge(
        df_bike_flows,
        on=["u", "v", "key"],
        how="left",
    )

    edges_bike_with_flow["bike_flow_daily"] = edges_bike_with_flow["bike_flow_daily"].fillna(0.0)

    max_bike_flow = edges_bike_with_flow["bike_flow_daily"].max()
    if max_bike_flow > 0:
        edges_bike_with_flow["bike_flow_norm"] = edges_bike_with_flow["bike_flow_daily"] / max_bike_flow
    else:
        edges_bike_with_flow["bike_flow_norm"] = 0.0

    gdf_bike_edges = gpd.GeoDataFrame(
        edges_bike_with_flow,
        geometry="geometry",
        crs=edges_bike_gdf.crs,
    ).to_crs(epsg=2056)

    logger.info(
        "Bike edges with bike_flow_daily > 0: %d / %d (CRS=%s)",
        int((gdf_bike_edges["bike_flow_daily"] > 0).sum()),
        len(gdf_bike_edges),
        gdf_bike_edges.crs,
    )

    # Ped
    edges_ped_with_index = edges_ped_gdf.reset_index()
    edges_ped_with_flow = edges_ped_with_index.merge(
        df_ped_flows,
        on=["u", "v", "key"],
        how="left",
    )

    edges_ped_with_flow["ped_flow_daily"] = edges_ped_with_flow["ped_flow_daily"].fillna(0.0)

    max_ped_flow = edges_ped_with_flow["ped_flow_daily"].max()
    if max_ped_flow > 0:
        edges_ped_with_flow["ped_flow_norm"] = edges_ped_with_flow["ped_flow_daily"] / max_ped_flow
    else:
        edges_ped_with_flow["ped_flow_norm"] = 0.0

    gdf_ped_edges = gpd.GeoDataFrame(
        edges_ped_with_flow,
        geometry="geometry",
        crs=edges_ped_gdf.crs,
    ).to_crs(epsg=2056)

    logger.info(
        "Ped edges with ped_flow_daily > 0: %d / %d (CRS=%s)",
        int((gdf_ped_edges["ped_flow_daily"] > 0).sum()),
        len(gdf_ped_edges),
        gdf_ped_edges.crs,
    )

    # --------------------------------------------------------------
    # 8. Save outputs (two files: bike + ped)
    # --------------------------------------------------------------
    save_geodataframe(
        gdf_bike_edges,
        output_format=output_format,
        local_root=None if use_s3 else local_root,
        output_path_or_key=bike_output_key,
        use_s3=use_s3,
        output_bucket=output_bucket,
    )

    save_geodataframe(
        gdf_ped_edges,
        output_format=output_format,
        local_root=None if use_s3 else local_root,
        output_path_or_key=ped_output_key,
        use_s3=use_s3,
        output_bucket=output_bucket,
    )

    logger.info("Done. Bike rows: %d, Ped rows: %d", len(gdf_bike_edges), len(gdf_ped_edges))


if __name__ == "__main__":
    main()
