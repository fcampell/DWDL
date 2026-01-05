#!/usr/bin/env python
"""
Pedestrian & cyclist flow simulation for Zürich – from counting stations to edge-time flows.

ADAPTATION (Dec 2025):
- Aggregate station counts per HOUR-OF-DAY (0..23) across all dates (mean across days).
- Propagate flows for each hour bucket.
- Add deterministic integer edge_id (per network: bike vs ped).

OUTPUT MINIMIZATION (this version):
- Bike edges: only [edge_id, geometry] in EPSG:2056
- Ped edges : only [edge_id, geometry] in EPSG:2056
- Bike flows: only [edge_id, hour, flow, flow_norm]
- Ped flows : only [edge_id, hour, flow, flow_norm]

This is meant to minimize downstream DB load. Join key is edge_id.
"""

import os
import logging
import tempfile
from collections import defaultdict

import boto3
import numpy as np
import pandas as pd
import geopandas as gpd
from pandas.api.types import is_object_dtype
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
# Generic helpers
# -------------------------------------------------------------------
def highway_is_any(highway_value, target_types):
    if isinstance(highway_value, list):
        return any(h in target_types for h in highway_value)
    return highway_value in target_types


def bearing_diff_deg(b1, b2):
    if b1 is None or b2 is None:
        return np.nan
    return abs((b1 - b2 + 180) % 360 - 180)


def direction_weight(seed_bearing, edge_bearing, max_diff, exponent):
    d = bearing_diff_deg(seed_bearing, edge_bearing)
    if np.isnan(d) or d >= max_diff:
        return 0.0
    base = 1.0 - (d / max_diff)
    return float(base ** exponent)


def distance_decay(flow_value, distance_m, alpha):
    if distance_m <= 0:
        return float(flow_value)
    return float(flow_value * np.exp(-alpha * distance_m))


def coerce_object_to_string_for_parquet(df: pd.DataFrame, geometry_col: str | None = None):
    df = df.copy()
    for col in df.columns:
        if geometry_col is not None and col == geometry_col:
            continue
        if is_object_dtype(df[col]):
            df[col] = df[col].astype("string")
    return df


def prepare_gdf_for_parquet(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    gdf = gdf.copy()
    geom_col = gdf.geometry.name
    for col in gdf.columns:
        if col == geom_col:
            continue
        if is_object_dtype(gdf[col]):
            gdf[col] = gdf[col].astype("string")
    return gdf


def save_gdf_local(gdf: gpd.GeoDataFrame, path: str, as_gpkg: bool):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if as_gpkg:
        logger.info("Writing local GPKG: %s", path)
        gdf.to_file(path, driver="GPKG")
    else:
        logger.info("Writing local GeoParquet: %s", path)
        prepare_gdf_for_parquet(gdf).to_parquet(path, index=False)


def save_gdf_s3(gdf: gpd.GeoDataFrame, bucket: str, key: str, as_gpkg: bool):
    s3 = boto3.client("s3")
    with tempfile.TemporaryDirectory() as tmpdir:
        if as_gpkg:
            tmp_path = os.path.join(tmpdir, "tmp_output.gpkg")
            gdf.to_file(tmp_path, driver="GPKG")
        else:
            tmp_path = os.path.join(tmpdir, "tmp_output.parquet")
            prepare_gdf_for_parquet(gdf).to_parquet(tmp_path, index=False)

        logger.info("Uploading to s3://%s/%s", bucket, key)
        s3.upload_file(tmp_path, bucket, key)


# -------------------------------------------------------------------
# I/O helpers
# -------------------------------------------------------------------
def load_parquet_local(root: str, rel_path: str) -> pd.DataFrame:
    path = os.path.join(root, rel_path)
    logger.info("Reading local Parquet: %s", path)
    if not os.path.exists(path):
        raise FileNotFoundError(f"Input file not found: {path}")
    return pd.read_parquet(path)


def load_parquet_local_multi(root: str, rel_dir: str, filename_substr: str) -> pd.DataFrame:
    dir_path = os.path.join(root, rel_dir)
    logger.info("Reading multiple local Parquet files from: %s (filter=%r)", dir_path, filename_substr)
    if not os.path.isdir(dir_path):
        raise FileNotFoundError(f"Input directory not found: {dir_path}")

    dfs = []
    for fname in sorted(os.listdir(dir_path)):
        if not fname.lower().endswith(".parquet"):
            continue
        if filename_substr and (filename_substr not in fname):
            continue
        full_path = os.path.join(dir_path, fname)
        logger.info("  - including %s", full_path)
        dfs.append(pd.read_parquet(full_path))

    if not dfs:
        raise FileNotFoundError(f"No Parquet files in {dir_path} matching substring {filename_substr!r}")

    df = pd.concat(dfs, ignore_index=True)
    logger.info("Concatenated local Parquet: %d rows, %d cols", len(df), len(df.columns))
    return df


def load_parquet_s3(bucket: str, key: str) -> pd.DataFrame:
    logger.info("Reading Parquet from s3://%s/%s", bucket, key)
    s3 = boto3.client("s3")
    with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
        s3.download_fileobj(bucket, key, tmp)
        tmp.flush()
        tmp.seek(0)
        return pd.read_parquet(tmp.name)


def load_parquet_s3_multi(bucket: str, prefix: str, filename_substr: str) -> pd.DataFrame:
    logger.info("Reading multiple Parquet files from s3://%s/%s* (filter=%r)", bucket, prefix, filename_substr)
    s3 = boto3.client("s3")
    dfs = []
    continuation_token = None

    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if continuation_token:
            kwargs["ContinuationToken"] = continuation_token

        resp = s3.list_objects_v2(**kwargs)
        for obj in resp.get("Contents", []):
            k = obj["Key"]
            fname = os.path.basename(k)
            if not fname.lower().endswith(".parquet"):
                continue
            if filename_substr and (filename_substr not in fname):
                continue

            logger.info("  - including s3://%s/%s", bucket, k)
            with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
                s3.download_fileobj(bucket, k, tmp)
                tmp.flush()
                tmp.seek(0)
                dfs.append(pd.read_parquet(tmp.name))

        if resp.get("IsTruncated"):
            continuation_token = resp.get("NextContinuationToken")
        else:
            break

    if not dfs:
        raise FileNotFoundError(f"No Parquet files under s3://{bucket}/{prefix} matching substring {filename_substr!r}")

    df = pd.concat(dfs, ignore_index=True)
    logger.info("Concatenated S3 Parquet: %d rows, %d cols", len(df), len(df.columns))
    return df


def save_df_parquet_local(df: pd.DataFrame, path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df_to_write = coerce_object_to_string_for_parquet(df, geometry_col=None)
    logger.info("Writing local Parquet: %s", path)
    df_to_write.to_parquet(path, index=False)


def save_df_parquet_s3(df: pd.DataFrame, bucket: str, key: str):
    df_to_write = coerce_object_to_string_for_parquet(df, geometry_col=None)
    s3 = boto3.client("s3")
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = os.path.join(tmpdir, "tmp_flows.parquet")
        df_to_write.to_parquet(tmp_path, index=False)
        logger.info("Uploading to s3://%s/%s", bucket, key)
        s3.upload_file(tmp_path, bucket, key)


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


def propagate_bike_flows(G: nx.MultiDiGraph, seeds_df: pd.DataFrame, params: dict):
    max_distance_m = params["max_distance_m"]
    min_flow = params["min_flow"]
    decay_alpha = params["decay_alpha"]
    max_bearing_diff = params["max_bearing_diff"]
    bearing_exp = params["bearing_weight_exp"]

    edge_flow = defaultdict(float)

    for _, seed in seeds_df.iterrows():
        base_flow = float(seed["VELO_HOUR"])
        if base_flow <= 0:
            continue

        u0, v0, k0 = seed["u"], seed["v"], seed["key"]
        if pd.isna(u0) or pd.isna(v0) or pd.isna(k0):
            continue
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

                weights, meta = [], []
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
                        dir_w = direction_weight(seed_bearing, edge_bearing, max_bearing_diff, bearing_exp)

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

        if flow_fwd > 0:
            propagate_from_edge(u0, v0, k0, flow_fwd, bearing_fwd)

        if flow_back > 0 and G.has_edge(v0, u0):
            key_back = list(G[v0][u0].keys())[0]
            edge_data_back = G[v0][u0][key_back]
            bearing_back = edge_data_back.get("bearing", None)
            propagate_from_edge(v0, u0, key_back, flow_back, bearing_back)

    return edge_flow


def propagate_ped_flows(G: nx.MultiDiGraph, seeds_df: pd.DataFrame, params: dict):
    max_distance_m = params["max_distance_m"]
    min_flow = params["min_flow"]
    decay_alpha = params["decay_alpha"]
    max_bearing_diff = params["max_bearing_diff"]
    bearing_exp = params["bearing_weight_exp"]

    edge_flow = defaultdict(float)

    for _, seed in seeds_df.iterrows():
        base_flow = float(seed["FUSS_HOUR"])
        if base_flow <= 0:
            continue

        u0, v0, k0 = seed["u"], seed["v"], seed["key"]
        if pd.isna(u0) or pd.isna(v0) or pd.isna(k0):
            continue
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

                weights, meta = [], []
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
                        dir_w = direction_weight(seed_bearing, edge_bearing, max_bearing_diff, bearing_exp)

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
    input_bucket = os.getenv("INPUT_BUCKET")
    input_key = os.getenv("INPUT_KEY")
    input_prefix = os.getenv("INPUT_PREFIX")
    input_dir = os.getenv("INPUT_DIR")

    output_bucket = os.getenv("OUTPUT_BUCKET")
    output_key = os.getenv("OUTPUT_KEY")
    version_tag = os.getenv("VERSION_TAG", "").strip()

    if output_bucket and "/" in output_bucket:
        raise ValueError("OUTPUT_BUCKET must be bucket name only. Use OUTPUT_KEY for prefixes.")

    local_root = os.getenv("LOCAL_BUCKET_ROOT", "/data")
    default_parquet_name = os.getenv(
        "PEDESTRIAN_VELO_PARQUET_FILE",
        "2025_verkehrszaehlungen_werte_fussgaenger_velo.parquet",
    )
    filename_substr = os.getenv("INPUT_FILENAME_SUBSTR", "pedestrian")

    # Aggregation function for station-hour across days
    # "mean" = typical day; "sum" = totals across all observed days
    aggfunc = os.getenv("AGG_FUNC", "mean").strip().lower()
    if aggfunc not in {"mean", "sum", "median"}:
        logger.warning("AGG_FUNC=%r not in {mean,sum,median}. Using as provided to pandas.", aggfunc)

    use_s3 = bool(input_bucket and output_bucket and (input_key or input_prefix))

    if use_s3:
        base_prefix = (output_key or "pedbike_flows").rstrip("/")
        logger.info("Running in S3 mode. Output base: s3://%s/%s", output_bucket, base_prefix)
    else:
        output_dir = os.getenv("OUTPUT_DIR", "pedbike_flows")
        base_prefix = output_dir
        logger.info("Running in local mode. Output base: %s", os.path.join(local_root, base_prefix))

    # --------------------------------------------------------------
    # 1) Load input Parquet(s)
    # --------------------------------------------------------------
    if use_s3:
        if input_prefix:
            df_raw = load_parquet_s3_multi(input_bucket, input_prefix, filename_substr)
        else:
            effective_key = input_key or default_parquet_name
            df_raw = load_parquet_s3(input_bucket, effective_key)
    else:
        if input_dir:
            df_raw = load_parquet_local_multi(local_root, input_dir, filename_substr)
        else:
            effective_key = input_key or default_parquet_name
            df_raw = load_parquet_local(local_root, effective_key)

    logger.info("Loaded raw counts: %d rows, %d cols", len(df_raw), len(df_raw.columns))

    # --------------------------------------------------------------
    # 2) Parse datetime + numeric + hour buckets
    # --------------------------------------------------------------
    df_raw["DATUM"] = pd.to_datetime(df_raw["DATUM"], errors="coerce")
    df_raw = df_raw[df_raw["DATUM"].notna()].copy()

    for col in ["VELO_IN", "VELO_OUT", "FUSS_IN", "FUSS_OUT"]:
        if col not in df_raw.columns:
            df_raw[col] = 0.0
        df_raw[col] = pd.to_numeric(df_raw[col], errors="coerce").fillna(0.0)

    df_raw["DATE"] = df_raw["DATUM"].dt.date
    df_raw["HOUR"] = df_raw["DATUM"].dt.hour.astype("int32")

    df_raw["OST"] = pd.to_numeric(df_raw["OST"], errors="coerce")
    df_raw["NORD"] = pd.to_numeric(df_raw["NORD"], errors="coerce")

    # --------------------------------------------------------------
    # 3) Station coords (stable per FK_STANDORT)
    # --------------------------------------------------------------
    if "FK_STANDORT" not in df_raw.columns:
        raise ValueError("Expected 'FK_STANDORT' in raw input (station identifier).")

    df_coords = df_raw.groupby("FK_STANDORT", as_index=False)[["OST", "NORD"]].first()

    # --------------------------------------------------------------
    # 4) Aggregate station flows to hour-of-day (0..23)
    # --------------------------------------------------------------
    df_station_hour = (
        df_raw.groupby(["FK_STANDORT", "HOUR"], as_index=False)[["VELO_IN", "VELO_OUT", "FUSS_IN", "FUSS_OUT"]]
        .agg(aggfunc)
    )
    df_station_hour = df_station_hour.merge(df_coords, on="FK_STANDORT", how="left")

    df_station_hour["VELO_HOUR"] = df_station_hour["VELO_IN"] + df_station_hour["VELO_OUT"]
    df_station_hour["FUSS_HOUR"] = df_station_hour["FUSS_IN"] + df_station_hour["FUSS_OUT"]

    df_station_hour["VELO_IN_SHARE"] = df_station_hour["VELO_IN"].div(df_station_hour["VELO_HOUR"]).fillna(0.0)
    df_station_hour["VELO_OUT_SHARE"] = df_station_hour["VELO_OUT"].div(df_station_hour["VELO_HOUR"]).fillna(0.0)
    df_station_hour["FUSS_IN_SHARE"] = df_station_hour["FUSS_IN"].div(df_station_hour["FUSS_HOUR"]).fillna(0.0)
    df_station_hour["FUSS_OUT_SHARE"] = df_station_hour["FUSS_OUT"].div(df_station_hour["FUSS_HOUR"]).fillna(0.0)

    # --------------------------------------------------------------
    # 5) Build station GeoDataFrames (LV95 -> WGS84)
    # --------------------------------------------------------------
    gdf_stations = gpd.GeoDataFrame(
        df_station_hour.copy(),
        geometry=gpd.points_from_xy(df_station_hour["OST"], df_station_hour["NORD"]),
        crs="EPSG:2056",
    )
    gdf_stations_4326 = gdf_stations.to_crs(epsg=4326)

    gdf_bike_stations = gdf_stations_4326[gdf_stations_4326["VELO_HOUR"] > 0].copy()
    gdf_ped_stations = gdf_stations_4326[gdf_stations_4326["FUSS_HOUR"] > 0].copy()

    logger.info("Station-hour rows: %d", len(gdf_stations_4326))
    logger.info("Bike station-hour rows: %d", len(gdf_bike_stations))
    logger.info("Ped  station-hour rows: %d", len(gdf_ped_stations))

    # --------------------------------------------------------------
    # 6) OSM networks
    # --------------------------------------------------------------
    ox.settings.use_cache = True
    ox.settings.log_console = False
    place_name = os.getenv("OSM_PLACE", "Zürich, Switzerland")

    logger.info("Downloading bike network for %s ...", place_name)
    G_bike = ox.graph_from_place(place_name, network_type="bike", simplify=True)
    G_bike = ox.add_edge_speeds(G_bike)
    G_bike = ox.add_edge_travel_times(G_bike)
    G_bike = ox.add_edge_bearings(G_bike)
    edges_bike_gdf = ox.graph_to_gdfs(G_bike, nodes=False, edges=True)

    logger.info("Downloading pedestrian network for %s ...", place_name)
    G_ped = ox.graph_from_place(place_name, network_type="walk", simplify=True)
    G_ped = ox.add_edge_speeds(G_ped)
    G_ped = ox.add_edge_travel_times(G_ped)
    G_ped = ox.add_edge_bearings(G_ped)
    edges_ped_gdf = ox.graph_to_gdfs(G_ped, nodes=False, edges=True)

    # --------------------------------------------------------------
    # 7) Snap station points to nearest edges (LV95)
    # --------------------------------------------------------------
    # Bike snapping
    gdf_bike_proj = gdf_bike_stations.to_crs(epsg=2056)
    edges_bike_proj = edges_bike_gdf.to_crs(epsg=2056).reset_index()
    edges_bike_snap = edges_bike_proj[["u", "v", "key", "geometry"]].copy()

    bike_station_edges = gpd.sjoin_nearest(
        gdf_bike_proj,
        edges_bike_snap,
        how="left",
        distance_col="snap_distance_m",
    ).drop(columns=["geometry"]).copy()

    # Ped snapping
    gdf_ped_proj = gdf_ped_stations.to_crs(epsg=2056)
    edges_ped_proj = edges_ped_gdf.to_crs(epsg=2056).reset_index()
    edges_ped_snap = edges_ped_proj[["u", "v", "key", "geometry"]].copy()

    ped_station_edges = gpd.sjoin_nearest(
        gdf_ped_proj,
        edges_ped_snap,
        how="left",
        distance_col="snap_distance_m",
    ).drop(columns=["geometry"]).copy()

    # --------------------------------------------------------------
    # 8) Build seeds per hour-of-day
    # --------------------------------------------------------------
    df_bike_seeds = bike_station_edges.copy()
    df_ped_seeds = ped_station_edges.copy()

    # --------------------------------------------------------------
    # 9) Propagate flows for each hour bucket (0..23)
    # --------------------------------------------------------------
    bike_flow_records = []
    if not df_bike_seeds.empty:
        hours = sorted(df_bike_seeds["HOUR"].dropna().unique().tolist())
        for h in hours:
            seeds_h = df_bike_seeds[df_bike_seeds["HOUR"] == h]
            if seeds_h["VELO_HOUR"].sum() <= 0:
                continue
            logger.info("Propagating bike flows for HOUR=%d ...", int(h))
            edge_flows_h = propagate_bike_flows(G_bike, seeds_h, BIKE_MODEL_PARAMS)
            for (u, v, k), flow in edge_flows_h.items():
                bike_flow_records.append({"hour": int(h), "u": u, "v": v, "key": k, "bike_flow": float(flow)})

    df_bike_flows_all = pd.DataFrame(bike_flow_records)
    logger.info("Bike flow records (edge×hour): %d", len(df_bike_flows_all))

    ped_flow_records = []
    if not df_ped_seeds.empty:
        hours = sorted(df_ped_seeds["HOUR"].dropna().unique().tolist())
        for h in hours:
            seeds_h = df_ped_seeds[df_ped_seeds["HOUR"] == h]
            if seeds_h["FUSS_HOUR"].sum() <= 0:
                continue
            logger.info("Propagating ped flows for HOUR=%d ...", int(h))
            edge_flows_h = propagate_ped_flows(G_ped, seeds_h, PED_MODEL_PARAMS)
            for (u, v, k), flow in edge_flows_h.items():
                ped_flow_records.append({"hour": int(h), "u": u, "v": v, "key": k, "ped_flow": float(flow)})

    df_ped_flows_all = pd.DataFrame(ped_flow_records)
    logger.info("Ped flow records (edge×hour): %d", len(df_ped_flows_all))

    # --------------------------------------------------------------
    # 10) Build minimal edges + edge_id mapping (deterministic)
    # --------------------------------------------------------------
    def build_edges_and_idmap(edges_gdf: gpd.GeoDataFrame) -> tuple[gpd.GeoDataFrame, pd.DataFrame]:
        edges = edges_gdf.reset_index()[["u", "v", "key", "geometry"]].copy()
        edges = edges.sort_values(["u", "v", "key"]).reset_index(drop=True)
        edges["edge_id"] = np.arange(1, len(edges) + 1, dtype=np.int64)

        # minimal outputs
        gdf_edges_min = gpd.GeoDataFrame(
            edges[["edge_id", "geometry"]].copy(),
            geometry="geometry",
            crs=edges_gdf.crs,
        ).to_crs(epsg=2056)

        idmap = edges[["u", "v", "key", "edge_id"]].copy()
        return gdf_edges_min, idmap

    gdf_bike_edges, bike_idmap = build_edges_and_idmap(edges_bike_gdf)
    gdf_ped_edges, ped_idmap = build_edges_and_idmap(edges_ped_gdf)

    # --------------------------------------------------------------
    # 11) Attach edge_id to flows and reduce to minimal flow columns
    # --------------------------------------------------------------
    # Bike flows
    if not df_bike_flows_all.empty:
        df_bike = df_bike_flows_all.merge(bike_idmap, on=["u", "v", "key"], how="left")
        if df_bike["edge_id"].isna().any():
            raise ValueError("Some bike flows could not be mapped to edge_id (unexpected).")
        df_bike["edge_id"] = df_bike["edge_id"].astype("int64")
        df_bike["hour"] = df_bike["hour"].astype("int32")

        max_flow = float(df_bike["bike_flow"].max()) if len(df_bike) else 0.0
        df_bike["flow_norm"] = df_bike["bike_flow"] / max_flow if max_flow > 0 else 0.0

        df_bike_flows_out = df_bike[["edge_id", "hour", "bike_flow", "flow_norm"]].rename(
            columns={"bike_flow": "flow"}
        )
    else:
        df_bike_flows_out = pd.DataFrame(columns=["edge_id", "hour", "flow", "flow_norm"])

    # Ped flows
    if not df_ped_flows_all.empty:
        df_ped = df_ped_flows_all.merge(ped_idmap, on=["u", "v", "key"], how="left")
        if df_ped["edge_id"].isna().any():
            raise ValueError("Some ped flows could not be mapped to edge_id (unexpected).")
        df_ped["edge_id"] = df_ped["edge_id"].astype("int64")
        df_ped["hour"] = df_ped["hour"].astype("int32")

        max_flow = float(df_ped["ped_flow"].max()) if len(df_ped) else 0.0
        df_ped["flow_norm"] = df_ped["ped_flow"] / max_flow if max_flow > 0 else 0.0

        df_ped_flows_out = df_ped[["edge_id", "hour", "ped_flow", "flow_norm"]].rename(
            columns={"ped_flow": "flow"}
        )
    else:
        df_ped_flows_out = pd.DataFrame(columns=["edge_id", "hour", "flow", "flow_norm"])

    # --------------------------------------------------------------
    # 12) Save outputs
    # --------------------------------------------------------------
    def make_filename(stub: str) -> str:
        return f"{version_tag}_{stub}" if version_tag else stub

    if use_s3:
        def s3_key(filename: str) -> str:
            return f"{base_prefix}/{filename}" if base_prefix else filename

        # Bike (minimal)
        save_gdf_s3(gdf_bike_edges, output_bucket, s3_key(make_filename("bike_edges.gpkg")), as_gpkg=True)
        save_gdf_s3(gdf_bike_edges, output_bucket, s3_key(make_filename("bike_edges.parquet")), as_gpkg=False)
        save_df_parquet_s3(df_bike_flows_out, output_bucket, s3_key(make_filename("bike_flows.parquet")))

        # Ped (minimal)
        save_gdf_s3(gdf_ped_edges, output_bucket, s3_key(make_filename("ped_edges.gpkg")), as_gpkg=True)
        save_gdf_s3(gdf_ped_edges, output_bucket, s3_key(make_filename("ped_edges.parquet")), as_gpkg=False)
        save_df_parquet_s3(df_ped_flows_out, output_bucket, s3_key(make_filename("ped_flows.parquet")))

    else:
        base_path = os.path.join(local_root, base_prefix)
        os.makedirs(base_path, exist_ok=True)

        def local_path(filename: str) -> str:
            return os.path.join(base_path, filename)

        # Bike (minimal)
        save_gdf_local(gdf_bike_edges, local_path(make_filename("bike_edges.gpkg")), as_gpkg=True)
        save_gdf_local(gdf_bike_edges, local_path(make_filename("bike_edges.parquet")), as_gpkg=False)
        save_df_parquet_local(df_bike_flows_out, local_path(make_filename("bike_flows.parquet")))

        # Ped (minimal)
        save_gdf_local(gdf_ped_edges, local_path(make_filename("ped_edges.gpkg")), as_gpkg=True)
        save_gdf_local(gdf_ped_edges, local_path(make_filename("ped_edges.parquet")), as_gpkg=False)
        save_df_parquet_local(df_ped_flows_out, local_path(make_filename("ped_flows.parquet")))

    logger.info(
        "Done (MINIMAL OUTPUT). "
        "Bike edges=%d, bike flows=%d | Ped edges=%d, ped flows=%d",
        len(gdf_bike_edges), len(df_bike_flows_out),
        len(gdf_ped_edges), len(df_ped_flows_out),
    )


if __name__ == "__main__":
    main()
