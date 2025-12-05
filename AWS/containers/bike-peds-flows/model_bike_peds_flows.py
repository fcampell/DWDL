#!/usr/bin/env python
"""
Pedestrian & cyclist flow simulation for Zürich – from counting stations to edge-time flows.

Changes vs previous version:
- Input can be a *folder/prefix* containing many Parquet files.
- Only files whose filenames contain a given substring (e.g. "pedestrian")
  are read and concatenated before processing.
- Still keeps all observations (hourly), models flows per timestamp.
- **Outputs are now only normalized forms**:
  - geometries (one row per edge) in GeoPackage + GeoParquet
  - flows (edge × time) in plain Parquet.

--------------------------------------------------
Environment variables
--------------------------------------------------

I/O mode (local vs S3)
~~~~~~~~~~~~~~~~~~~~~~
If all of these are set, we run in S3 mode:
- INPUT_BUCKET          : S3 bucket for input Parquet files
- OUTPUT_BUCKET         : S3 bucket for outputs

You can then choose between:
  Single-file mode:
    - INPUT_KEY         : S3 key for one Parquet file

  Multi-file mode:
    - INPUT_PREFIX      : S3 prefix (i.e. "folder") with many Parquet files
    - INPUT_FILENAME_SUBSTR (optional) : substring that must be in filename
                                         (default: "pedestrian")

Otherwise, we run in local mode:
- LOCAL_BUCKET_ROOT     : root folder inside container (default: "/data")

Input choice:
  Single-file mode:
    - INPUT_KEY         : Parquet file relative to LOCAL_BUCKET_ROOT

  Multi-file mode:
    - INPUT_DIR         : folder relative to LOCAL_BUCKET_ROOT with many Parquet files
    - INPUT_FILENAME_SUBSTR (optional) : substring that must be in filename
                                         (default: "pedestrian")

If neither INPUT_DIR/INPUT_PREFIX nor INPUT_KEY is set, we fall back to:
- PEDESTRIAN_VELO_PARQUET_FILE (default: "2025_verkehrszaehlungen_werte_fussgaenger_velo.parquet")

Outputs
~~~~~~~
Per mode (bike & pedestrian) we now write **only normalized outputs**:

Bike:
  1) Normalized edges (GeoPackage, geometries only, one row per edge, CRS=EPSG:2056):
       - <base>_bike_edges.gpkg    (S3) or bike_edges.gpkg (local)
  2) Normalized edges (GeoParquet, geometries only, one row per edge, CRS=EPSG:2056):
       - <base>_bike_edges.parquet or bike_edges.parquet
  3) Normalized flows (Parquet, non-geo, edge × time):
       - <base>_bike_flows.parquet or bike_flows.parquet

Pedestrian: analogous with "ped_" names.

Output base:
- S3  : OUTPUT_PREFIX (e.g. "silver/zurich_pedbike_flows")
- Local: OUTPUT_DIR under LOCAL_BUCKET_ROOT (e.g. "silver/zurich_pedbike_flows")

Other config
~~~~~~~~~~~~
- LOG_LEVEL         : "INFO" (default), "DEBUG", ...
- OSM_PLACE         : place name for OSMnx (default: "Zürich, Switzerland")
- INPUT_FILENAME_SUBSTR : default "pedestrian"

Example – local, multi-file input
---------------------------------
docker run --rm \
  -v "$(pwd)/local_bucket:/data" \
  -e LOCAL_BUCKET_ROOT=/data \
  -e INPUT_DIR="bronze/pedbike_counts_2025" \
  -e INPUT_FILENAME_SUBSTR="pedestrian" \
  -e OUTPUT_DIR="silver/zurich_pedbike_flows" \
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
from pandas.api.types import is_object_dtype
from shapely.geometry import Point
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


def coerce_object_to_string_for_parquet(df: pd.DataFrame, geometry_col: str | None = None):
    """
    Convert all object columns (except geometry) to string before writing to Parquet.
    Safe for plain DataFrames (flows tables).
    """
    df = df.copy()
    for col in df.columns:
        if geometry_col is not None and col == geometry_col:
            continue
        if is_object_dtype(df[col]):
            df[col] = df[col].astype("string")
    return df


def prepare_gdf_for_parquet(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    For GeoParquet: keep GeoDataFrame (to preserve geometry metadata),
    but coerce all non-geometry object columns to string.
    """
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
        gdf_to_write = prepare_gdf_for_parquet(gdf)
        gdf_to_write.to_parquet(path, index=False)


def save_gdf_s3(
    gdf: gpd.GeoDataFrame,
    bucket: str,
    key: str,
    as_gpkg: bool,
):
    s3 = boto3.client("s3")
    with tempfile.TemporaryDirectory() as tmpdir:
        if as_gpkg:
            tmp_name = "tmp_output.gpkg"
            tmp_path = os.path.join(tmpdir, tmp_name)
            gdf.to_file(tmp_path, driver="GPKG")
        else:
            tmp_name = "tmp_output.parquet"
            tmp_path = os.path.join(tmpdir, tmp_name)
            gdf_to_write = prepare_gdf_for_parquet(gdf)
            gdf_to_write.to_parquet(tmp_path, index=False)

        size_bytes = os.path.getsize(tmp_path)
        logger.info("Temporary output %s (size=%d bytes)", tmp_path, size_bytes)
        logger.info("Uploading to s3://%s/%s", bucket, key)
        s3.upload_file(tmp_path, bucket, key)


# -------------------------------------------------------------------
# I/O helpers (Parquet-based, single + multi-file)
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
        raise FileNotFoundError(
            f"No Parquet files in {dir_path} matching substring {filename_substr!r}"
        )

    df = pd.concat(dfs, ignore_index=True)
    logger.info("Concatenated local Parquet: %d rows, %d columns", len(df), len(df.columns))
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
    logger.info(
        "Reading multiple Parquet files from s3://%s/%s* (filter=%r)",
        bucket,
        prefix,
        filename_substr,
    )
    s3 = boto3.client("s3")
    dfs = []
    continuation_token = None

    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if continuation_token:
            kwargs["ContinuationToken"] = continuation_token

        resp = s3.list_objects_v2(**kwargs)
        contents = resp.get("Contents", [])
        for obj in contents:
            key = obj["Key"]
            fname = os.path.basename(key)
            if not fname.lower().endswith(".parquet"):
                continue
            if filename_substr and (filename_substr not in fname):
                continue

            logger.info("  - including s3://%s/%s", bucket, key)
            with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
                s3.download_fileobj(bucket, key, tmp)
                tmp.flush()
                tmp.seek(0)
                dfs.append(pd.read_parquet(tmp.name))

        if resp.get("IsTruncated"):
            continuation_token = resp.get("NextContinuationToken")
        else:
            break

    if not dfs:
        raise FileNotFoundError(
            f"No Parquet files under s3://{bucket}/{prefix} matching substring {filename_substr!r}"
        )

    df = pd.concat(dfs, ignore_index=True)
    logger.info("Concatenated S3 Parquet: %d rows, %d columns", len(df), len(df.columns))
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
        size_bytes = os.path.getsize(tmp_path)
        logger.info("Temporary Parquet %s (size=%d bytes)", tmp_path, size_bytes)
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
    """
    Propagate cyclist flows from seed edges through the bike network for ONE timestep.

    seeds_df: FK_STANDORT, VELO_DAY, VELO_IN_SHARE, VELO_OUT_SHARE, u, v, key
    """
    max_distance_m = params["max_distance_m"]
    min_flow = params["min_flow"]
    decay_alpha = params["decay_alpha"]
    max_bearing_diff = params["max_bearing_diff"]
    bearing_exp = params["bearing_weight_exp"]

    edge_flow = defaultdict(float)

    for idx, seed in seeds_df.iterrows():
        base_flow = float(seed["VELO_DAY"])
        if base_flow <= 0:
            continue

        u0 = seed["u"]
        v0 = seed["v"]
        k0 = seed["key"]

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


def propagate_ped_flows(G: nx.MultiDiGraph, seeds_df: pd.DataFrame, params: dict):
    """
    Propagate pedestrian flows from seed edges through the pedestrian network for ONE timestep.

    seeds_df: FK_STANDORT, FUSS_DAY, FUSS_IN_SHARE, FUSS_OUT_SHARE, u, v, key
    """
    max_distance_m = params["max_distance_m"]
    min_flow = params["min_flow"]
    decay_alpha = params["decay_alpha"]
    max_bearing_diff = params["max_bearing_diff"]
    bearing_exp = params["bearing_weight_exp"]

    edge_flow = defaultdict(float)

    for idx, seed in seeds_df.iterrows():
        base_flow = float(seed["FUSS_DAY"])
        if base_flow <= 0:
            continue

        u0 = seed["u"]
        v0 = seed["v"]
        k0 = seed["key"]

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
    # I/O config
    # --------------------------------------------------------------
    input_bucket = os.getenv("INPUT_BUCKET")
    input_key = os.getenv("INPUT_KEY")
    input_prefix = os.getenv("INPUT_PREFIX")  # for S3 multi-file
    input_dir = os.getenv("INPUT_DIR")        # for local multi-file
    output_bucket = os.getenv("OUTPUT_BUCKET")
    output_prefix = os.getenv("OUTPUT_PREFIX")

    local_root = os.getenv("LOCAL_BUCKET_ROOT", "/data")
    default_parquet_name = os.getenv(
        "PEDESTRIAN_VELO_PARQUET_FILE",
        "2025_verkehrszaehlungen_werte_fussgaenger_velo.parquet",
    )
    filename_substr = os.getenv("INPUT_FILENAME_SUBSTR", "pedestrian")

    # Decide whether we are in S3 mode
    use_s3 = bool(input_bucket and output_bucket and (input_key or input_prefix))

    # Base name for outputs
    if use_s3:
        base_name = output_prefix or "pedbike_flows"
        logger.info("Running in S3 mode.")
        if input_prefix:
            logger.info(
                "Input multi-file: s3://%s/%s* (filter=%r)",
                input_bucket,
                input_prefix,
                filename_substr,
            )
        else:
            effective_key = input_key or default_parquet_name
            logger.info("Input single file: s3://%s/%s", input_bucket, effective_key)
        logger.info("Output base prefix: s3://%s/%s", output_bucket, base_name)
    else:
        output_dir = os.getenv("OUTPUT_DIR", "pedbike_flows")
        base_name = output_dir
        logger.info("Running in local mode.")
        logger.info("Local root: %s", local_root)

        if input_dir:
            logger.info(
                "Input multi-file: %s (relative to LOCAL_BUCKET_ROOT, filter=%r)",
                input_dir,
                filename_substr,
            )
        else:
            effective_key = input_key or default_parquet_name
            logger.info(
                "Input single file: %s",
                os.path.join(local_root, effective_key),
            )
        logger.info("Output base dir: %s", os.path.join(local_root, output_dir))

    # --------------------------------------------------------------
    # 1. Load input Parquet(s)
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

    logger.info("Loaded raw counts: %d rows, %d columns", len(df_raw), len(df_raw.columns))

    # --------------------------------------------------------------
    # 2. Parse datetime & numeric flows (keep all observations)
    # --------------------------------------------------------------
    df_raw["DATUM"] = pd.to_datetime(df_raw["DATUM"], errors="coerce")

    agg_cols = ["VELO_IN", "VELO_OUT", "FUSS_IN", "FUSS_OUT"]
    for col in agg_cols:
        if col in df_raw.columns:
            df_raw[col] = pd.to_numeric(df_raw[col], errors="coerce").fillna(0.0)
        else:
            df_raw[col] = 0.0

    df_raw["VELO_DAY"] = df_raw["VELO_IN"] + df_raw["VELO_OUT"]
    df_raw["FUSS_DAY"] = df_raw["FUSS_IN"] + df_raw["FUSS_OUT"]

    df_raw["VELO_IN_SHARE"] = df_raw["VELO_IN"].div(df_raw["VELO_DAY"]).fillna(0.0)
    df_raw["VELO_OUT_SHARE"] = df_raw["VELO_OUT"].div(df_raw["VELO_DAY"]).fillna(0.0)

    df_raw["FUSS_IN_SHARE"] = df_raw["FUSS_IN"].div(df_raw["FUSS_DAY"]).fillna(0.0)
    df_raw["FUSS_OUT_SHARE"] = df_raw["FUSS_OUT"].div(df_raw["FUSS_DAY"]).fillna(0.0)

    df_raw["DATE"] = df_raw["DATUM"].dt.date
    df_raw["HOUR"] = df_raw["DATUM"].dt.hour

    # Coordinates
    df_raw["OST"] = pd.to_numeric(df_raw["OST"], errors="coerce")
    df_raw["NORD"] = pd.to_numeric(df_raw["NORD"], errors="coerce")

    # Station-level coords (one row per FK_STANDORT)
    df_coords = (
        df_raw
        .groupby("FK_STANDORT", as_index=False)[["OST", "NORD"]]
        .first()
    )

    # Station-level total flows (for filtering bike/ped stations)
    station_flows = (
        df_raw
        .groupby("FK_STANDORT", as_index=False)[["VELO_DAY", "FUSS_DAY"]]
        .sum()
    )

    df_station = station_flows.merge(df_coords, on="FK_STANDORT", how="left")
    logger.info("Stations after merging coords: %d", len(df_station))

    # --------------------------------------------------------------
    # 3. Build station GeoDataFrames (LV95 -> WGS84)
    # --------------------------------------------------------------
    gdf_stations = gpd.GeoDataFrame(
        df_station.copy(),
        geometry=gpd.points_from_xy(df_station["OST"], df_station["NORD"]),
        crs="EPSG:2056",  # LV95
    )

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
    # 5. Snap stations to nearest edges (in LV95) to get u,v,key mapping
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
    ).to_crs(epsg=4326)

    bike_station_edges = bike_station_edges[
        [
            "FK_STANDORT",
            "u",
            "v",
            "key",
            "snap_distance_m",
        ]
    ].copy()

    logger.info(
        "Bike station-edge matches: %d (snap distance min=%.2f m, max=%.2f m)",
        len(bike_station_edges),
        float(bike_station_edges["snap_distance_m"].min())
        if len(bike_station_edges) > 0 else float("nan"),
        float(bike_station_edges["snap_distance_m"].max())
        if len(bike_station_edges) > 0 else float("nan"),
    )

    # Ped snapping
    gdf_ped_proj = gdf_ped_stations.to_crs(epsg=2056)
    edges_ped_proj = edges_ped_gdf.to_crs(epsg=2056).reset_index()
    edges_ped_snap = edges_ped_proj[["u", "v", "key", "geometry"]].copy()

    ped_station_edges = gpd.sjoin_nearest(
        gdf_ped_proj,
        edges_ped_snap,
        how="left",
        distance_col="snap_distance_m",
    ).to_crs(epsg=4326)

    ped_station_edges = ped_station_edges[
        [
            "FK_STANDORT",
            "u",
            "v",
            "key",
            "snap_distance_m",
        ]
    ].copy()

    logger.info(
        "Ped station-edge matches: %d (snap distance min=%.2f m, max=%.2f m)",
        len(ped_station_edges),
        float(ped_station_edges["snap_distance_m"].min())
        if len(ped_station_edges) > 0 else float("nan"),
        float(ped_station_edges["snap_distance_m"].max())
        if len(ped_station_edges) > 0 else float("nan"),
    )

    # --------------------------------------------------------------
    # 6. Build seeds per observation (keep all timestamps)
    # --------------------------------------------------------------
    df_bike_obs = df_raw[df_raw["VELO_DAY"] > 0].copy()
    df_ped_obs = df_raw[df_raw["FUSS_DAY"] > 0].copy()

    df_bike_seeds = df_bike_obs.merge(
        bike_station_edges,
        on="FK_STANDORT",
        how="left",
    )

    df_ped_seeds = df_ped_obs.merge(
        ped_station_edges,
        on="FK_STANDORT",
        how="left",
    )

    logger.info("Bike observations with VELO_DAY>0: %d", len(df_bike_seeds))
    logger.info("Ped observations with FUSS_DAY>0: %d", len(df_ped_seeds))

    # --------------------------------------------------------------
    # 7. Propagate flows for each timestamp (hourly)
    # --------------------------------------------------------------
    # Bike
    bike_flow_records = []
    if len(df_bike_seeds) > 0:
        bike_times = sorted(df_bike_seeds["DATUM"].dropna().unique())
        total_bike = len(bike_times)
        logger.info("Unique bike timesteps: %d", total_bike)

        next_pct_bike = 10  # next percentage threshold to log

        for i, tstamp in enumerate(bike_times, start=1):
            seeds_t = df_bike_seeds[df_bike_seeds["DATUM"] == tstamp]
            if seeds_t["VELO_DAY"].sum() <= 0:
                continue

            # existing per-timestep log (keep or drop, up to you)
            logger.info(
                "Propagating bike flows (%d/%d) for %s ...",
                i,
                total_bike,
                tstamp,
            )

            edge_flows_t = propagate_bike_flows(G_bike, seeds_t, BIKE_MODEL_PARAMS)

            for (u, v, k), flow in edge_flows_t.items():
                bike_flow_records.append(
                    {
                        "DATUM": tstamp,
                        "DATE": tstamp.date(),
                        "HOUR": tstamp.hour,
                        "u": u,
                        "v": v,
                        "key": k,
                        "bike_flow": flow,
                    }
                )

            # --- 10% progress logging ---
            if total_bike > 0:
                pct = int(100 * i / total_bike)
                if pct >= next_pct_bike:
                    logger.info(
                        "Bike propagation progress: %d%% (%d/%d timesteps)",
                        next_pct_bike,
                        i,
                        total_bike,
                    )
                    next_pct_bike += 10

        if total_bike > 0:
            logger.info(
                "Bike propagation progress: 100%% (%d/%d timesteps)",
                total_bike,
                total_bike,
            )

    df_bike_flows_all = pd.DataFrame(bike_flow_records)
    logger.info("Bike flow records (edge×time): %d", len(df_bike_flows_all))

    # Ped
    ped_flow_records = []
    if len(df_ped_seeds) > 0:
        ped_times = sorted(df_ped_seeds["DATUM"].dropna().unique())
        total_ped = len(ped_times)
        logger.info("Unique ped timesteps: %d", total_ped)

        next_pct_ped = 10  # next percentage threshold to log

        for i, tstamp in enumerate(ped_times, start=1):
            seeds_t = df_ped_seeds[df_ped_seeds["DATUM"] == tstamp]
            if seeds_t["FUSS_DAY"].sum() <= 0:
                continue

            logger.info(
                "Propagating ped flows (%d/%d) for %s ...",
                i,
                total_ped,
                tstamp,
            )

            edge_flows_t = propagate_ped_flows(G_ped, seeds_t, PED_MODEL_PARAMS)

            for (u, v, k), flow in edge_flows_t.items():
                ped_flow_records.append(
                    {
                        "DATUM": tstamp,
                        "DATE": tstamp.date(),
                        "HOUR": tstamp.hour,
                        "u": u,
                        "v": v,
                        "key": k,
                        "ped_flow": flow,
                    }
                )

            # --- 10% progress logging ---
            if total_ped > 0:
                pct = int(100 * i / total_ped)
                if pct >= next_pct_ped:
                    logger.info(
                        "Ped propagation progress: %d%% (%d/%d timesteps)",
                        next_pct_ped,
                        i,
                        total_ped,
                    )
                    next_pct_ped += 10

        if total_ped > 0:
            logger.info(
                "Ped propagation progress: 100%% (%d/%d timesteps)",
                total_ped,
                total_ped,
            )


    df_ped_flows_all = pd.DataFrame(ped_flow_records)
    logger.info("Ped flow records (edge×time): %d", len(df_ped_flows_all))

    # --------------------------------------------------------------
    # 8. Compute flow_norm per mode (0–1 across all edge×time)
    # --------------------------------------------------------------
    if not df_bike_flows_all.empty:
        max_bike_flow = df_bike_flows_all["bike_flow"].max()
        if max_bike_flow > 0:
            df_bike_flows_all["bike_flow_norm"] = df_bike_flows_all["bike_flow"] / max_bike_flow
        else:
            df_bike_flows_all["bike_flow_norm"] = 0.0
    else:
        df_bike_flows_all["bike_flow_norm"] = []

    if not df_ped_flows_all.empty:
        max_ped_flow = df_ped_flows_all["ped_flow"].max()
        if max_ped_flow > 0:
            df_ped_flows_all["ped_flow_norm"] = df_ped_flows_all["ped_flow"] / max_ped_flow
        else:
            df_ped_flows_all["ped_flow_norm"] = 0.0
    else:
        df_ped_flows_all["ped_flow_norm"] = []

    # --------------------------------------------------------------
    # 9. Build normalized edges (unique edges, no time)
    # --------------------------------------------------------------
    # Keep original CRS (likely EPSG:4326), then reproject to EPSG:2056 at the end
    edges_bike_with_index = edges_bike_gdf.reset_index()
    edges_ped_with_index = edges_ped_gdf.reset_index()

    gdf_bike_edges = gpd.GeoDataFrame(
        edges_bike_with_index.copy(),
        geometry="geometry",
        crs=edges_bike_gdf.crs,
    ).to_crs(epsg=2056)

    gdf_ped_edges = gpd.GeoDataFrame(
        edges_ped_with_index.copy(),
        geometry="geometry",
        crs=edges_ped_gdf.crs,
    ).to_crs(epsg=2056)

    logger.info(
        "Normalized edges — bike: %d, ped: %d",
        len(gdf_bike_edges),
        len(gdf_ped_edges),
    )

    # --------------------------------------------------------------
    # 10. Save outputs – 3 files per type (edges GPKG, edges GeoParquet, flows Parquet)
    # --------------------------------------------------------------
    def make_name(suffix: str) -> str:
        # For S3, base_name is prefix; for local, it's dir name.
        return f"{base_name.rstrip('/')}{suffix}"

    if use_s3:
        # Bike
        save_gdf_s3(
            gdf_bike_edges,
            bucket=output_bucket,
            key=make_name("_bike_edges.gpkg"),
            as_gpkg=True,
        )
        save_gdf_s3(
            gdf_bike_edges,
            bucket=output_bucket,
            key=make_name("_bike_edges.parquet"),
            as_gpkg=False,
        )
        save_df_parquet_s3(
            df_bike_flows_all,
            bucket=output_bucket,
            key=make_name("_bike_flows.parquet"),
        )

        # Ped
        save_gdf_s3(
            gdf_ped_edges,
            bucket=output_bucket,
            key=make_name("_ped_edges.gpkg"),
            as_gpkg=True,
        )
        save_gdf_s3(
            gdf_ped_edges,
            bucket=output_bucket,
            key=make_name("_ped_edges.parquet"),
            as_gpkg=False,
        )
        save_df_parquet_s3(
            df_ped_flows_all,
            bucket=output_bucket,
            key=make_name("_ped_flows.parquet"),
        )
    else:
        output_dir = base_name
        base_path = os.path.join(local_root, output_dir)
        os.makedirs(base_path, exist_ok=True)

        def local_path(filename: str) -> str:
            return os.path.join(base_path, filename)

        # Bike
        save_gdf_local(
            gdf_bike_edges,
            path=local_path("bike_edges.gpkg"),
            as_gpkg=True,
        )
        save_gdf_local(
            gdf_bike_edges,
            path=local_path("bike_edges.parquet"),
            as_gpkg=False,
        )
        save_df_parquet_local(
            df_bike_flows_all,
            path=local_path("bike_flows.parquet"),
        )

        # Ped
        save_gdf_local(
            gdf_ped_edges,
            path=local_path("ped_edges.gpkg"),
            as_gpkg=True,
        )
        save_gdf_local(
            gdf_ped_edges,
            path=local_path("ped_edges.parquet"),
            as_gpkg=False,
        )
        save_df_parquet_local(
            df_ped_flows_all,
            path=local_path("ped_flows.parquet"),
        )

    logger.info(
        "Done. Bike edges: %d, bike flow records: %d | Ped edges: %d, ped flow records: %d",
        len(gdf_bike_edges),
        len(df_bike_flows_all),
        len(gdf_ped_edges),
        len(df_ped_flows_all),
    )


if __name__ == "__main__":
    main()
