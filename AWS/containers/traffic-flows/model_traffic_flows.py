#!/usr/bin/env python
"""
Traffic flow simulation for Zürich – from partitioned Parquet counting stations to edge×hour flows.

Key behaviors
-------------
- Loads many Parquet files from a folder/prefix, filtered by INPUT_FILTER substring in filename.
- Aggregates observations into a *typical day profile*:
    (msid, richtung, hour) with hour ∈ {0..23}
  using mean flow per hour over all days.

- Propagates flows along an OSM graph (drive network) with distance decay + bearing weighting.

- Writes 4 outputs:
  1) Full edge×hour flows with geometry (GeoPackage)
  2) Full edge×hour flows with geometry (GeoParquet)
  3) Normalized edges-only geometry (GeoParquet)
  4) Normalized flows table (no geometry, Parquet)

Critical fix vs previous version
--------------------------------
- Avoids OOM by *stream-aggregating* flows during propagation (no giant flows_dir_records list).

Normalized ID
-------------
- Adds a single integer `edge_id` that matches:
    - edges normalized dataset (edges-only geometry)
    - flows normalized table (edge×hour)
  and also appears in the full edge×hour geodata outputs.

Environment variables
---------------------
S3 mode (if all set):
- INPUT_BUCKET
- INPUT_PREFIX
- OUTPUT_BUCKET
- OUTPUT_KEY
Optional:
- INPUT_FILTER

Local mode (otherwise):
- LOCAL_BUCKET_ROOT (default "/data")
- INPUT_PREFIX
- INPUT_FILTER
- OUTPUT_KEY

Other:
- VERSION_TAG
- LOG_LEVEL

OSM / propagation:
- OSM_PLACE (default "Zürich, Switzerland")
- MAX_DISTANCE_M (default 5000)
- DECAY_FACTOR (default 0.985)  # per-100m decay
- MIN_FLOW (default 5)
- MAX_BEARING_DIFF (default 135)
- MIN_EDGE_SHARE (default 0.01)
"""

import os
import math
import logging
import tempfile
from collections import defaultdict, deque

import boto3
import numpy as np
import pandas as pd
import geopandas as gpd
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
# Helpers: parsing envs
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
# Direction / bearing helpers
# -------------------------------------------------------------------
def classify_direction(x: str) -> str:
    """Classify richtung into 'in', 'out', or 'label'."""
    if pd.isna(x):
        return "label"
    x = str(x).strip().lower()
    if "einwärts" in x or "einwaerts" in x:
        return "in"
    elif "auswärts" in x or "auswaerts" in x:
        return "out"
    else:
        return "label"


label_bearings = {
    # Norden / Nordosten
    "oerlikon": 45, "affoltern": 45, "schwamendingen": 45,
    "bucheggplatz": 0, "heimplatz": 15, "klusplatz": 20,
    "zoo": 30, "hoengg": 330, "hoenggerberg": 330, "zollikon": 80,
    "wipkingerplatz": 350, "limmatplatz": 0, "schaffhauserplatz": 10,
    "milchbuck": 20, "milchbucktunnel": 20, "universitaet": 30,
    "hegibachplatz": 40, "kreuzplatz": 40, "winterthur": 45,
    "zehntenhausplatz": 350,

    # Osten
    "vorderberg": 90,

    # Süden / Südosten (See / City)
    "see": 135, "bellevue": 135, "bellvue": 135,
    "sihlporte": 160, "central": 150, "stauffacher": 160,
    "hauptbahnhof": 140, "bahnhof": 140,
    "talstrasse": 150, "manessestrasse": 170,
    "birmensdorferstrasse": 190, "soodstrasse": 190,
    "sihlhoelzli": 180, "forchstrasse": 110, "hubertus": 180,
    "buerkliplatz": 150, "loewe": 150, "loewenplatz": 150,
    "rudolf": 150, "brun": 150, "rudolf-brun-bruecke": 150,
    "brudolf-brun-bruecke": 150, "enge": 170,
    "suedstrasse": 180, "selnau": 170,
    "nord": 0, "muehlegasse": 160,
    "chur": 180,

    # Westen / Südwesten
    "albisriederplatz": 270, "altstetten": 260, "altstetter": 260,
    "badenerstrasse": 260, "hardplatz": 260, "hardbruecke": 260,
    "hohlstrasse": 260, "pfingstweidstrasse": 280,
    "letzigrund": 260, "letzigraben": 260,
    "autobahn": 250, "bernerstrasse": 260,
    "helvetiaplatz": 250, "uetlibergtunnel": 210,
    "allmend": 200, "luggweg": 260,
    "altstetter-/luggwegstrasse": 260, "a3": 250,
    "schimmelstrasse": 260,

    # Südwest / West-Südwest
    "triemli": 210, "utogrund": 230, "albis": 220,

    # Sonderfälle / zusammengesetzte
    "höngg": 330, "höngg/europabrücke": 330,
    "escher wyss": 350, "escher wyss platz": 350,
    "walcheplatz": 10, "mühlegasse": 160,
    "bleicherweg": 160,
}


def get_label_bearing(label: str):
    """Get bearing from heuristic label mapping."""
    if pd.isna(label):
        return None
    label = str(label).lower().strip()
    label = label.replace("ö", "oe").replace("ü", "ue").replace("ä", "ae")
    for key, bearing in label_bearings.items():
        if key in label:
            return float(bearing)
    return None


# Approximate city center (Paradeplatz-ish)
CENTER_LON, CENTER_LAT = 8.5390, 47.3724


def bearing_to_center(lon: float, lat: float) -> float:
    dx = CENTER_LON - lon
    dy = CENTER_LAT - lat
    angle = math.degrees(math.atan2(dx, dy))
    if angle < 0:
        angle += 360.0
    return angle


def bearing_from_type(row: pd.Series):
    """Heuristic bearing depending on richtung_typ."""
    lon = row["lon"]
    lat = row["lat"]
    rtyp = row["richtung_typ"]

    if pd.isna(lon) or pd.isna(lat):
        return None

    if rtyp == "in":
        return bearing_to_center(lon, lat)
    elif rtyp == "out":
        b = bearing_to_center(lon, lat)
        return (b + 180.0) % 360.0
    else:
        return get_label_bearing(row["richtung"])


def bearing_diff(b1: float, b2: float) -> float:
    if b1 is None or b2 is None or pd.isna(b1) or pd.isna(b2):
        return 180.0
    diff = abs(b1 - b2) % 360.0
    return min(diff, 360.0 - diff)


def direction_weight(seed_bearing: float, edge_bearing: float, max_bearing_diff: float) -> float:
    diff = bearing_diff(seed_bearing, edge_bearing)
    if diff > max_bearing_diff:
        return 0.0
    return max(0.0, 1.0 - diff / max_bearing_diff)


def damp_flow(flow_value: float, distance_m: float, decay_factor: float) -> float:
    """Exponential decay per 100 m."""
    decay_steps = distance_m / 100.0
    return float(flow_value) * (decay_factor ** decay_steps)


# -------------------------------------------------------------------
# I/O helpers for Parquet
# -------------------------------------------------------------------
def load_parquet_folder_local(root: str, rel_folder: str, name_filter: str) -> pd.DataFrame:
    folder = os.path.join(root, rel_folder)
    logger.info("Reading local Parquet folder: %s (filter=%r)", folder, name_filter)
    if not os.path.isdir(folder):
        raise FileNotFoundError(f"Input folder not found: {folder}")

    files = [
        f for f in os.listdir(folder)
        if f.endswith(".parquet") and (name_filter in f if name_filter else True)
    ]
    files.sort()

    if not files:
        raise FileNotFoundError(f"No Parquet files matching filter={name_filter!r} in {folder}")

    dfs = []
    for f in files:
        path = os.path.join(folder, f)
        logger.info("  -> reading %s", path)
        dfs.append(pd.read_parquet(path))

    df = pd.concat(dfs, ignore_index=True)
    logger.info("Concatenated %d files into %d rows", len(files), len(df))
    return df


def load_parquet_folder_s3(bucket: str, prefix: str, name_filter: str) -> pd.DataFrame:
    logger.info("Reading Parquet from s3://%s/%s* (filter=%r)", bucket, prefix, name_filter)
    s3 = boto3.client("s3")

    if prefix and not prefix.endswith("/"):
        prefix = prefix + "/"

    paginator = s3.get_paginator("list_objects_v2")
    page_iter = paginator.paginate(Bucket=bucket, Prefix=prefix)

    keys = []
    for page in page_iter:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".parquet"):
                continue
            fname = os.path.basename(key)
            if name_filter and name_filter not in fname:
                continue
            keys.append(key)

    keys.sort()

    if not keys:
        raise FileNotFoundError(
            f"No Parquet objects matching filter={name_filter!r} under s3://{bucket}/{prefix}"
        )

    dfs = []
    with tempfile.TemporaryDirectory() as tmpdir:
        for key in keys:
            tmp_path = os.path.join(tmpdir, os.path.basename(key))
            logger.info("  -> downloading s3://%s/%s to %s", bucket, key, tmp_path)
            with open(tmp_path, "wb") as f:
                s3.download_fileobj(bucket, key, f)
            dfs.append(pd.read_parquet(tmp_path))

    df = pd.concat(dfs, ignore_index=True)
    logger.info("Concatenated %d objects into %d rows", len(keys), len(df))
    return df


def cast_object_columns_to_str(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert all object dtype columns to pandas StringDtype, to make Parquet writing safer.
    Geometry columns (GeoSeries) are not affected.
    """
    obj_cols = df.select_dtypes(include=["object"]).columns
    for col in obj_cols:
        df[col] = df[col].astype("string")
    return df


# -------------------------------------------------------------------
# MAIN
# -------------------------------------------------------------------
def main():
    # --------------------------------------------------------------
    # I/O configuration
    # --------------------------------------------------------------
    input_bucket = os.getenv("INPUT_BUCKET")
    input_prefix = os.getenv("INPUT_PREFIX", "").strip()
    input_filter = os.getenv("INPUT_FILTER", "").strip()
    output_bucket = os.getenv("OUTPUT_BUCKET")
    output_key = os.getenv("OUTPUT_KEY", "").strip()
    version_tag = os.getenv("VERSION_TAG", "").strip()

    local_root = os.getenv("LOCAL_BUCKET_ROOT", "/data")
    use_s3 = all([input_bucket, input_prefix, output_bucket, output_key])

    if not output_key:
        output_key = "traffic_edge_flows"

    if use_s3:
        logger.info("Running in S3 mode.")
        logger.info("Input:  s3://%s/%s (filter=%r)", input_bucket, input_prefix, input_filter)
        logger.info("Output prefix: s3://%s/%s", output_bucket, output_key)
        out_dir_root = None
    else:
        logger.info("Running in local mode.")
        logger.info("Local root: %s", local_root)
        logger.info("Input folder: %s", os.path.join(local_root, input_prefix))
        logger.info("Input filter: %r", input_filter)
        out_dir_root = os.path.join(local_root, output_key)
        logger.info("Output folder: %s", out_dir_root)

    # --------------------------------------------------------------
    # Propagation parameters
    # --------------------------------------------------------------
    osm_place = os.getenv("OSM_PLACE", "Zürich, Switzerland")
    max_distance_m = get_env_int("MAX_DISTANCE_M", 5000)
    decay_factor = get_env_float("DECAY_FACTOR", 0.985)
    min_flow = get_env_float("MIN_FLOW", 5.0)
    max_bearing_diff = get_env_float("MAX_BEARING_DIFF", 135.0)
    min_edge_share = get_env_float("MIN_EDGE_SHARE", 0.01)

    logger.info("OSM place: %s", osm_place)
    logger.info(
        "MAX_DISTANCE_M=%d, DECAY_FACTOR=%.4f, MIN_FLOW=%.1f, MAX_BEARING_DIFF=%.1f, MIN_EDGE_SHARE=%.3f",
        max_distance_m, decay_factor, min_flow, max_bearing_diff, min_edge_share
    )

    # --------------------------------------------------------------
    # 1. Load traffic Parquet (partitioned folder)
    # --------------------------------------------------------------
    wanted_cols = [
        "msid", "msname", "zsid", "zsname",
        "achse", "hnr", "hoehe",
        "ekoord", "nkoord", "richtung",
        "messung_dat_zeit", "anz_fahrzeuge", "anz_fahrzeuge_status",
    ]

    if use_s3:
        df = load_parquet_folder_s3(input_bucket, input_prefix, input_filter)
    else:
        df = load_parquet_folder_local(local_root, input_prefix, input_filter)

    # Restrict to relevant columns if present (preserve desired order)
    keep_cols = [c for c in wanted_cols if c in df.columns]
    df = df[keep_cols].copy()
    logger.info("Loaded Parquet with %d rows and %d columns", len(df), len(df.columns))

    # Types
    if "anz_fahrzeuge" in df.columns:
        df["anz_fahrzeuge"] = pd.to_numeric(df["anz_fahrzeuge"], errors="coerce")
    df["messung_dat_zeit"] = pd.to_datetime(df["messung_dat_zeit"], errors="coerce")
    df["ekoord"] = pd.to_numeric(df["ekoord"], errors="coerce")
    df["nkoord"] = pd.to_numeric(df["nkoord"], errors="coerce")

    logger.info("After type conversion:\n%s", df.dtypes)

    # Drop rows without valid timestamp or coordinates
    df = df.dropna(subset=["messung_dat_zeit", "ekoord", "nkoord"]).copy()
    logger.info("After dropping invalid rows: %d", len(df))

    # Hour-of-day (0–23)
    df["hour"] = df["messung_dat_zeit"].dt.hour.astype("Int64")

    # --------------------------------------------------------------
    # 2. LV95 -> WGS84 & build stations GeoDataFrame
    # --------------------------------------------------------------
    transformer = Transformer.from_crs("EPSG:2056", "EPSG:4326", always_xy=True)
    lon, lat = transformer.transform(df["ekoord"].values, df["nkoord"].values)
    df["lon"] = lon
    df["lat"] = lat

    logger.info(
        "Lon bounds: [%.4f, %.4f], Lat bounds: [%.4f, %.4f]",
        df["lon"].min(), df["lon"].max(),
        df["lat"].min(), df["lat"].max(),
    )

    gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df["lon"], df["lat"]),
        crs="EPSG:4326",
    )

    # --------------------------------------------------------------
    # 3. Aggregate flow per station + direction + HOUR of day
    # --------------------------------------------------------------
    agg = (
        gdf
        .groupby(["msid", "richtung", "hour"], as_index=False)
        .agg(
            anz_fahrzeuge=("anz_fahrzeuge", "mean"),
            lon=("lon", "first"),
            lat=("lat", "first"),
            msname=("msname", "first"),
            zsname=("zsname", "first"),
            achse=("achse", "first"),
        )
        .rename(columns={"anz_fahrzeuge": "flow_value"})
    )


    gdf_stations = gpd.GeoDataFrame(
        agg,
        geometry=gpd.points_from_xy(agg["lon"], agg["lat"]),
        crs="EPSG:4326",
    )

    logger.info("Number of station×direction×hour seeds: %d", len(gdf_stations))

    # richtung_typ classification
    gdf_stations["richtung_typ"] = gdf_stations["richtung"].apply(classify_direction)
    logger.info("richtung_typ counts:\n%s", gdf_stations["richtung_typ"].value_counts())

    # --------------------------------------------------------------
    # 4. Seed bearings
    # --------------------------------------------------------------
    gdf_stations["bearing_seed"] = gdf_stations.apply(bearing_from_type, axis=1)

    # For seeds still missing, fallback to label-based
    mask_missing = gdf_stations["bearing_seed"].isna()
    gdf_stations.loc[mask_missing, "bearing_seed"] = (
        gdf_stations.loc[mask_missing, "richtung"].apply(get_label_bearing)
    )

    logger.info(
        "Seeds with bearing_seed defined: %d / %d",
        gdf_stations["bearing_seed"].notna().sum(),
        len(gdf_stations),
    )

    # Drop seeds without bearing at all (can't propagate directionally)
    gdf_stations = gdf_stations.dropna(subset=["bearing_seed"]).copy()
    logger.info("Remaining seeds after dropping missing bearings: %d", len(gdf_stations))

    # --------------------------------------------------------------
    # 5. OSM street network for Zurich
    # --------------------------------------------------------------
    logger.info("Downloading OSM graph for %s ...", osm_place)
    G = ox.graph_from_place(osm_place, network_type="drive")
    logger.info("Graph: %d nodes, %d edges", len(G.nodes), len(G.edges))

    # Add edge bearings
    G = ox.bearing.add_edge_bearings(G)

    # --------------------------------------------------------------
    # 6. Snap stations to nearest edges
    # --------------------------------------------------------------
    assert gdf_stations.crs.to_string() == "EPSG:4326", "gdf_stations must be in WGS84"

    xs = gdf_stations["lon"].to_numpy()
    ys = gdf_stations["lat"].to_numpy()

    logger.info("Computing nearest edges for %d seeds ...", len(gdf_stations))
    edges_arr, dists_arr = ox.distance.nearest_edges(G, X=xs, Y=ys, return_dist=True)

    gdf_stations["edge_u"] = [t[0] for t in edges_arr]
    gdf_stations["edge_v"] = [t[1] for t in edges_arr]
    gdf_stations["edge_k"] = [t[2] for t in edges_arr]
    gdf_stations["snap_dist_m"] = dists_arr

    logger.info("Snap distance stats (m):\n%s", gdf_stations["snap_dist_m"].describe())

    # Determine propagation start node (aligned with bearing)
    start_nodes = []
    for _, row in gdf_stations.iterrows():
        u = row["edge_u"]
        v = row["edge_v"]
        k = row["edge_k"]
        seed_bearing = float(row["bearing_seed"])
        edge_data = G.get_edge_data(u, v, k) or {}
        edge_bearing = float(edge_data.get("bearing", 180.0))
        if bearing_diff(seed_bearing, edge_bearing) > 90.0:
            start_nodes.append(v)
        else:
            start_nodes.append(u)

    gdf_stations["start_node"] = start_nodes

    # Seeds GeoDataFrame for propagation (only what we need)
    gdf_seeds = gpd.GeoDataFrame(
        gdf_stations[
            [
                "msid",
                "richtung",
                "richtung_typ",
                "flow_value",
                "bearing_seed",
                "start_node",
                "hour",
                "zsname",
                "achse",
            ]
        ].copy(),
        geometry=gdf_stations.geometry,
        crs="EPSG:4326",
    )


    logger.info("Final seeds to propagate: %d", len(gdf_seeds))

    # --------------------------------------------------------------
    # 7+8. MASS-BALANCED FLOW PROPAGATION (REPLACEMENT)
    # --------------------------------------------------------------
    logger.info("Starting MASS-BALANCED flow propagation for %d seeds ...", len(gdf_seeds))

    sum_flow = defaultdict(float)        # (u, v, k, hour) -> flow
    meta_zs = defaultdict(lambda: defaultdict(int))
    meta_achse = defaultdict(lambda: defaultdict(int))

    for idx, seed in gdf_seeds.iterrows():

        if idx > 0 and idx % 50 == 0:
            logger.info("  Processed %d seeds ...", idx)

        seed_node = seed["start_node"]
        seed_flow = float(seed["flow_value"])
        seed_bearing = float(seed["bearing_seed"])
        hour = int(seed["hour"])

        queue = deque()
        queue.append((seed_node, 0.0, seed_flow))
        visited = {}

        while queue:
            node, dist, flow = queue.popleft()

            if dist > max_distance_m or flow < min_flow:
                continue

            if node in visited and visited[node] <= dist:
                continue
            visited[node] = dist

            edges = list(G.out_edges(node, keys=True, data=True))
            if not edges:
                continue

            candidates = []
            for _, v, k, data in edges:
                w = direction_weight(
                    seed_bearing,
                    float(data.get("bearing", np.nan)),
                    max_bearing_diff
                )
                if w >= min_edge_share:
                    candidates.append((v, k, data, w))

            if not candidates:
                continue

            w_sum = sum(w for *_, w in candidates)
            if w_sum <= 0:
                continue

            for v, k, data, w in candidates:
                share = w / w_sum
                edge_flow = flow * share

                key4 = (node, v, k, hour)
                sum_flow[key4] += edge_flow

                meta_zs[key4][seed["zsname"]] += 1
                meta_achse[key4][seed["achse"]] += 1

                edge_len = float(data.get("length", 0.0))
                new_dist = dist + edge_len
                new_flow = damp_flow(edge_flow, edge_len, decay_factor)

                queue.append((v, new_dist, new_flow))

    logger.info("Propagation finished. Aggregated %d edge×hour keys.", len(sum_flow))

    # --------------------------------------------------------------
    # GLOBAL MASS BALANCE PER HOUR (HARD BRAKE)
    # --------------------------------------------------------------
    df_edge_flow = pd.DataFrame(
        [(u, v, k, h, f) for (u, v, k, h), f in sum_flow.items()],
        columns=["u", "v", "key", "hour", "total_flow"]
    )

    seed_totals = gdf_seeds.groupby("hour")["flow_value"].sum()

    for h, total_seed_flow in seed_totals.items():
        mask = df_edge_flow["hour"] == h
        total_edge_flow = df_edge_flow.loc[mask, "total_flow"].sum()
        if total_edge_flow > 0:
            scale = total_seed_flow / total_edge_flow
            df_edge_flow.loc[mask, "total_flow"] *= scale


    logger.info("Done. Flow model stabilized (local + global mass balance).")

    # --------------------------------------------------------------
    # 9. Build edge GeoDataFrame and join flows
    # --------------------------------------------------------------
    edges_gdf = ox.graph_to_gdfs(G, nodes=False, edges=True)
    edges_gdf = edges_gdf.reset_index()[["u", "v", "key", "geometry", "name", "length", "bearing"]]

    # Normalized edges-only geometry (unique by u,v,key)
    edges_geom = edges_gdf.drop_duplicates(subset=["u", "v", "key"]).copy()

    # Create stable edge_id (single int key) based on sorted (u,v,key)
    edges_geom = edges_geom.sort_values(["u", "v", "key"]).reset_index(drop=True)
    edges_geom["edge_id"] = np.arange(1, len(edges_geom) + 1, dtype=np.int64)

    # Prepare a mapping table for joining edge_id into flows
    edge_id_map = edges_geom[["u", "v", "key", "edge_id"]].copy()

    # Attach edge_id to flows
    if not df_edge_flow.empty:
        df_edge_flow = df_edge_flow.merge(edge_id_map, how="left", on=["u", "v", "key"])
        missing_ids = df_edge_flow["edge_id"].isna().sum()
        if missing_ids:
            logger.warning("edge_id missing for %d flow rows (unexpected).", missing_ids)
        df_edge_flow["edge_id"] = df_edge_flow["edge_id"].astype("Int64")

    # Full edge×hour geodata
    if not df_edge_flow.empty:
        gdf_full = df_edge_flow.merge(edges_gdf, how="left", on=["u", "v", "key"])
    else:
        gdf_full = pd.DataFrame(columns=["u", "v", "key", "hour", "total_flow", "n_sources", "edge_id"])

    # Ensure GeoDataFrame with correct CRS if geometry exists
    if "geometry" in gdf_full.columns:
        gdf_full = gpd.GeoDataFrame(gdf_full, geometry="geometry", crs="EPSG:4326")
    else:
        gdf_full = gpd.GeoDataFrame(gdf_full, geometry=None, crs="EPSG:4326")

    # Reorder columns a bit
    if not gdf_full.empty:
        cols_front = [
            "edge_id",
            "u", "v", "key", "hour",
            "total_flow", "n_sources",
            "name", "length", "bearing",
            "geometry",
        ]
        gdf_full = gdf_full[[c for c in cols_front if c in gdf_full.columns] +
                            [c for c in gdf_full.columns if c not in cols_front]]

    # Fill NaNs
    if "total_flow" in gdf_full.columns:
        gdf_full["total_flow"] = gdf_full["total_flow"].fillna(0.0)
    if "n_sources" in gdf_full.columns:
        gdf_full["n_sources"] = gdf_full["n_sources"].fillna(0).astype(int)

    # Normalized flow per hour (max-normalize within each hour)
    def _norm_group(x: pd.Series) -> pd.Series:
        m = x.max()
        if m > 0:
            return x / m
        return 0.0

    if not gdf_full.empty and "total_flow" in gdf_full.columns:
        gdf_full["flow_norm"] = gdf_full.groupby("hour")["total_flow"].transform(_norm_group)
    else:
        gdf_full["flow_norm"] = 0.0

    # --------------------------------------------------------------
    # 9b. Reproject to LV95 (EPSG:2056) for geodata outputs
    # --------------------------------------------------------------
    target_crs = "EPSG:2056"

    if not gdf_full.empty and "geometry" in gdf_full.columns and gdf_full.geometry is not None:
        gdf_full = gdf_full.to_crs(target_crs)

    edges_geom = gpd.GeoDataFrame(edges_geom, geometry="geometry", crs="EPSG:4326").to_crs(target_crs)

    logger.info("Full edge×hour rows: %d, edges geometry rows: %d", len(gdf_full), len(edges_geom))

    # Flows table (no geometry) - keep only what you need, but include edge_id as the join key
    if not gdf_full.empty:
        flows_table = gdf_full.drop(columns=["geometry"]).copy()
    else:
        flows_table = pd.DataFrame(columns=["edge_id", "u", "v", "key", "hour", "total_flow", "n_sources", "flow_norm"])

    # --------------------------------------------------------------
    # 10. Save 4 outputs
    # --------------------------------------------------------------
    # (kept your naming convention; adjust if you prefer)
    full_gpkg_name = f"{version_tag}_motorized_traffic_complete.gpkg"
    full_geopq_name = f"{version_tag}_motorized_traffic_complete_full.parquet"
    edges_name = f"{version_tag}_motorized_traffic_edges.parquet"
    flows_name = f"{version_tag}_motorized_traffic_flows.parquet"

    logger.info("=== REACHED OUTPUT WRITING SECTION === use_s3=%s", use_s3)

    if use_s3:
        s3 = boto3.client("s3")
        prefix = output_key.rstrip("/")

        logger.info("Writing 4 outputs to s3://%s/%s/ ...", output_bucket, prefix)

        with tempfile.TemporaryDirectory() as tmpdir:
            # Full GPKG
            full_gpkg_path = os.path.join(tmpdir, full_gpkg_name)
            if len(gdf_full) > 0:
                gdf_full.to_file(full_gpkg_path, driver="GPKG")
            else:
                # write an empty layer (still creates a file) -> easiest: create empty GeoDataFrame with geometry col
                gpd.GeoDataFrame({"geometry": []}, geometry="geometry", crs=target_crs).to_file(full_gpkg_path, driver="GPKG")

            # Prepare safe copies for Parquet (cast object -> string)
            gdf_full_pq = cast_object_columns_to_str(gdf_full.copy())
            edges_geom_pq = cast_object_columns_to_str(edges_geom.copy())
            flows_table_pq = cast_object_columns_to_str(flows_table.copy())

            # Full GeoParquet
            full_geopq_path = os.path.join(tmpdir, full_geopq_name)
            gdf_full_pq.to_parquet(full_geopq_path, index=False)

            # Edges GeoParquet (normalized, includes edge_id)
            edges_path = os.path.join(tmpdir, edges_name)
            edges_geom_pq.to_parquet(edges_path, index=False)

            # Flows table Parquet (normalized join via edge_id)
            flows_path = os.path.join(tmpdir, flows_name)
            flows_table_pq.to_parquet(flows_path, index=False)

            def _upload(local_path: str, name: str):
                key = f"{prefix}/{name}"
                size_bytes = os.path.getsize(local_path)
                logger.info("Uploading %s (%d bytes) to s3://%s/%s", name, size_bytes, output_bucket, key)
                s3.upload_file(local_path, output_bucket, key)

            _upload(full_gpkg_path, full_gpkg_name)
            _upload(full_geopq_path, full_geopq_name)
            _upload(edges_path, edges_name)
            _upload(flows_path, flows_name)

        logger.info(
            "Uploaded 4 outputs to s3://%s/%s/{%s, %s, %s, %s}",
            output_bucket, prefix, full_gpkg_name, full_geopq_name, edges_name, flows_name
        )

    else:
        os.makedirs(out_dir_root, exist_ok=True)

        full_gpkg_path = os.path.join(out_dir_root, full_gpkg_name)
        full_geopq_path = os.path.join(out_dir_root, full_geopq_name)
        edges_path = os.path.join(out_dir_root, edges_name)
        flows_path = os.path.join(out_dir_root, flows_name)

        logger.info("Writing full GPKG to %s", full_gpkg_path)
        if len(gdf_full) > 0:
            gdf_full.to_file(full_gpkg_path, driver="GPKG")
        else:
            gpd.GeoDataFrame({"geometry": []}, geometry="geometry", crs=target_crs).to_file(full_gpkg_path, driver="GPKG")

        # Prepare safe copies for Parquet
        gdf_full_pq = cast_object_columns_to_str(gdf_full.copy())
        edges_geom_pq = cast_object_columns_to_str(edges_geom.copy())
        flows_table_pq = cast_object_columns_to_str(flows_table.copy())

        logger.info("Writing full GeoParquet to %s", full_geopq_path)
        gdf_full_pq.to_parquet(full_geopq_path, index=False)

        logger.info("Writing edges GeoParquet (normalized) to %s", edges_path)
        edges_geom_pq.to_parquet(edges_path, index=False)

        logger.info("Writing flows table Parquet (normalized) to %s", flows_path)
        flows_table_pq.to_parquet(flows_path, index=False)

        logger.info(
            "Done (local). Outputs written to %s: {%s, %s, %s, %s}",
            out_dir_root, full_gpkg_name, full_geopq_name, edges_name, flows_name
        )

    logger.info("Done. Full rows: %d, edges: %d, flows table rows: %d",
                len(gdf_full), len(edges_geom), len(flows_table))


if __name__ == "__main__":
    main()
