#!/usr/bin/env python
"""
Traffic flow simulation for Zürich – from raw counting stations to edge flows.

This script is a batch version of your notebook:
- Loads SID traffic counts (sid_dav_verkehrszaehlung_miv_od2031_2025.csv)
- Transforms LV95 coordinates to WGS84
- Infers directions ("in", "out", label-based bearings)
- Snaps stations to OSM street network (Zürich, drive)
- Propagates flows along the network with distance decay & direction weights
- Aggregates flows per road segment (OSM edge)
- Writes one output file (GPKG or Parquet)

Environment variables
---------------------

I/O mode (local vs S3)
~~~~~~~~~~~~~~~~~~~~~~
If all of these are set, we run in S3 mode:
- INPUT_BUCKET      : S3 bucket for input CSV
- INPUT_KEY         : S3 key for input CSV
- OUTPUT_BUCKET     : S3 bucket for output file
- OUTPUT_KEY        : S3 key for output file

Otherwise, we run in local mode:
- LOCAL_BUCKET_ROOT : root folder inside container (default: "/data")
- INPUT_KEY         : path to CSV relative to LOCAL_BUCKET_ROOT
                      e.g. "bronze/sid_dav_verkehrszaehlung_miv_od2031_2025.csv"
- OUTPUT_KEY        : path for output relative to LOCAL_BUCKET_ROOT
                      e.g. "silver/traffic_edge_flows.gpkg"

Other config
~~~~~~~~~~~~
- OUTPUT_FORMAT     : "gpkg" (default) or "parquet"
- LOG_LEVEL         : "INFO" (default), "DEBUG", ...

Traffic CSV
~~~~~~~~~~~
- TRAFFIC_CSV_FILE  : optional override for file name if INPUT_KEY is not set.
                      default: "sid_dav_verkehrszaehlung_miv_od2031_2025.csv"

OSM / propagation parameters (optional)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
- OSM_PLACE         : place name for osmnx.graph_from_place
                      default: "Zürich, Switzerland"
- MAX_DISTANCE_M    : max propagation distance (default: 5000)
- DECAY_FACTOR      : per-100m decay factor (default: 0.985)
- MIN_FLOW          : minimum propagated flow (default: 5)
- MAX_BEARING_DIFF  : max allowed bearing difference in degrees (default: 135)
- MIN_EDGE_SHARE    : minimum direction weight to keep edge (default: 0.01)

Typical local run
-----------------
docker run --rm \
  -v "$(pwd)/local_bucket:/data" \
  -e LOCAL_BUCKET_ROOT=/data \
  -e INPUT_KEY="bronze/sid_dav_verkehrszaehlung_miv_od2031_2025.csv" \
  -e OUTPUT_KEY="silver/traffic_edge_flows.gpkg" \
  -e OUTPUT_FORMAT="gpkg" \
  traffic-flow:latest
"""

import os
import math
import logging
import tempfile

import boto3
import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, LineString
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
    """Classify Richtung into 'in', 'out', or 'label'."""
    if pd.isna(x):
        return "label"
    x = x.strip().lower()
    if "einwärts" in x or "einwaerts" in x:
        return "in"
    elif "auswärts" in x or "auswaerts" in x:
        return "out"
    else:
        return "label"


# Full Zurich heuristic mapping as in notebook
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
    # Umlaut handling
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


def bearing_from_type(row: pd.Series) -> float | None:
    """Heuristic bearing depending on Richtung_typ."""
    lon = row["lon"]
    lat = row["lat"]
    rtyp = row["Richtung_typ"]

    if pd.isna(lon) or pd.isna(lat):
        return None

    if rtyp == "in":
        return bearing_to_center(lon, lat)
    elif rtyp == "out":
        b = bearing_to_center(lon, lat)
        return (b + 180.0) % 360.0
    else:
        # Label-based bearing
        return get_label_bearing(row["Richtung"])


def bearing_diff(b1: float, b2: float) -> float:
    if b1 is None or b2 is None or pd.isna(b1) or pd.isna(b2):
        return 180.0
    diff = abs(b1 - b2) % 360.0
    return min(diff, 360.0 - diff)


def direction_weight(seed_bearing: float, edge_bearing: float,
                     max_bearing_diff: float) -> float:
    diff = bearing_diff(seed_bearing, edge_bearing)
    if diff > max_bearing_diff:
        return 0.0
    # linear weight in [0,1]
    return max(0.0, 1.0 - diff / max_bearing_diff)


def damp_flow(flow_value: float, distance_m: float, decay_factor: float) -> float:
    """Exponential decay per 100 m."""
    decay_steps = distance_m / 100.0
    return float(flow_value) * (decay_factor ** decay_steps)


# -------------------------------------------------------------------
# I/O helpers
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


def save_geodataframe(gdf: gpd.GeoDataFrame,
                      output_format: str,
                      local_root: str | None,
                      output_path_or_key: str,
                      use_s3: bool,
                      output_bucket: str | None):
    """Save GeoDataFrame either locally or to S3 as one file."""
    if use_s3:
        assert output_bucket is not None
        s3 = boto3.client("s3")
        with tempfile.TemporaryDirectory() as tmpdir:
            if output_format == "gpkg":
                tmp_name = "traffic_edge_flows.gpkg"
                tmp_path = os.path.join(tmpdir, tmp_name)
                gdf.to_file(tmp_path, driver="GPKG")
            else:
                tmp_name = "traffic_edge_flows.parquet"
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
# MAIN
# -------------------------------------------------------------------
def main():
    # --------------------------------------------------------------
    # I/O configuration
    # --------------------------------------------------------------
    input_bucket = os.getenv("INPUT_BUCKET")
    input_key = os.getenv("INPUT_KEY")
    output_bucket = os.getenv("OUTPUT_BUCKET")
    output_key = os.getenv("OUTPUT_KEY")

    local_root = os.getenv("LOCAL_BUCKET_ROOT", "/data")
    traffic_default_name = os.getenv(
        "TRAFFIC_CSV_FILE", "sid_dav_verkehrszaehlung_miv_od2031_2025.csv"
    )

    use_s3 = all([input_bucket, input_key, output_bucket, output_key])

    if not output_key:
        output_key = "traffic_edge_flows.gpkg"

    if not input_key:
        input_key = traffic_default_name

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
        logger.info("Output: s3://%s/%s", output_bucket, output_key)
    else:
        logger.info("Running in local mode.")
        logger.info("Local root: %s", local_root)
        logger.info("Input:  %s", os.path.join(local_root, input_key))
        logger.info("Output: %s", os.path.join(local_root, output_key))

    logger.info("Output format: %s", output_format)

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
    logger.info("MAX_DISTANCE_M=%d, DECAY_FACTOR=%.4f, MIN_FLOW=%.1f, "
                "MAX_BEARING_DIFF=%.1f, MIN_EDGE_SHARE=%.3f",
                max_distance_m, decay_factor, min_flow,
                max_bearing_diff, min_edge_share)

    # --------------------------------------------------------------
    # 1. Load traffic CSV
    # --------------------------------------------------------------
    cols = [
        "MSID", "MSName", "ZSID", "ZSName",
        "Achse", "HNr", "Hoehe",
        "EKoord", "NKoord", "Richtung",
        "MessungDatZeit", "AnzFahrzeuge", "AnzFahrzeugeStatus",
    ]

    if use_s3:
        df = load_csv_s3(input_bucket, input_key)
    else:
        df = load_csv_local(local_root, input_key)

    # Restrict to relevant columns (if loaded extra)
    df = df[[c for c in df.columns if c in cols]].copy()
    logger.info("Loaded CSV with %d rows and %d columns", len(df), len(df.columns))

    # Types
    df["AnzFahrzeuge"] = pd.to_numeric(df["AnzFahrzeuge"], errors="coerce")
    df["MessungDatZeit"] = pd.to_datetime(df["MessungDatZeit"], errors="coerce")
    df["EKoord"] = pd.to_numeric(df["EKoord"], errors="coerce")
    df["NKoord"] = pd.to_numeric(df["NKoord"], errors="coerce")

    logger.info("After type conversion:\n%s", df.dtypes)

    # --------------------------------------------------------------
    # 2. LV95 -> WGS84 & build stations GeoDataFrame
    # --------------------------------------------------------------
    transformer = Transformer.from_crs("EPSG:2056", "EPSG:4326", always_xy=True)
    lon, lat = transformer.transform(df["EKoord"].values, df["NKoord"].values)
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
    # 3. Aggregate mean flow per station + direction
    # --------------------------------------------------------------
    agg = (
        gdf
        .groupby(["MSID", "Richtung"], as_index=False)
        .agg({
            "AnzFahrzeuge": "mean",
            "lon": "first",
            "lat": "first",
            "MSName": "first",
            "Achse": "first",
        })
        .rename(columns={"AnzFahrzeuge": "flow_mean"})
    )

    gdf_stations = gpd.GeoDataFrame(
        agg,
        geometry=gpd.points_from_xy(agg["lon"], agg["lat"]),
        crs="EPSG:4326",
    )

    logger.info("Number of aggregated station directions: %d", len(gdf_stations))

    # Richtung_typ classification
    gdf_stations["Richtung_typ"] = gdf_stations["Richtung"].apply(classify_direction)
    logger.info("Richtung_typ counts:\n%s", gdf_stations["Richtung_typ"].value_counts())

    # --------------------------------------------------------------
    # 4. Seed bearings
    # --------------------------------------------------------------
    gdf_stations["bearing_seed"] = gdf_stations.apply(bearing_from_type, axis=1)
    # For seeds still missing, fallback to label-based
    mask_missing = gdf_stations["bearing_seed"].isna()
    gdf_stations.loc[mask_missing, "bearing_seed"] = (
        gdf_stations.loc[mask_missing, "Richtung"].apply(get_label_bearing)
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

    # edges_arr is a list/array of (u, v, key) tuples
    gdf_stations["edge_u"] = [t[0] for t in edges_arr]
    gdf_stations["edge_v"] = [t[1] for t in edges_arr]
    gdf_stations["edge_k"] = [t[2] for t in edges_arr]
    gdf_stations["snap_dist_m"] = dists_arr

    logger.info(
        "Snap distance stats (m):\n%s",
        gdf_stations["snap_dist_m"].describe(),
    )

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
            # flip
            start_nodes.append(v)
        else:
            start_nodes.append(u)

    gdf_stations["start_node"] = start_nodes

    # Seeds GeoDataFrame for propagation (just what we need)
    gdf_seeds = gpd.GeoDataFrame(
        gdf_stations[
            [
                "MSID",
                "Richtung",
                "Richtung_typ",
                "flow_mean",
                "bearing_seed",
                "start_node",
            ]
        ].copy(),
        geometry=gdf_stations.geometry,
        crs="EPSG:4326",
    )

    logger.info("Final seeds to propagate: %d", len(gdf_seeds))

    # --------------------------------------------------------------
    # 7. Propagate flows along OSM graph
    # --------------------------------------------------------------
    flows_dir_records: list[dict] = []

    logger.info("Starting flow propagation for %d seeds ...", len(gdf_seeds))
    for idx, seed in gdf_seeds.iterrows():
        if idx > 0 and idx % 50 == 0:
            logger.info("  Processed %d seeds ...", idx)

        seed_node = seed["start_node"]
        start_flow = float(seed["flow_mean"])
        seed_bearing = float(seed["bearing_seed"])

        # Dijkstra from seed_node up to MAX_DISTANCE_M
        lengths = nx.single_source_dijkstra_path_length(
            G,
            seed_node,
            cutoff=max_distance_m,
            weight="length",
        )

        for node_id, dist_m in lengths.items():
            if dist_m == 0:
                continue

            flow_value = damp_flow(start_flow, dist_m, decay_factor)
            if flow_value < min_flow:
                continue

            # For each outgoing edge at this node, distribute flow
            for _, v2, k2, data in G.out_edges(node_id, keys=True, data=True):
                edge_bearing = float(data.get("bearing", np.nan))
                w = direction_weight(seed_bearing, edge_bearing, max_bearing_diff)
                if w < min_edge_share:
                    continue

                flows_dir_records.append(
                    {
                        "MSID": seed["MSID"],
                        "u": node_id,
                        "v": v2,
                        "key": k2,
                        "distance_m": dist_m,
                        "flow_value": flow_value * w,
                        "bearing_seed": seed_bearing,
                        "bearing_edge": edge_bearing,
                        "weight": w,
                    }
                )

    df_flows_dir = pd.DataFrame(flows_dir_records)
    logger.info(
        "Flow propagation finished. Generated %d edge-flow records.",
        len(df_flows_dir),
    )

    if df_flows_dir.empty:
        logger.warning("No flows generated; output will have zero total_flow.")
        # Still build edges GeoDataFrame with zero flows.

    # --------------------------------------------------------------
    # 8. Aggregate flows per OSM edge
    # --------------------------------------------------------------
    if not df_flows_dir.empty:
        df_edge_flow = (
            df_flows_dir.groupby(["u", "v", "key"], as_index=False)
            .agg(
                total_flow=("flow_value", "sum"),
                n_sources=("MSID", "nunique"),
            )
        )
        logger.info(
            "Aggregated flows for %d unique edges.",
            len(df_edge_flow),
        )
    else:
        df_edge_flow = pd.DataFrame(columns=["u", "v", "key", "total_flow", "n_sources"])

    # --------------------------------------------------------------
    # 9. Build edge GeoDataFrame from graph and join flows
    # --------------------------------------------------------------
    edges_gdf = ox.graph_to_gdfs(G, nodes=False, edges=True)
    edges_gdf = edges_gdf.reset_index()[["u", "v", "key", "geometry", "name", "length", "bearing"]]

    # Ensure types match
    df_edge_flow["u"] = df_edge_flow["u"].astype(edges_gdf["u"].dtype, errors="ignore")
    df_edge_flow["v"] = df_edge_flow["v"].astype(edges_gdf["v"].dtype, errors="ignore")

    edges_gdf = edges_gdf.merge(
        df_edge_flow,
        how="left",
        on=["u", "v", "key"],
    )

    edges_gdf["total_flow"] = edges_gdf["total_flow"].fillna(0.0)
    edges_gdf["n_sources"] = edges_gdf["n_sources"].fillna(0).astype(int)

    max_flow = edges_gdf["total_flow"].max()
    if max_flow > 0:
        edges_gdf["flow_norm"] = edges_gdf["total_flow"] / max_flow
    else:
        edges_gdf["flow_norm"] = 0.0

    # --------------------------------------------------------------
    # 9b. Reproject to LV95 (EPSG:2056) for output
    # --------------------------------------------------------------
    target_crs = "EPSG:2056"
    gdf_edges = gpd.GeoDataFrame(edges_gdf, geometry="geometry", crs="EPSG:4326")
    gdf_edges = gdf_edges.to_crs(target_crs)

    logger.info(
        "Edges with total_flow > 0: %d / %d (CRS=%s)",
        int((gdf_edges["total_flow"] > 0).sum()),
        len(gdf_edges),
        target_crs,
    )

    # --------------------------------------------------------------
    # 10. Save output
    # --------------------------------------------------------------
    save_geodataframe(
        gdf_edges,
        output_format=output_format,
        local_root=None if use_s3 else local_root,
        output_path_or_key=output_key,
        use_s3=use_s3,
        output_bucket=output_bucket,
    )

    logger.info("Done. Output rows: %d", len(gdf_edges))


if __name__ == "__main__":
    main()
