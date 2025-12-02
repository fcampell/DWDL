#!/usr/bin/env python
"""
VBZ passenger flows in Zürich – from raw VBZ CSVs to georeferenced flows.

This script replicates the notebook pipeline and adds:
- Local vs S3 input/output handling via environment variables.
- Single output file, either GPKG or Parquet, controlled by env vars.

Environment variables (pattern taken from run_visibility_model.py):

Core I/O
-------
INPUT_BUCKET       : (optional) S3 bucket for inputs. If set together with
                     INPUT_PREFIX, OUTPUT_BUCKET, OUTPUT_KEY → S3 mode.
INPUT_PREFIX       : (optional) S3 "folder"/prefix where VBZ CSV files reside.
                     e.g. "vbz_raw" (files are expected inside this prefix).

OUTPUT_BUCKET      : (optional) S3 bucket for output (S3 mode only).
OUTPUT_KEY         : (optional) S3 object key for final output.
                     e.g. "silver/vbz_flows_daily.gpkg"

LOCAL_BUCKET_ROOT  : (local mode) root directory of your "bucket".
                     default: "/data"
INPUT_PREFIX       : (also used in local mode) subfolder under LOCAL_BUCKET_ROOT
                     where VBZ files live, e.g. "vbz_raw".

VBZ file names (optional overrides)
-----------------------------------
VBZ_REISENDE_FILE      : default "REISENDE.csv"
VBZ_TAGTYP_FILE        : default "TAGTYP.csv"
VBZ_HALTESTELLEN_FILE  : default "HALTESTELLEN.csv"
VBZ_LINIE_FILE         : default "LINIE.csv"

Processing / output
-------------------
OUTPUT_FORMAT      : "gpkg" (default) or "parquet"/"geoparquet"
OUTPUT_LEVEL       : "daily" (default) or "hourly"
GTFS_URL           : optional override for GTFS ZIP URL.
LOG_LEVEL          : logging level (e.g. "INFO", "DEBUG")

Typical local run:
------------------
LOCAL_BUCKET_ROOT=/data \
INPUT_PREFIX=vbz_raw \
OUTPUT_KEY="silver/vbz_flows_daily.gpkg" \
OUTPUT_FORMAT=gpkg \
OUTPUT_LEVEL=daily \
python run_vbz_flows_model.py

Typical S3/ECS run:
-------------------
INPUT_BUCKET=your-bucket \
INPUT_PREFIX=bronze/vbz \
OUTPUT_BUCKET=your-bucket \
OUTPUT_KEY="silver/vbz_flows_daily.parquet" \
OUTPUT_FORMAT=parquet \
OUTPUT_LEVEL=daily \
python run_vbz_flows_model.py
"""

import os
import io
import zipfile
import tempfile
import logging

import boto3
import requests
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, LineString
import osmnx as ox
import networkx as nx

# -------------------------------------------------------------------
# Logging config (same style as run_visibility_model.py)
# -------------------------------------------------------------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)


# -------------------------------------------------------------------
# Helper functions
# -------------------------------------------------------------------
def normalize_name(s: str) -> str:
    """Normalize stop names to improve matching between VBZ and GTFS."""
    if pd.isna(s):
        return ""
    return (
        str(s)
        .upper()
        .replace(" (STADT ZÜRICH)", "")
        .replace(" (ZÜRICH)", "")
        .strip()
    )


def path_linestring(G_proj, stop_node_lookup, from_id: str, to_id: str):
    """
    Compute a LineString following the street graph from stop `from_id` to `to_id`.

    Parameters
    ----------
    G_proj : networkx.MultiDiGraph
        Graph projected to a metric CRS.
    stop_node_lookup : dict
        Mapping from Haltestellen_Id (as string) to nearest graph node.
    from_id, to_id : str
        Stop IDs as strings.

    Returns
    -------
    shapely.geometry.LineString or None
    """
    if from_id not in stop_node_lookup or to_id not in stop_node_lookup:
        return None

    u = stop_node_lookup[from_id]
    v = stop_node_lookup[to_id]

    try:
        path_nodes = nx.shortest_path(G_proj, u, v, weight="length")
    except (nx.NetworkXNoPath, nx.NodeNotFound):
        return None

    coords = [(G_proj.nodes[n]["x"], G_proj.nodes[n]["y"]) for n in path_nodes]
    if len(coords) < 2:
        return None

    return LineString(coords)


# -------------------------------------------------------------------
# MAIN
# -------------------------------------------------------------------
def main():
    # -------------------------------
    # I/O config (S3 vs local)
    # -------------------------------
    input_bucket = os.getenv("INPUT_BUCKET")
    input_prefix = os.getenv("INPUT_PREFIX", "").strip()
    output_bucket = os.getenv("OUTPUT_BUCKET")
    output_key = os.getenv("OUTPUT_KEY")

    use_s3 = all([input_bucket, input_prefix, output_bucket, output_key])

    # Output format
    output_format_raw = os.getenv("OUTPUT_FORMAT", "gpkg").strip().lower()
    if output_format_raw in ("gpkg", "geopackage"):
        output_format = "gpkg"
    elif output_format_raw in ("parquet", "geoparquet"):
        output_format = "parquet"
    else:
        logger.warning(
            "Unknown OUTPUT_FORMAT=%r, falling back to 'gpkg'.",
            output_format_raw,
        )
        output_format = "gpkg"

    # Output level: daily vs hourly
    output_level_raw = os.getenv("OUTPUT_LEVEL", "daily").strip().lower()
    if output_level_raw not in ("daily", "hourly"):
        logger.warning(
            "Unknown OUTPUT_LEVEL=%r, falling back to 'daily'.",
            output_level_raw,
        )
        output_level_raw = "daily"
    output_level = output_level_raw

    local_bucket_root = os.getenv("LOCAL_BUCKET_ROOT", "/data")

    # VBZ file names (can be overridden via env)
    vbz_reisende_file = os.getenv("VBZ_REISENDE_FILE", "REISENDE.csv")
    vbz_tagtyp_file = os.getenv("VBZ_TAGTYP_FILE", "TAGTYP.csv")
    vbz_haltestellen_file = os.getenv("VBZ_HALTESTELLEN_FILE", "HALTESTELLEN.csv")
    vbz_linie_file = os.getenv("VBZ_LINIE_FILE", "LINIE.csv")

    # Normalize prefix for path/key building
    def build_path_or_key(prefix: str, filename: str) -> str:
        if prefix and not prefix.endswith("/"):
            prefix_local = prefix + os.sep
            prefix_s3 = prefix + "/"
        else:
            prefix_local = prefix
            prefix_s3 = prefix

        # We only use local version for local mode and s3 version for S3 mode.
        return prefix_local, prefix_s3, filename

    if use_s3:
        logger.info("Running in S3 mode.")
    else:
        logger.info("S3 config not fully set; running in local mode.")
        if not output_key:
            # Provide a default relative output path for local mode
            output_key = f"vbz_flows_{output_level}.{output_format}"

    # Build local input/output paths (only used in local mode)
    if not input_prefix:
        # Default: files directly under LOCAL_BUCKET_ROOT
        input_dir = local_bucket_root
    else:
        input_dir = os.path.join(local_bucket_root, input_prefix)

    if not os.path.isdir(input_dir) and not use_s3:
        logger.warning("Local input directory %s does not exist yet.", input_dir)

    if not use_s3:
        output_path = os.path.join(local_bucket_root, output_key)
        logger.info("Output (local): %s", output_path)

    logger.info("Output format: %s", output_format)
    logger.info("Output level: %s", output_level)

    # -------------------------------
    # 1. Load VBZ passenger data
    # -------------------------------
    logger.info("Loading VBZ CSVs (REISENDE, TAGTYP, HALTESTELLEN, LINIE)")

    if use_s3:
        s3 = boto3.client("s3")

        def load_csv_from_s3(bucket: str, prefix: str, filename: str) -> pd.DataFrame:
            key = prefix
            if key and not key.endswith("/"):
                key = key + "/"
            key = key + filename
            logger.info("Reading s3://%s/%s", bucket, key)
            with tempfile.NamedTemporaryFile(suffix=".csv") as tmp:
                s3.download_fileobj(bucket, key, tmp)
                tmp.flush()
                tmp.seek(0)
                return pd.read_csv(tmp.name, sep=";")

        reisende_raw = load_csv_from_s3(input_bucket, input_prefix, vbz_reisende_file)
        tagtyp = load_csv_from_s3(input_bucket, input_prefix, vbz_tagtyp_file)
        haltestellen = load_csv_from_s3(
            input_bucket, input_prefix, vbz_haltestellen_file
        )
        linie = load_csv_from_s3(input_bucket, input_prefix, vbz_linie_file)

    else:
        def load_csv_local(directory: str, filename: str) -> pd.DataFrame:
            path = os.path.join(directory, filename)
            logger.info("Reading %s", path)
            if not os.path.exists(path):
                raise FileNotFoundError(f"Input file not found: {path}")
            return pd.read_csv(path, sep=";")

        reisende_raw = load_csv_local(input_dir, vbz_reisende_file)
        tagtyp = load_csv_local(input_dir, vbz_tagtyp_file)
        haltestellen = load_csv_local(input_dir, vbz_haltestellen_file)
        linie = load_csv_local(input_dir, vbz_linie_file)

    logger.info("Loaded REISENDE rows: %d", len(reisende_raw))
    logger.info("Loaded TAGTYP rows:   %d", len(tagtyp))
    logger.info("Loaded HALTESTELLEN rows: %d", len(haltestellen))
    logger.info("Loaded LINIE rows:   %d", len(linie))

    # -------------------------------
    # 2. Basic cleaning and typing
    # -------------------------------
    reisende = reisende_raw.copy()

    # Numeric columns in REISENDE come as text; convert to numeric
    numeric_cols = [
        "Einsteiger", "Aussteiger", "Besetzung", "Distanz",
        "Tage_DTV", "Tage_DWV", "Tage_SA", "Tage_SO",
        "Tage_SA_N", "Tage_SO_N",
    ]
    for col in numeric_cols:
        if col in reisende.columns:
            reisende[col] = pd.to_numeric(reisende[col], errors="coerce")

    # Convert Nach_Hst_Id to integer (nullable)
    if "Nach_Hst_Id" in reisende.columns:
        reisende["Nach_Hst_Id"] = reisende["Nach_Hst_Id"].astype("Int64")

    # Parse departure time FZ_AB -> hour of day
    if "FZ_AB" in reisende.columns:
        reisende["hour"] = pd.to_datetime(
            reisende["FZ_AB"], format="%H:%M:%S", errors="coerce"
        ).dt.hour
        reisende["hour"].fillna(-1, inplace=True)
    else:
        logger.warning("FZ_AB column not found; setting hour=-1 for all records.")
        reisende["hour"] = -1

    # -------------------------------
    # 3. Join TAGTYP for weekday info
    # -------------------------------
    if "Tagtyp_Id" in tagtyp.columns:
        tagtyp["Tagtyp_Id"] = tagtyp["Tagtyp_Id"].astype("int64")

    if "Tagtyp_Id" in reisende.columns and "Tagtyp_Id" in tagtyp.columns:
        reisende = reisende.merge(
            tagtyp[["Tagtyp_Id", "Tagtypname", "Bemerkung"]],
            on="Tagtyp_Id",
            how="left",
        )
    else:
        logger.warning("Tagtyp_Id missing in either REISENDE or TAGTYP; skipping join.")

    # For now: keep all days (same as notebook, no weekday filter)
    reisende_weekday = reisende.copy()

    # -------------------------------
    # 4. Build flow dataset (segment + hour)
    # -------------------------------
    # Drop rows without next stop
    flows = reisende_weekday.dropna(subset=["Nach_Hst_Id"]).copy()

    # Only keep rows with valid hour
    flows = flows[flows["hour"] >= 0].copy()

    # Aggregate: total Besetzung per segment + hour
    segment_hour = (
        flows
        .groupby(["Haltestellen_Id", "Nach_Hst_Id", "hour"], as_index=False)
        .agg(
            flow_besetzung=("Besetzung", "sum"),
            n_runs=("Plan_Fahrt_Id", "nunique"),
        )
    )

    logger.info(
        "Built segment_hour: %d rows, %d unique segments.",
        len(segment_hour),
        segment_hour[["Haltestellen_Id", "Nach_Hst_Id"]].drop_duplicates().shape[0],
    )

    # -------------------------------
    # 5. Load ZVV GTFS for stop coords
    # -------------------------------
    gtfs_url_default = (
        "https://data.stadt-zuerich.ch/dataset/"
        "vbz_fahrplandaten_gtfs/download/2025_google_transit.zip"
    )
    gtfs_url = os.getenv("GTFS_URL", gtfs_url_default)

    logger.info("Downloading GTFS feed from %s ...", gtfs_url)
    resp = requests.get(gtfs_url)
    resp.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        with zf.open("stops.txt") as f:
            gtfs_stops = pd.read_csv(f)

    # Normalize names and match
    haltestellen["name_norm"] = haltestellen["Haltestellenlangname"].astype(str).map(
        normalize_name
    )
    gtfs_stops["name_norm"] = gtfs_stops["stop_name"].astype(str).map(normalize_name)

    gtfs_stops_grouped = (
        gtfs_stops
        .groupby("name_norm", as_index=False)
        .agg(stop_lat=("stop_lat", "mean"), stop_lon=("stop_lon", "mean"))
    )

    stops_match = haltestellen.merge(
        gtfs_stops_grouped,
        on="name_norm",
        how="left",
    )

    coverage = stops_match["stop_lat"].notna().mean()
    logger.info("Match coverage (VBZ HALTESTELLEN with GTFS coords): %.1f%%", coverage * 100)

    # -------------------------------
    # 7. Matched stops to GeoDataFrame
    # -------------------------------
    stops_geo = stops_match.dropna(subset=["stop_lat", "stop_lon"]).copy()
    stops_geo_gdf = gpd.GeoDataFrame(
        stops_geo,
        geometry=gpd.points_from_xy(stops_geo["stop_lon"], stops_geo["stop_lat"]),
        crs="EPSG:4326",
    )

    # For osmnx graph extent
    minx, miny, maxx, maxy = stops_geo_gdf.total_bounds

    # -------------------------------
    # 8. Download street network via osmnx
    # -------------------------------
    ox.settings.log_console = False
    ox.settings.use_cache = True

    # Add some margin around bbox
    margin = 0.01  # ~1 km at this latitude
    north = maxy + margin
    south = miny - margin
    east = maxx + margin
    west = minx - margin

    logger.info("Downloading street network (driveable roads) from OSM...")
    # Correct call signature for osmnx.graph_from_bbox
    G = ox.graph_from_bbox(north, south, east, west, network_type="drive")

    logger.info("Downloaded graph with %d nodes, %d edges.", len(G.nodes), len(G.edges))

    # Project graph to a metric CRS
    G_proj = ox.project_graph(G)
    graph_crs = G_proj.graph["crs"]
    logger.info("Graph projected CRS: %s", graph_crs)

    # -------------------------------
    # 9. Project stops into graph CRS and snap to nearest nodes
    # -------------------------------
    stops_graph = stops_geo_gdf.to_crs(graph_crs).copy()
    xs = stops_graph.geometry.x.values
    ys = stops_graph.geometry.y.values

    logger.info("Finding nearest graph nodes for each stop...")
    nearest_nodes = ox.distance.nearest_nodes(G_proj, X=xs, Y=ys)
    stops_graph["nearest_node"] = nearest_nodes

    # Make sure Haltestellen_Id is string for consistent lookup
    stops_graph["Haltestellen_Id"] = (
        stops_graph["Haltestellen_Id"].astype("Int64").astype(str)
    )

    stop_node_lookup = (
        stops_graph[["Haltestellen_Id", "nearest_node"]]
        .drop_duplicates(subset="Haltestellen_Id")
        .set_index("Haltestellen_Id")["nearest_node"]
        .to_dict()
    )

    logger.info("Built stop_node_lookup for %d stops.", len(stop_node_lookup))

    # -------------------------------
    # 10. Compute street-following paths per segment
    # -------------------------------
    # Convert IDs in segment_hour to string to match stop_node_lookup
    segment_hour["Haltestellen_Id"] = segment_hour["Haltestellen_Id"].astype(str)
    segment_hour["Nach_Hst_Id"] = segment_hour["Nach_Hst_Id"].astype(str)

    unique_segments = (
        segment_hour[["Haltestellen_Id", "Nach_Hst_Id"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )

    logger.info("Unique segments to route: %d", len(unique_segments))

    geoms = []
    for idx, row in unique_segments.iterrows():
        if idx > 0 and idx % 1000 == 0:
            logger.info("Processed %d segments...", idx)
        geom = path_linestring(
            G_proj,
            stop_node_lookup,
            row["Haltestellen_Id"],
            row["Nach_Hst_Id"],
        )
        geoms.append(geom)

    unique_segments["geometry"] = geoms
    before = len(unique_segments)
    unique_segments = unique_segments[~unique_segments["geometry"].isna()].copy()
    logger.info(
        "Segments with valid street-following geometry: %d (out of %d)",
        len(unique_segments),
        before,
    )

    # -------------------------------
    # Attach geometries to flows
    # -------------------------------
    flows_geom = segment_hour.merge(
        unique_segments[["Haltestellen_Id", "Nach_Hst_Id", "geometry"]],
        on=["Haltestellen_Id", "Nach_Hst_Id"],
        how="inner",
    )

    flows_geom = gpd.GeoDataFrame(flows_geom, geometry="geometry", crs=graph_crs)
    logger.info("Segments with flow + geometry: %d", len(flows_geom))

    # -------------------------------
    # Reproject and aggregate (daily/hourly)
    # -------------------------------
    target_crs = "EPSG:2056"
    flows_hourly = flows_geom.to_crs(target_crs)

    flows_day = (
        flows_hourly
        .groupby(["Haltestellen_Id", "Nach_Hst_Id"], as_index=False)
        .agg(
            flow_besetzung=("flow_besetzung", "sum"),
            n_runs=("n_runs", "sum"),
            geometry=("geometry", "first"),
        )
    )
    flows_day_gdf = gpd.GeoDataFrame(flows_day, geometry="geometry", crs=target_crs)

    logger.info("Hourly flows rows: %d", len(flows_hourly))
    logger.info("Daily  flows rows: %d", len(flows_day_gdf))

    # Choose output dataframe
    if output_level == "hourly":
        gdf_out = flows_hourly
    else:
        gdf_out = flows_day_gdf

    # -------------------------------
    # Save output (single file)
    # -------------------------------
    logger.info("Writing final output (%s, %s)...", output_level, output_format)

    if use_s3:
        s3 = boto3.client("s3")
        logger.info("Writing output to s3://%s/%s", output_bucket, output_key)

        with tempfile.TemporaryDirectory() as tmpdir:
            if output_format == "gpkg":
                tmp_filename = "vbz_flows.gpkg"
                tmp_path = os.path.join(tmpdir, tmp_filename)
                gdf_out.to_file(tmp_path, driver="GPKG")
            else:
                tmp_filename = "vbz_flows.parquet"
                tmp_path = os.path.join(tmpdir, tmp_filename)
                gdf_out.to_parquet(tmp_path, index=False)

            size_bytes = os.path.getsize(tmp_path)
            logger.info(
                "Temporary %s written to %s (size=%d bytes)",
                output_format,
                tmp_path,
                size_bytes,
            )

            s3.upload_file(tmp_path, output_bucket, output_key)

        logger.info("Uploaded output to s3://%s/%s", output_bucket, output_key)

    else:
        # Ensure local directory exists
        out_dir = os.path.dirname(output_path)
        if out_dir:
            os.makedirs(out_dir, exist_ok=True)

        if output_format == "gpkg":
            gdf_out.to_file(output_path, driver="GPKG")
        else:
            gdf_out.to_parquet(output_path, index=False)

        logger.info("Done (local). Output path: %s", output_path)

    logger.info("Done. Output rows: %d", len(gdf_out))


if __name__ == "__main__":
    main()
