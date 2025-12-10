"""
Facade Visibility Batch Model
=============================

This script computes visibility polygons for facade geometries and exports
**two output formats** for each run:

    1) GeoPackage (.gpkg)
    2) GeoParquet (.parquet)

It supports **both local execution** and **AWS ECS/Fargate execution**.


ENVIRONMENT VARIABLES
---------------------

### Core Inputs (required in ECS mode)
- INPUT_BUCKET
    Name of the S3 bucket containing the input file.
- INPUT_KEY
    Path inside the bucket for the input GPKG file.
    Example: "bronze/fassaden_zuerich.gpkg"

- OUTPUT_BUCKET
    S3 bucket where output files should be written.
- OUTPUT_KEY
    Base output key (directory or filename stem).
    The script will generate:
        <VERSION_TAG>_<stem>.gpkg
        <VERSION_TAG>_<stem>.parquet

    Example:
        OUTPUT_KEY="silver/facade_visibility"
        VERSION_TAG="v1"
    → Output:
        silver/v1_facade_visibility.gpkg
        silver/v1_facade_visibility.parquet

### Output Configuration
- VERSION_TAG
    Prefix added to output filenames.
    REQUIRED in production so each ECS job produces uniquely versioned files.

### Geometry Processing Parameters
- VISIBILITY_DISTANCE_M       (default: "30.0")
    Length of visibility rays in meters.

- VISIBILITY_HALF_ANGLE_DEG   (default: "45.0")
    Half-angle of the visibility cone from each endpoint.

- VISIBILITY_SIDE             (default: "left")
    Determines which side to project to: "left" or "right".

- VISIBILITY_RAY_SIGN         (default: "1")
    Controls angle direction for the ray emission.

- PROCESSING_CRS              (optional)
    CRS used during computation, e.g. "EPSG:2056".
    Output is re-projected back to original CRS.

### Local Mode Variables
These are used only when S3 variables are NOT set.
- LOCAL_BUCKET_ROOT           (default: "/data")
    Local folder acting as a pseudo S3 bucket.
    Example: /data/bronze/fassaden_zuerich.gpkg

### Testing Options
- MAX_ROWS
    Limit number of rows to process (debugging, dry-runs).


OUTPUT FILES
------------

The model **always writes both**:

1. GeoPackage (.gpkg)
2. GeoParquet (.parquet)

Both files contain **identical attributes and geometry**, including:

- x_lon_center   (WGS84 longitude of polygon centroid)
- y_lat_centre   (WGS84 latitude of polygon centroid)

Used by dashboard applications for map positioning.


RUNNING LOCALLY WITH DOCKER
---------------------------

Example local command using a local folder mapped to `/data`
(which simulates the "bucket"):

    docker run --rm \
        -v "$(pwd)/local_bucket:/data" \
        -e LOCAL_BUCKET_ROOT=/data \
        -e INPUT_KEY="bronze/fassaden_zuerich.gpkg" \
        -e OUTPUT_KEY="silver/facade_visibility" \
        -e VERSION_TAG="v1" \
        -e VISIBILITY_DISTANCE_M=30.0 \
        -e VISIBILITY_HALF_ANGLE_DEG=45.0 \
        facade-visibility:latest


SETTING ENVIRONMENT VARIABLES IN ECS / FARGATE
-----------------------------------------------

In the ECS Task Definition > Container Definitions > Environment:

    Name                      | Value
    ---------------------------------------------------------------
    INPUT_BUCKET             | facade-project-dev
    INPUT_KEY                | bronze/fassaden_zürich.gpkg
    OUTPUT_BUCKET            | facade-project-dev
    OUTPUT_KEY               | gold/facade_visibility
    VERSION_TAG              | V1
    VISIBILITY_DISTANCE_M    | 30
    VISIBILITY_HALF_ANGLE_DEG| 45
    VISIBILITY_SIDE          | left
    VISIBILITY_RAY_SIGN      | 1
    PROCESSING_CRS           | EPSG:2056

ECS automatically injects these variables into your running container.
If *all four* input/output S3 vars (INPUT_BUCKET, INPUT_KEY, OUTPUT_BUCKET,
OUTPUT_KEY) are defined, the model runs in **S3 mode**.

Otherwise it falls back to **local mode**.


NOTES
-----
- The script logs progress with ~20 evenly spaced status updates.
- All temporary files in ECS are written to the container's `/tmp`.
- AWS credentials are inherited through the ECS Task Role.
- Both outputs are uploaded to S3 using the derived final S3 keys.

"""

import os
import math
import logging
import boto3
import tempfile

import geopandas as gpd
from shapely.geometry import (
    LineString, MultiLineString, Point, Polygon
)
from shapely.ops import unary_union

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)

logger = logging.getLogger(__name__)


# ============================================================
#   GEOMETRY HELPERS — YOUR NEW ALGORITHM
# ============================================================

def _unit_vector(p_from, p_to):
    dx = p_to.x - p_from.x
    dy = p_to.y - p_from.y
    L = (dx*dx + dy*dy) ** 0.5
    if L == 0:
        return (0.0, 0.0)
    return (dx/L, dy/L)


def _angle_of_vec_deg(ux, uy):
    return math.degrees(math.atan2(uy, ux))


def _vec_from_angle_deg(angle_deg, length):
    t = math.radians(angle_deg)
    return (length * math.cos(t), length * math.sin(t))


def _endpoint_single_ray(endpoint_pt: Point, normal_angle_deg: float,
                         half_angle_deg: float, distance_m: float,
                         ray_sign: int = +1) -> Point:
    """
    One ray per endpoint at angle = normal +/- half_angle.
    ray_sign=+1 -> normal + half_angle (CCW)
    ray_sign=-1 -> normal - half_angle (CW)
    """
    a = normal_angle_deg + ray_sign * half_angle_deg
    vx, vy = _vec_from_angle_deg(a, distance_m)
    return Point(endpoint_pt.x + vx, endpoint_pt.y + vy)


def facade_visibility_quad_polygon(
    line: LineString,
    distance_m: float = 30.0,
    half_angle_deg: float = 45.0,
    side: str = "left",
    ray_sign: int = +1,
) -> Polygon | None:
    """
    Build a 4-vertex polygon:
      base = the original line
      apex = two rays of length distance_m from endpoints.
    """
    if not isinstance(line, LineString) or line.length == 0:
        return None

    p_start = Point(line.coords[0])
    p_end   = Point(line.coords[-1])

    # Tangent and outward normal
    ux, uy  = _unit_vector(p_start, p_end)
    theta   = _angle_of_vec_deg(ux, uy)
    normal  = theta + 90.0 if side.lower() == "left" else theta - 90.0

    # A single ray at each endpoint
    p_start_ray = _endpoint_single_ray(
        p_start, normal, half_angle_deg, distance_m, ray_sign=ray_sign
    )
    p_end_ray   = _endpoint_single_ray(
        p_end,   normal, half_angle_deg, distance_m, ray_sign=-ray_sign
    )

    # Polygon ring
    ring = [p_start, p_end, p_end_ray, p_start_ray, p_start]
    poly = Polygon([(pt.x, pt.y) for pt in ring])

    if not poly.is_valid:
        ring = [p_start, p_end, p_start_ray, p_end_ray, p_start]
        poly = Polygon([(pt.x, pt.y) for pt in ring])

    return poly


def explode_to_lines(geom):
    """Yield individual LineStrings from LineString or MultiLineString."""
    if geom is None or geom.is_empty:
        return
    if isinstance(geom, LineString):
        yield geom
    elif isinstance(geom, MultiLineString):
        for part in geom.geoms:
            if isinstance(part, LineString):
                yield part


# ============================================================
#   wrapper that creates quad polygons for ANY facade geometry
# ============================================================

def compute_visibility_for_facade(
    geom,
    distance_m=30.0,
    half_angle_deg=45.0,
    side="left",
    ray_sign=+1,
):
    """
    Apply the quad polygon algorithm to every line part.
    Then union them into a single polygon.
    """
    if geom is None or geom.is_empty:
        return None

    polys = []

    for line in explode_to_lines(geom):
        poly = facade_visibility_quad_polygon(
            line,
            distance_m=distance_m,
            half_angle_deg=half_angle_deg,
            side=side,
            ray_sign=ray_sign,
        )
        if poly and not poly.is_empty:
            polys.append(poly)

    if not polys:
        return None

    return unary_union(polys)


# ============================================================
#   MAIN BATCH SCRIPT
# ============================================================

def main():
    # --- S3-based config ---
    input_bucket = os.getenv("INPUT_BUCKET")
    input_key = os.getenv("INPUT_KEY")
    output_bucket = os.getenv("OUTPUT_BUCKET")
    output_key = os.getenv("OUTPUT_KEY")
    version_tag = os.getenv("VERSION_TAG", "").strip()

    use_s3 = all([input_bucket, input_key, output_bucket, output_key])

    local_bucket_root = os.getenv("LOCAL_BUCKET_ROOT", "/data")
    if not input_key:
        input_key = "bronze/fassaden_zuerich.gpkg"
    if not output_key:
        # base key used to derive both .gpkg and .parquet
        output_key = "silver/facade_visibility"

    if use_s3:
        logger.info("Running in S3 mode.")
    else:
        logger.info("S3 env vars not fully set, falling back to local mode.")
        input_path = os.path.join(local_bucket_root, input_key)
        if not os.path.exists(input_path):
            raise FileNotFoundError(f"Input file not found: {input_path}")
        logger.info("Input (local): %s", input_path)

    visibility_distance_m = float(os.getenv("VISIBILITY_DISTANCE_M", "30.0"))
    visibility_half_angle = float(os.getenv("VISIBILITY_HALF_ANGLE_DEG", "45.0"))
    visibility_side = os.getenv("VISIBILITY_SIDE", "left")
    visibility_ray_sign = int(os.getenv("VISIBILITY_RAY_SIGN", "1"))

    logger.info("Starting facade visibility batch process")

    # --------- LOAD INPUT ---------
    if use_s3:
        s3 = boto3.client("s3")
        logger.info("Loading from s3://%s/%s", input_bucket, input_key)
        with tempfile.NamedTemporaryFile(suffix=".gpkg") as tmp:
            s3.download_fileobj(input_bucket, input_key, tmp)
            tmp.flush()
            tmp.seek(0)
            gdf = gpd.read_file(tmp.name)
    else:
        gdf = gpd.read_file(input_path)

    # optional: limit rows for testing via MAX_ROWS
    max_rows_env = os.getenv("MAX_ROWS", "").strip()
    if max_rows_env:
        try:
            max_rows_val = int(max_rows_env)
            if max_rows_val > 0:
                gdf = gdf.head(max_rows_val)
                logger.info(
                    "MAX_ROWS=%d → limiting to first %d rows",
                    max_rows_val,
                    len(gdf)
                )
        except ValueError:
            logger.warning("Invalid MAX_ROWS=%r, ignoring.", max_rows_env)

    logger.info("Loaded %d facades", len(gdf))
    logger.info("Input CRS: %s", gdf.crs)

    original_crs = gdf.crs

    processing_crs = os.getenv("PROCESSING_CRS", "")
    if processing_crs:
        gdf = gdf.to_crs(processing_crs)

    # --------------------------------------------------------
    # Compute polygon visibility per row, with progress logging
    # --------------------------------------------------------
    total = len(gdf)
    if total == 0:
        logger.warning("No facades found in input. Nothing to do.")
        return

    log_every = max(1, total // 20)  # ~20 log lines max

    logger.info(
        "Computing visibility polygons for %d facades (log every %d rows)...",
        total, log_every
    )

    vis_geoms = []
    for idx, geom in enumerate(gdf.geometry):
        poly = compute_visibility_for_facade(
            geom,
            distance_m=visibility_distance_m,
            half_angle_deg=visibility_half_angle,
            side=visibility_side,
            ray_sign=visibility_ray_sign,
        )
        vis_geoms.append(poly)

        if (idx + 1) % log_every == 0 or (idx + 1) == total:
            pct = 100.0 * (idx + 1) / total
            logger.info("Processed %d/%d facades (%.1f%%)", idx + 1, total, pct)

    gdf_out = gdf.copy()
    gdf_out["geometry"] = vis_geoms

    # Drop empty
    before = len(gdf_out)
    gdf_out = gdf_out[gdf_out.geometry.notnull() & ~gdf_out.geometry.is_empty]
    after = len(gdf_out)
    logger.info("Dropped %d rows with empty geometry", before - after)

    # Back to original CRS
    if processing_crs and original_crs and original_crs != processing_crs:
        gdf_out = gdf_out.to_crs(original_crs)

    # --------------------------------------------------------
    # Add centroid coordinates in WGS84 (EPSG:4326) as attributes
    # --------------------------------------------------------
    if gdf_out.crs is not None:
        try:
            gdf_wgs = gdf_out.to_crs(epsg=4326)
            centers = gdf_wgs.geometry.centroid
            gdf_out["x_lon_center"] = centers.x
            gdf_out["y_lat_centre"] = centers.y
            logger.info("Added centroid attributes x_lon_center, y_lat_centre in WGS84.")
        except Exception as e:
            logger.warning("Failed to compute WGS84 centroids: %s", e)
            gdf_out["x_lon_center"] = None
            gdf_out["y_lat_centre"] = None
    else:
        logger.warning("CRS is not set; cannot compute WGS84 centroids.")
        gdf_out["x_lon_center"] = None
        gdf_out["y_lat_centre"] = None

    # --------------------------------------------------------
    # Derive output names and paths/keys (always GPKG + Parquet)
    # VERSION_TAG is prefixed to the file name if provided.
    # --------------------------------------------------------
    base_key = output_key  # e.g. "silver/facade_visibility.gpkg" or "silver/facade_visibility"
    out_dirname = os.path.dirname(base_key)
    base_name = os.path.basename(base_key)
    stem, _ext = os.path.splitext(base_name)
    if not stem:
        stem = "facade_visibility"

    if version_tag:
        file_stem = f"{version_tag}_{stem}"
    else:
        file_stem = stem

    gpkg_filename = f"{file_stem}.gpkg"
    parquet_filename = f"{file_stem}.parquet"

    if use_s3:
        # Build S3 keys using "/" to avoid backslashes
        if out_dirname:
            gpkg_key = f"{out_dirname}/{gpkg_filename}"
            parquet_key = f"{out_dirname}/{parquet_filename}"
        else:
            gpkg_key = gpkg_filename
            parquet_key = parquet_filename

        logger.info("Writing outputs to s3://%s/{%s, %s}", output_bucket, gpkg_key, parquet_key)
        s3 = boto3.client("s3")

        with tempfile.TemporaryDirectory() as tmpdir:
            # GPKG
            gpkg_tmp_path = os.path.join(tmpdir, gpkg_filename)
            gdf_out.to_file(gpkg_tmp_path, driver="GPKG")
            size_gpkg = os.path.getsize(gpkg_tmp_path)
            logger.info(
                "Temporary GPKG written to %s (size=%d bytes)",
                gpkg_tmp_path, size_gpkg
            )
            s3.upload_file(gpkg_tmp_path, output_bucket, gpkg_key)
            logger.info("Uploaded GPKG to s3://%s/%s", output_bucket, gpkg_key)

            # GeoParquet
            parquet_tmp_path = os.path.join(tmpdir, parquet_filename)
            gdf_out.to_parquet(parquet_tmp_path, index=False)
            size_parquet = os.path.getsize(parquet_tmp_path)
            logger.info(
                "Temporary Parquet written to %s (size=%d bytes)",
                parquet_tmp_path, size_parquet
            )
            s3.upload_file(parquet_tmp_path, output_bucket, parquet_key)
            logger.info("Uploaded Parquet to s3://%s/%s", output_bucket, parquet_key)

        logger.info("Done (S3 mode). Output rows: %d", len(gdf_out))

    else:
        # Local file system
        if out_dirname:
            local_out_dir = os.path.join(local_bucket_root, out_dirname)
        else:
            local_out_dir = local_bucket_root

        os.makedirs(local_out_dir, exist_ok=True)

        gpkg_path = os.path.join(local_out_dir, gpkg_filename)
        parquet_path = os.path.join(local_out_dir, parquet_filename)

        logger.info("Writing local GPKG: %s", gpkg_path)
        gdf_out.to_file(gpkg_path, driver="GPKG")

        logger.info("Writing local GeoParquet: %s", parquet_path)
        gdf_out.to_parquet(parquet_path, index=False)

        logger.info(
            "Done (local mode). Output rows: %d, GPKG: %s, Parquet: %s",
            len(gdf_out), gpkg_path, parquet_path
        )

    logger.info("Done. Output rows: %d", len(gdf_out))


if __name__ == "__main__":
    main()
