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
#   NEW: wrapper that creates quad polygons for ANY facade geometry
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

    use_s3 = all([input_bucket, input_key, output_bucket, output_key])

    # --- Output format: gpkg (default) or parquet/geoparquet ---
    output_format_raw = os.getenv("OUTPUT_FORMAT", "gpkg").strip().lower()
    if output_format_raw in ("gpkg", "geopackage"):
        output_format = "gpkg"
    elif output_format_raw in ("parquet", "geoparquet"):
        output_format = "parquet"
    else:
        logger.warning(
            "Unknown OUTPUT_FORMAT=%r, falling back to 'gpkg'.",
            output_format_raw
        )
        output_format = "gpkg"

    local_bucket_root = os.getenv("LOCAL_BUCKET_ROOT", "/data")
    if not input_key:
        input_key = "bronze/fassaden_zuerich.gpkg"
    if not output_key:
        output_key = f"silver/facade_visibility.{output_format}"

    if use_s3:
        logger.info("Running in S3 mode.")
    else:
        logger.info("S3 env vars not fully set, falling back to local mode.")
        input_path = os.path.join(local_bucket_root, input_key)
        output_path = os.path.join(local_bucket_root, output_key)

    visibility_distance_m = float(os.getenv("VISIBILITY_DISTANCE_M", "30.0"))
    visibility_half_angle = float(os.getenv("VISIBILITY_HALF_ANGLE_DEG", "45.0"))
    visibility_side = os.getenv("VISIBILITY_SIDE", "left")
    visibility_ray_sign = int(os.getenv("VISIBILITY_RAY_SIGN", "1"))

    logger.info("Starting facade visibility batch process")
    logger.info("Output format: %s", output_format)

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
        logger.info("Input (local):  %s", input_path)
        logger.info("Output (local): %s", output_path)
        if not os.path.exists(input_path):
            raise FileNotFoundError(f"Input file not found: {input_path}")
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
    # Write output (GPKG or GeoParquet)
    # --------------------------------------------------------
    if use_s3:
        logger.info("Writing output to s3://%s/%s", output_bucket, output_key)
        s3 = boto3.client("s3")

        with tempfile.TemporaryDirectory() as tmpdir:
            if output_format == "gpkg":
                tmp_filename = "facade_visibility.gpkg"
                tmp_path = os.path.join(tmpdir, tmp_filename)
                gdf_out.to_file(tmp_path, driver="GPKG")
            else:  # parquet / geoparquet
                tmp_filename = "facade_visibility.parquet"
                tmp_path = os.path.join(tmpdir, tmp_filename)
                # index=False keeps it clean
                gdf_out.to_parquet(tmp_path, index=False)

            size_bytes = os.path.getsize(tmp_path)
            logger.info(
                "Temporary %s written to %s (size=%d bytes)",
                output_format, tmp_path, size_bytes
            )

            s3.upload_file(tmp_path, output_bucket, output_key)

        logger.info("Uploaded output to s3://%s/%s", output_bucket, output_key)
    else:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        if output_format == "gpkg":
            gdf_out.to_file(output_path, driver="GPKG")
        else:
            gdf_out.to_parquet(output_path, index=False)
        logger.info("Done (local). Output rows: %d", len(gdf_out))

    logger.info("Done. Output rows: %d", len(gdf_out))


if __name__ == "__main__":
    main()
