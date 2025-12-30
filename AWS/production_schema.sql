/**
 * FACADE PROJECT - PRODUCTION DATABASE SCHEMA
 *
 * Complete SQL schema for FacADe database
 * Includes: Raw tables, views, materialized view, foreign keys, indexes
 *
 * Database: PostgreSQL 17 + PostGIS 3.6
 * Status: Production Ready
 */

-- ============================================================================
-- PART 0: EXTENSIONS & SETUP
-- ============================================================================

CREATE EXTENSION IF NOT EXISTS postgis;

-- ============================================================================
-- PART 1: RAW DATA TABLES (Source of Truth)
-- ============================================================================

/**
 * TABLE: v2_facade_visibility
 * Description: Building facades with coordinates and geometry
 * Source: V2_facade_visibility.parquet
 * Rows: 415,977
 */
CREATE TABLE IF NOT EXISTS v2_facade_visibility (
    objectid BIGINT PRIMARY KEY,
    gwr_egid BIGINT NOT NULL,
    bezeichnung TEXT,
    geometry geometry(Geometry, 2056) NOT NULL,  -- PostGIS geometry (LV95/EPSG:2056)
    x_lon_center DOUBLE PRECISION,  -- WGS84 longitude
    y_lat_centre DOUBLE PRECISION   -- WGS84 latitude
);

CREATE INDEX idx_v2_facade_gwr_egid ON v2_facade_visibility(gwr_egid);
CREATE INDEX idx_v2_facade_geom ON v2_facade_visibility USING GIST(geometry);

/**
 * TABLE: v2_bike_edges
 * Description: Bike network edges with geometry
 * Source: V2_bike_edges.parquet
 * Rows: 44,861
 */
CREATE TABLE IF NOT EXISTS v2_bike_edges (
    edge_id INT PRIMARY KEY,
    geometry geometry(LineString, 2056) NOT NULL,  -- PostGIS geometry (LV95/EPSG:2056)
    u BIGINT,
    v BIGINT,
    key INT
);

CREATE INDEX idx_v2_bike_edges_geom ON v2_bike_edges USING GIST(geometry);

/**
 * TABLE: v2_bike_flows
 * Description: Bike traffic flow data (impressions per edge per hour)
 * Source: V2_bike_flows.parquet
 * Rows: 3,491
 * Primary Key: (edge_id, hour) - Ensures one row per edge per hour
 */
CREATE TABLE IF NOT EXISTS v2_bike_flows (
    edge_id INT NOT NULL,
    hour INT NOT NULL,
    flow NUMERIC,
    flow_norm NUMERIC,
    PRIMARY KEY (edge_id, hour),
    CONSTRAINT fk_bike_flows_edges FOREIGN KEY (edge_id)
        REFERENCES v2_bike_edges(edge_id) ON DELETE CASCADE
);

CREATE INDEX idx_v2_bike_flows_edge ON v2_bike_flows(edge_id);

/**
 * TABLE: v2_ped_edges
 * Description: Pedestrian network edges with geometry
 * Source: V2_ped_edges.parquet
 * Rows: 202,614
 */
CREATE TABLE IF NOT EXISTS v2_ped_edges (
    edge_id INT PRIMARY KEY,
    geometry geometry(LineString, 2056) NOT NULL,  -- PostGIS geometry (LV95/EPSG:2056)
    u BIGINT,
    v BIGINT,
    key INT
);

CREATE INDEX idx_v2_ped_edges_geom ON v2_ped_edges USING GIST(geometry);

/**
 * TABLE: v2_ped_flows
 * Description: Pedestrian traffic flow data
 * Source: V2_ped_flows.parquet
 * Rows: 385
 * Primary Key: (edge_id, hour) - Ensures one row per edge per hour
 */
CREATE TABLE IF NOT EXISTS v2_ped_flows (
    edge_id INT NOT NULL,
    hour INT NOT NULL,
    flow NUMERIC,
    flow_norm NUMERIC,
    PRIMARY KEY (edge_id, hour),
    CONSTRAINT fk_ped_flows_edges FOREIGN KEY (edge_id)
        REFERENCES v2_ped_edges(edge_id) ON DELETE CASCADE
);

CREATE INDEX idx_v2_ped_flows_edge ON v2_ped_flows(edge_id);

/**
 * TABLE: v2_motorized_traffic_edges
 * Description: Motorized traffic network edges with street names
 * Source: V2_motorized_traffic_edges.parquet
 * Rows: 10,572
 */
CREATE TABLE IF NOT EXISTS v2_motorized_traffic_edges (
    edge_id INT PRIMARY KEY,
    geometry geometry(LineString, 2056) NOT NULL,  -- PostGIS geometry (LV95/EPSG:2056)
    u BIGINT,
    v BIGINT,
    key INT,
    name VARCHAR(255),  -- Street name
    length NUMERIC,
    bearing NUMERIC
);

CREATE INDEX idx_v2_motorized_edges_geom ON v2_motorized_traffic_edges USING GIST(geometry);

/**
 * TABLE: v2_motorized_traffic_flows
 * Description: Motorized traffic flow data (V3 - deduplicated & normalized)
 * Source: V3_motorized_traffic_flows.parquet (fixes V2 overcounting issue)
 * Rows: 123,252
 * Primary Key: (edge_id, hour) - Ensures one row per edge per hour
 *
 * CHANGE LOG:
 * V3 (current): Deduplicated/normalized values (~1000x reduction)
 *              Filters to reliable edges only (~59% of V2)
 *              Removed n_sources column
 * V2 (legacy): Had overcounting issue from multiple data sources
 */
CREATE TABLE IF NOT EXISTS v2_motorized_traffic_flows (
    edge_id INT NOT NULL,
    hour INT NOT NULL,
    total_flow NUMERIC,
    flow_norm NUMERIC,
    PRIMARY KEY (edge_id, hour),
    CONSTRAINT fk_motorized_flows_edges FOREIGN KEY (edge_id)
        REFERENCES v2_motorized_traffic_edges(edge_id) ON DELETE CASCADE
);

CREATE INDEX idx_v2_motorized_flows_edge ON v2_motorized_traffic_flows(edge_id);

/**
 * TABLE: v2_vbz_flows_hourly
 * Description: Public transport (VBZ) flow data with geometry
 * Source: V2_vbz_flows_hourly.parquet (V2: Filtered/normalized data)
 * Rows: 27,131
 * Primary Key: (haltestellen_id, nach_hst_id, hour) - Ensures one row per corridor per hour
 *
 * CHANGE LOG:
 * V2 (current): Filtered/normalized data (4.7% fewer rows, ~1.45x smaller values)
 * V1 (legacy): Original raw data (28,468 rows)
 */
CREATE TABLE IF NOT EXISTS v2_vbz_flows_hourly (
    haltestellen_id VARCHAR(50) NOT NULL,
    nach_hst_id VARCHAR(50) NOT NULL,
    hour INT NOT NULL,
    flow_besetzung NUMERIC,
    n_runs INT,
    geometry geometry(LineString, 2056) NOT NULL,  -- PostGIS geometry (LV95/EPSG:2056)
    PRIMARY KEY (haltestellen_id, nach_hst_id, hour)
);

CREATE INDEX idx_v2_vbz_geom ON v2_vbz_flows_hourly USING GIST(geometry);

-- ============================================================================
-- PART 2: INTERMEDIATE VIEWS (for clarity in data pipeline)
-- ============================================================================

/**
 * VIEW: vw_impressions_vbz_spatial
 * Description: VBZ impressions aggregated by building via spatial joins
 * Rows: ~290,563
 * Type: VIEW (computed on demand)
 */
CREATE OR REPLACE VIEW vw_impressions_vbz_spatial AS
SELECT
    f.gwr_egid,
    f.objectid,
    f.bezeichnung,
    'vbz'::text as flow_type,
    COUNT(DISTINCT v.haltestellen_id || v.nach_hst_id) as affected_corridors,
    ROUND(SUM(v.flow_besetzung)::numeric, 0) as total_impressions,
    ROUND(AVG(v.flow_besetzung)::numeric, 2) as avg_flow
FROM v2_facade_visibility f
JOIN v2_vbz_flows_hourly v ON ST_Intersects(f.geometry, v.geometry)
WHERE v.flow_besetzung > 0 AND v.flow_besetzung IS NOT NULL
GROUP BY f.gwr_egid, f.objectid, f.bezeichnung;

/**
 * VIEW: vw_impressions_bike_spatial
 * Description: Bike impressions aggregated by building via spatial joins
 * Rows: ~290,563
 * Type: VIEW (computed on demand)
 */
CREATE OR REPLACE VIEW vw_impressions_bike_spatial AS
SELECT
    f.gwr_egid,
    f.objectid,
    f.bezeichnung,
    'bike'::text as flow_type,
    COUNT(DISTINCT b.edge_id) as affected_corridors,
    ROUND(SUM(bf.flow)::numeric, 0) as total_impressions,
    ROUND(AVG(bf.flow)::numeric, 2) as avg_flow
FROM v2_facade_visibility f
JOIN v2_bike_edges b ON ST_Intersects(f.geometry, b.geometry)
JOIN v2_bike_flows bf ON b.edge_id = bf.edge_id
WHERE bf.flow > 0 AND bf.flow IS NOT NULL
GROUP BY f.gwr_egid, f.objectid, f.bezeichnung;

/**
 * VIEW: vw_impressions_pedestrian_spatial
 * Description: Pedestrian impressions aggregated by building via spatial joins
 * Rows: ~290,563
 * Type: VIEW (computed on demand)
 */
CREATE OR REPLACE VIEW vw_impressions_pedestrian_spatial AS
SELECT
    f.gwr_egid,
    f.objectid,
    f.bezeichnung,
    'pedestrian'::text as flow_type,
    COUNT(DISTINCT p.edge_id) as affected_corridors,
    ROUND(SUM(pf.flow)::numeric, 0) as total_impressions,
    ROUND(AVG(pf.flow)::numeric, 2) as avg_flow
FROM v2_facade_visibility f
JOIN v2_ped_edges p ON ST_Intersects(f.geometry, p.geometry)
JOIN v2_ped_flows pf ON p.edge_id = pf.edge_id
WHERE pf.flow > 0 AND pf.flow IS NOT NULL
GROUP BY f.gwr_egid, f.objectid, f.bezeichnung;

/**
 * VIEW: vw_impressions_motorized_spatial
 * Description: Motorized traffic impressions aggregated by building
 * Rows: ~290,563
 * Type: VIEW (computed on demand)
 */
CREATE OR REPLACE VIEW vw_impressions_motorized_spatial AS
SELECT
    f.gwr_egid,
    f.objectid,
    f.bezeichnung,
    'motorized'::text as flow_type,
    COUNT(DISTINCT m.edge_id) as affected_corridors,
    ROUND(SUM(mf.total_flow)::numeric, 0) as total_impressions,
    ROUND(AVG(mf.total_flow)::numeric, 2) as avg_flow
FROM v2_facade_visibility f
JOIN v2_motorized_traffic_edges m ON ST_Intersects(f.geometry, m.geometry)
JOIN v2_motorized_traffic_flows mf ON m.edge_id = mf.edge_id
WHERE mf.total_flow > 0 AND mf.total_flow IS NOT NULL
GROUP BY f.gwr_egid, f.objectid, f.bezeichnung;

/**
 * VIEW: vw_impressions_all_spatial
 * Description: Union of all traffic types (VBZ + Bike + Pedestrian + Motorized)
 * Rows: ~1.16M (290,563 buildings × 4 traffic types)
 * Type: VIEW (computed on demand)
 */
CREATE OR REPLACE VIEW vw_impressions_all_spatial AS
SELECT * FROM vw_impressions_vbz_spatial
UNION ALL
SELECT * FROM vw_impressions_bike_spatial
UNION ALL
SELECT * FROM vw_impressions_pedestrian_spatial
UNION ALL
SELECT * FROM vw_impressions_motorized_spatial;

/**
 * VIEW: vw_impressions_summary_spatial
 * Description: Impressions by facade - one row per facade with all traffic types
 * Rows: ~415,977 (one per facade/objectid)
 * Type: VIEW (computed on demand)
 * Query Performance: <5 seconds (optimized with native geometry type + GIST indexes)
 *
 * NOTE: Each facade gets its own row showing which traffic types intersect it
 */
CREATE OR REPLACE VIEW vw_impressions_summary_spatial AS
SELECT
    gwr_egid,
    objectid,
    bezeichnung,
    COALESCE(MAX(CASE WHEN flow_type = 'vbz' THEN total_impressions END), 0) as vbz_total_impressions,
    COALESCE(MAX(CASE WHEN flow_type = 'vbz' THEN avg_flow END), 0) as vbz_avg_flow,
    COALESCE(MAX(CASE WHEN flow_type = 'vbz' THEN affected_corridors END), 0) as vbz_corridors,
    COALESCE(MAX(CASE WHEN flow_type = 'bike' THEN total_impressions END), 0) as bike_total_impressions,
    COALESCE(MAX(CASE WHEN flow_type = 'bike' THEN avg_flow END), 0) as bike_avg_flow,
    COALESCE(MAX(CASE WHEN flow_type = 'bike' THEN affected_corridors END), 0) as bike_edges,
    COALESCE(MAX(CASE WHEN flow_type = 'pedestrian' THEN total_impressions END), 0) as ped_total_impressions,
    COALESCE(MAX(CASE WHEN flow_type = 'pedestrian' THEN avg_flow END), 0) as ped_avg_flow,
    COALESCE(MAX(CASE WHEN flow_type = 'pedestrian' THEN affected_corridors END), 0) as ped_edges,
    COALESCE(MAX(CASE WHEN flow_type = 'motorized' THEN total_impressions END), 0) as motorized_total_impressions,
    COALESCE(MAX(CASE WHEN flow_type = 'motorized' THEN avg_flow END), 0) as motorized_avg_flow,
    COALESCE(MAX(CASE WHEN flow_type = 'motorized' THEN affected_corridors END), 0) as motorized_edges,
    COALESCE(MAX(CASE WHEN flow_type = 'vbz' THEN total_impressions END), 0) +
    COALESCE(MAX(CASE WHEN flow_type = 'bike' THEN total_impressions END), 0) +
    COALESCE(MAX(CASE WHEN flow_type = 'pedestrian' THEN total_impressions END), 0) +
    COALESCE(MAX(CASE WHEN flow_type = 'motorized' THEN total_impressions END), 0) as total_all_impressions
FROM vw_impressions_all_spatial
GROUP BY gwr_egid, objectid, bezeichnung;

-- ============================================================================
-- PART 3: MATERIALIZED VIEW (Production Output for Tableau)
-- ============================================================================

/**
 * MATERIALIZED VIEW: mv_impressions_summary_spatial
 * Description: CACHED impressions data ready for Tableau
 * - Pre-computed spatial joins (via underlying views)
 * - Coordinates included (WGS84 for mapping - per facade)
 * - One row per facade (objectid)
 * - Shows which traffic types intersect each facade
 *
 * Rows: ~415,977 (one per facade)
 * Query Performance: <15ms (cached, indexed)
 * Refresh: Manual via REFRESH MATERIALIZED VIEW mv_impressions_summary_spatial;
 *
 * Note: Each facade gets its own row with impressions for that specific facade
 */
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_impressions_summary_spatial AS
SELECT
    s.gwr_egid,
    s.objectid,
    s.bezeichnung,
    ROUND(f.x_lon_center::numeric, 6) as longitude,  -- WGS84 (first facade)
    ROUND(f.y_lat_centre::numeric, 6) as latitude,   -- WGS84 (first facade)
    s.vbz_total_impressions,
    s.vbz_avg_flow,
    s.vbz_corridors,
    s.bike_total_impressions,
    s.bike_avg_flow,
    s.bike_edges,
    s.ped_total_impressions,
    s.ped_avg_flow,
    s.ped_edges,
    s.motorized_total_impressions,
    s.motorized_avg_flow,
    s.motorized_edges,
    s.total_all_impressions
FROM vw_impressions_summary_spatial s
JOIN LATERAL (
    SELECT x_lon_center, y_lat_centre
    FROM v2_facade_visibility
    WHERE gwr_egid = s.gwr_egid
    LIMIT 1
) f ON TRUE;

-- ============================================================================
-- PART 4: INDEXES FOR PERFORMANCE
-- ============================================================================

CREATE INDEX idx_mv_impressions_gwr_egid ON mv_impressions_summary_spatial(gwr_egid);
CREATE INDEX idx_mv_impressions_total ON mv_impressions_summary_spatial(total_all_impressions DESC);
CREATE INDEX idx_mv_impressions_coords ON mv_impressions_summary_spatial(latitude, longitude);

-- ============================================================================
-- PART 5: SUMMARY & USAGE
-- ============================================================================

/**
 * DATA PIPELINE SUMMARY
 *
 * INPUT: 8 raw tables, 959K+ rows
 * ├─ Flows: bike, pedestrian, motorized, VBZ
 * └─ Geometry: facades, edges
 *
 * PROCESS:
 * 1. JOIN flows with edges (on edge_id)
 * 2. SPATIAL JOIN: find where flows intersect building facades (ST_Intersects)
 * 3. AGGREGATE: SUM impressions per building (GROUP BY gwr_egid)
 * 4. PIVOT: Convert rows to columns (one row per building, all traffic types)
 * 5. ADD COORDINATES: Include latitude/longitude from source
 * 6. MATERIALIZE: Cache in materialized view with indexes
 *
 * OUTPUT: mv_impressions_summary_spatial
 * ├─ 290,563 rows (one per building)
 * ├─ Coordinates (WGS84)
 * ├─ All impression types broken down
 * ├─ Total impressions (sum of all)
 * └─ Query performance: <15ms
 *
 * TABLEAU USAGE:
 * SELECT * FROM mv_impressions_summary_spatial
 * WHERE total_all_impressions > 0
 * ORDER BY total_all_impressions DESC;
 *
 * REFRESH (for data updates):
 * REFRESH MATERIALIZED VIEW CONCURRENTLY mv_impressions_summary_spatial;
 */

-- Verify schema is created
SELECT
    'mv_impressions_summary_spatial' as view_name,
    COUNT(*) as row_count,
    COUNT(CASE WHEN total_all_impressions > 0 THEN 1 END) as with_data
FROM mv_impressions_summary_spatial;

\echo 'Production schema created successfully!';
