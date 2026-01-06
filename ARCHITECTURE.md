# FacADe System Architecture

## Overview

FacADe is built on a **medallion lakehouse architecture** that separates concerns across four logical layers:

1. **Data Lake** (Bronze → Silver → Gold): Raw ingestion → Standardization → Analytical modeling
2. **Data Warehouse**: PostgreSQL with PostGIS for spatial analytics
3. **Visualization**: Tableau dashboards for end-user insights
4. **Orchestration**: AWS Lambda + EventBridge for scheduling and coordination

This design prioritizes:
- **Separation of concerns**: Heavy modeling in the lake, serving in the warehouse
- **Data lineage & traceability**: Versioned models and watermark tracking
- **Scalability**: Independent scaling of ingestion, modeling, and analytics
- **Reproducibility**: All transformations documented and versioned

---

## Data Lake Architecture

### Medallion Structure

The data lake uses a three-layer medallion pattern to organize data by quality and maturity:

```
Bronze Layer (Raw)
    ↓ Normalize & Standardize
Silver Layer (Standardized)
    ↓ Model & Enrich
Gold Layer (Business-Ready)
```

#### Bronze Layer
**Purpose:** Preserve raw data exactly as received from sources

**Characteristics:**
- Schema-on-read approach
- Raw files stored as-is (CSV, GeoPackage)
- No transformations applied
- Complete historical record maintained
- Updated incrementally when source data changes

**Storage:**
- Amazon S3: `s3://facade-project-dev/bronze/`
- Partitioned by: `domain/year/month/`
- Example paths:
  - `bronze/traffic/2025/01/motorized_traffic_counts.parquet`
  - `bronze/pedestrian/2025/01/ped_counts.parquet`
  - `bronze/vbz/2025/01/reisende.parquet`

#### Silver Layer
**Purpose:** Standardize and normalize data for analytical processing

**Transformations Applied:**
- Column naming: snake_case convention
- Timestamp standardization: ISO 8601 format
- Data type enforcement: integer, float, text, timestamp
- Geometry conversion: WKT/WKB → GeoParquet format
- Null handling: Explicit NA values
- CSV → Parquet conversion for efficiency

**Characteristics:**
- Schema enforced and documented
- Data types consistent across datasets
- Temporal fields aligned to hourly buckets
- Spatial data in standard formats
- Partitioned by year/month for efficient querying

**Storage:**
- Amazon S3: `s3://facade-project-dev/silver/`
- Partitioned by: `domain/year/month/`
- Format: Parquet (tabular), GeoParquet (spatial)

#### Gold Layer
**Purpose:** Produce business-ready, modeled outputs for analytics

**Characteristics:**
- Versioned model outputs
- Complete spatial coverage (propagation fills gaps)
- Temporal aggregations at multiple granularities
- Clean, enriched features ready for BI
- Immutable outputs (no in-place updates)

**Datasets in Gold:**
1. **facade_visibility** - Computed visibility polygons for all facades
2. **motorized_traffic_flows** - Propagated vehicle flows on street network
3. **pedestrian_flows** - Propagated pedestrian flows on path network
4. **bicycle_flows** - Propagated bicycle flows on cycling network
5. **vbz_flows_hourly** - Public transport passenger flows on segments

**Storage:**
- Amazon S3: `s3://facade-project-dev/gold/`
- Partitioned by: `dataset/version/year/month/`
- Format: GeoParquet (with geometry) + Parquet (geometry-free metrics)
- Example: `gold/motorized_traffic/v2_2025/2025/01/flows.parquet`

---

## Data Ingestion Pipeline

### Architecture

```
┌─────────────────────────────────────────────────┐
│          AWS EventBridge (Weekly Trigger)       │
└──────────────────────────┬──────────────────────┘
                           │
         ┌─────────────────┼─────────────────┬──────────────────┐
         │                 │                 │                  │
    ┌────▼────┐        ┌───▼──────┐    ┌────▼────┐        ┌────▼──────┐
    │ Traffic │        │ Pedestrian│    │   VBZ   │        │  Facades  │
    │ Lambda  │        │ Velo Lambdas   │ Lambda  │        │  (Manual) │
    └────┬────┘        └───┬──────┘    └────┬────┘        └────┬──────┘
         │                 │                 │                  │
    Zürich OGD API    Zürich OGD API    Zürich OGD API     GeoCat API
         │                 │                 │                  │
         └─────────────────┼─────────────────┼──────────────────┘
                           │
         ┌─────────────────▼─────────────────┐
         │      Amazon S3 - Bronze Layer     │
         │   (Raw data, partitioned)         │
         └──────────────────┬────────────────┘
                           │
         ┌─────────────────▼─────────────────┐
         │  Normalization Lambdas (Inline)   │
         │  - Standardize schemas            │
         │  - Convert to Parquet             │
         │  - Enforce timestamps             │
         └──────────────────┬────────────────┘
                           │
         ┌─────────────────▼─────────────────┐
         │      Amazon S3 - Silver Layer     │
         │   (Standardized, normalized)      │
         └─────────────────────────────────┘
```

### Ingestion Schedule

**Frequency:** Weekly (Saturday 2:00 AM UTC)

| Source | Schedule | Mechanism | Latency |
|--------|----------|-----------|---------|
| Motorized Traffic | Weekly | CKAN API → Lambda | 1 week |
| Pedestrian/Bicycle | Weekly | CKAN API → Lambda | 1 week |
| VBZ Passenger Data | Weekly | Download → Lambda | 1 month (reported monthly) |
| Facade Geometries | Manual | Download → Direct S3 | As updated |
| OSM Network | On-demand | Fetched in containers | Current |

### Data Quality Controls

**Watermark Management:**
- CKAN resources: Track resource timestamp
- VBZ files: Track file hash (MD5)
- Bronze overwrites: Only if timestamp/hash differs
- Silver recompute: Only affected partitions

**Schema Validation:**
- Expected columns checked on load
- Data types enforced
- Null handling rules applied
- Missing columns logged as warnings

**Audit Tracking:**
- DynamoDB audit table: Every ingestion run
- CloudWatch logs: Detailed execution logs
- Fields logged: timestamp, dataset, rows_processed, status

---

## Data Transformation Pipeline

### Two-Stage Transformation

#### Stage 1: Normalization (Bronze → Silver)
**Execution:** Within Lambda ingestion functions
**Duration:** < 1 minute per dataset
**Tools:** pandas, geopandas, pyarrow

**Transformations:**
```python
# Column naming
df.columns = df.columns.str.lower().str.replace(' ', '_')

# Timestamp standardization
df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)

# Geometry handling
gdf['geometry'] = gpd.GeoSeries.from_wkt(gdf['wkt_geometry'])
gdf = gdf.set_crs('EPSG:2056')  # Swiss coordinates

# CSV → Parquet
df.to_parquet(s3_path, compression='snappy', index=False)
```

#### Stage 2: Geospatial Modeling (Silver → Gold)
**Execution:** AWS ECS Fargate containers
**Duration:** 5-30 minutes per model (parallel execution)
**Tools:** geopandas, GDAL, NetworkX, Shapely, pygeos

**Models Executed:**

1. **Facade Visibility Model**
   - Input: Facade geometries (Silver)
   - Process: Buffer facade lines by distance, project by orientation
   - Output: Visibility polygons (Gold)
   - Files: `facade_visibility_v2.gpkg`

2. **Motorized Traffic Flow Model**
   - Input: Traffic counts (Silver) + OSM network
   - Process: Flow propagation with mass-balance constraints
   - Output: Flows per edge and hour (Gold)
   - Files: `motorized_traffic_edges.gpkg`, `motorized_traffic_flows.parquet`

3. **Pedestrian & Bicycle Flow Model**
   - Input: Pedestrian/bicycle counts (Silver) + OSM walking/cycling networks
   - Process: Mode-specific flow propagation
   - Output: Flows per edge and hour (Gold)
   - Files: `ped_bike_edges.gpkg`, `ped_flows.parquet`, `bike_flows.parquet`

4. **VBZ Public Transport Model**
   - Input: VBZ fact tables (Silver) + GTFS
   - Process: Georeference stops, compute routes, aggregate by segment/hour
   - Output: Flows per segment and hour (Gold)
   - Files: `vbz_flows_hourly.gpkg`

### Model Orchestration

Models run in parallel via AWS Step Functions:

```json
{
  "Parallel": [
    {"Resource": "arn:aws:ecs:...facade-model"},
    {"Resource": "arn:aws:ecs:...traffic-model"},
    {"Resource": "arn:aws:ecs:...ped-bike-model"},
    {"Resource": "arn:aws:ecs:...vbz-model"}
  ]
}
```

**Advantages:**
- Independent scaling per model
- Fault isolation (one model failure doesn't block others)
- Easy to version models separately
- Simple deployment (Docker image per model)

---

## Data Warehouse Architecture

### Schema Design

The warehouse uses a **star schema adapted for spatial data**:

```
                    ┌─────────────────────────┐
                    │   FACT: Impressions     │
                    │  (Facade × Mode × Hour) │
                    └────────────┬────────────┘
                                 │
         ┌───────────────────────┼───────────────────────┐
         │                       │                       │
    ┌────▼──────┐          ┌─────▼──────┐          ┌────▼────┐
    │ DIM:      │          │ DIM: Time  │          │ DIM:    │
    │ Facades   │          │   Hourly   │          │ Modes   │
    │ & Buildings          │            │          │ (Ped,   │
    │ (EGID)    │          │            │          │  Bike,  │
    └───────────┘          └────────────┘          │  Auto,  │
                                                    │  Transit)
                                                    └─────────┘
```

### Core Tables

#### v2_facade_visibility (Geometry)
```sql
CREATE TABLE v2_facade_visibility (
    objectid INTEGER PRIMARY KEY,
    egid VARCHAR(20),           -- Building identifier
    geometry POLYGON,            -- Visibility polygon (EPSG:2056)
    x_lon_center FLOAT,         -- Centroid for web display
    y_lat_center FLOAT,
    beziechnung VARCHAR(255),   -- Building label
    created_at TIMESTAMP
);

CREATE INDEX idx_facade_visibility_geom
    ON v2_facade_visibility USING GIST(geometry);
```

#### Flow Tables (Time-Series Fact)
```sql
CREATE TABLE v2_motorized_traffic_flows (
    edge_id INTEGER,            -- Road segment identifier
    hour INTEGER,               -- Hour of day (0-23)
    flow FLOAT,                 -- Vehicle flow count
    flow_norm FLOAT,            -- Normalized flow
    created_at TIMESTAMP,
    PRIMARY KEY (edge_id, hour)
);

CREATE INDEX idx_traffic_flows_edge ON v2_motorized_traffic_flows(edge_id);
```

#### Edge Tables (Geometry Reference)
```sql
CREATE TABLE v2_motorized_traffic_edges (
    edge_id INTEGER PRIMARY KEY,
    geometry LINESTRING,        -- Road segment (EPSG:2056)
    name VARCHAR(255),
    length FLOAT,
    bearing FLOAT
);

CREATE INDEX idx_traffic_edges_geom
    ON v2_motorized_traffic_edges USING GIST(geometry);
```

### Spatial Joins

**Computing Facade Impressions:**

```sql
-- Join facades with flows within visibility area
SELECT
    f.objectid,
    f.egid,
    SUM(flows.flow) as total_flow,
    flows.mode,
    flows.hour
FROM v2_facade_visibility f
JOIN (
    -- Union of all flows
    SELECT edge_id, flow, 'motorized' as mode, hour
    FROM v2_motorized_traffic_flows
    UNION ALL
    SELECT edge_id, flow, 'pedestrian' as mode, hour
    FROM v2_ped_flows
) flows
ON ST_Intersects(f.geometry, edge_geom)
GROUP BY f.objectid, f.egid, flows.mode, flows.hour;
```

### Performance Optimization

**Materialized View (Cache):**
```sql
CREATE MATERIALIZED VIEW facade_impressions_summary AS
SELECT
    objectid,
    egid,
    SUM(CASE WHEN mode = 'motorized' THEN flow ELSE 0 END) as motorized_total,
    SUM(CASE WHEN mode = 'pedestrian' THEN flow ELSE 0 END) as ped_total,
    SUM(CASE WHEN mode = 'bicycle' THEN flow ELSE 0 END) as bike_total,
    SUM(CASE WHEN mode = 'transit' THEN flow ELSE 0 END) as transit_total,
    SUM(flow) as total_impressions
FROM (computed impressions table)
GROUP BY objectid, egid;

CREATE INDEX idx_impressions_egid ON facade_impressions_summary(egid);
```

**Spatial Indexes:**
- GIST indexes on all geometry columns
- Partitioning by geographic zones (optional, for very large deployments)

---

## Data Loading Strategy

### Initial Load (Setup)

```bash
# 1. Load geometry reference tables
psql -h RDS_ENDPOINT -U postgres -d facade_db \
  -c "COPY v2_motorized_traffic_edges FROM STDIN;" < edges.csv

# 2. Load facade geometries
ogr2ogr -f PostgreSQL PG:"dbname=facade_db" \
  facade_visibility.gpkg v2_facade_visibility

# 3. Create spatial indexes
psql -h RDS_ENDPOINT -U postgres -d facade_db \
  -f create_spatial_indexes.sql
```

### Incremental Load (Weekly)

```bash
# Extract from Gold layer Parquet
aws s3 cp s3://facade-project-dev/gold/motorized_traffic/v2_2025/flows.parquet .

# Load via Glue job with JDBC
python glue_loader_jdbc.py \
  --input flows.parquet \
  --table v2_motorized_traffic_flows \
  --mode overwrite_partition \
  --partition_cols year,month
```

---

## Scalability & Performance

### Ingestion Scaling

| Component | Capacity | Scaling Method |
|-----------|----------|----------------|
| Lambda (ingestion) | 1000 concurrent | Auto-scaling |
| S3 (Bronze/Silver) | Unlimited | Partitioning strategy |
| S3 (data rate) | 3,500 PUT/s | Batch writes, back-pressure |

### Transformation Scaling

| Component | Configuration | Notes |
|-----------|---------------|-------|
| ECS Task CPU | 2 vCPU | Sufficient for city-scale models |
| ECS Task Memory | 8 GB | GIS operations memory-intensive |
| Task Concurrency | 1 per model | Sequential by design (dependency) |
| Execution Time | 10-30 min | Acceptable for weekly batch |

### Warehouse Scaling

| Operation | Current | Optimized For |
|-----------|---------|---------------|
| Facade count | ~150K | Millions (via partitioning) |
| Flow edges | ~50K | Millions (via shard keys) |
| Hourly records | ~50M/year | Billions (via time partitioning) |
| Query latency | < 500ms | Materialized views cached |

---

## Data Quality Measures

### Validation Layers

1. **Ingestion Validation**
   - Schema conformance checks
   - Required columns verified
   - Data types enforced
   - Null handling rules applied

2. **Transformation Validation**
   - Geometry validity checks (via GDAL)
   - Network connectivity verification
   - Mass-balance constraints (flow conservation)
   - Temporal continuity checks

3. **Warehouse Validation**
   - Referential integrity (foreign keys)
   - Spatial index health checks
   - Partition verification
   - Row count reconciliation

### Quality Metrics Tracked

- **Completeness**: % of expected records received
- **Validity**: % of geometries that pass GIST tests
- **Consistency**: Flow conservation across aggregations
- **Timeliness**: Lag between source update and Gold availability
- **Accuracy**: Spot-check against source systems

### Data Issues Handling

**Common Issues & Resolution:**

| Issue | Detection | Resolution |
|-------|-----------|-----------|
| Missing counts at station | Schema validation | Log & skip; propagation models account for sparsity |
| Invalid geometries | GDAL on load | Buffer/simplify or exclude |
| Duplicate records | Hash-based dedup | Remove based on timestamp |
| Schema drift | Column name mismatch | Schema evolution in Silver |
| Network gaps | Disconnected components | Flag but allow (edge case) |

---

## Versioning Strategy

### Model Versioning

Each model output includes version tag:
```
s3://facade-project-dev/gold/motorized_traffic/v2_2025/
  2025/01/flows.parquet
```

**Version Components:**
- `v2` = Model version (e.g., v1→v2 when major algorithm changes)
- `2025` = Data year
- `01` = Data month

### Schema Versioning

Models document schema in Gold metadata:
```python
# model_traffic_flows.py
SCHEMA_VERSION = "2.0"
OUTPUT_SCHEMA = {
    'edge_id': 'int64',
    'hour': 'int64',
    'flow': 'float64',
    'flow_norm': 'float64',
    'year': 'int64',
    'month': 'int64'
}
```

### Data Lineage

Every Gold dataset includes metadata:
```json
{
  "model": "motorized_traffic_flow_propagation",
  "model_version": "2.0",
  "source_data": "silver/traffic/2025/01",
  "source_hash": "abc123def456",
  "execution_date": "2025-01-04T02:15:00Z",
  "parameters": {
    "max_distance": 5000,
    "decay_exponent": 2.0
  },
  "records_processed": 50000,
  "execution_duration_seconds": 180
}
```

---

## Disaster Recovery & Backup

### Backup Strategy

| Layer | Backup | Frequency | Retention |
|-------|--------|-----------|-----------|
| Bronze | S3 versioning | Continuous | 90 days |
| Silver | S3 replication | After ingest | 30 days |
| Gold | S3 cross-region | Weekly | 1 year |
| RDS | Automated snapshots | Daily | 7 days |

### Recovery Procedures

**Scenario: Model corruption in Gold**
```bash
# 1. Identify last good version
aws s3api list-object-versions \
  --bucket facade-project-dev \
  --prefix gold/motorized_traffic/

# 2. Restore from version
aws s3api get-object \
  --bucket facade-project-dev \
  --key gold/motorized_traffic/v2_2025/flows.parquet \
  --version-id abc123 \
  flows-restored.parquet

# 3. Re-sync to warehouse
python glue_loader_jdbc.py --input flows-restored.parquet --mode overwrite
```

---

## Monitoring & Logging

### Key Metrics

```
Ingestion:
- Lambda invocation duration (target: < 5 min)
- Bytes ingested per week (trend analysis)
- Error rate (target: < 0.1%)

Transformation:
- ECS task duration per model (trend analysis)
- S3 PUT/GET latency (target: < 100ms p99)
- CPU/memory utilization (target: 60-80%)

Warehouse:
- Query latency (target: < 500ms p95)
- Index health (EXPLAIN ANALYZE)
- Table sizes (trend analysis)
```

### Logs

- **CloudWatch**: Lambda execution logs, error stack traces
- **CloudTrail**: S3 access logs, data governance
- **Application Logs**: Model execution logs in containerized format

---

## Cost Optimization

### Current Estimates (Monthly)

| Service | Usage | Cost |
|---------|-------|------|
| Lambda | 1000 invocations/month | ~$5 |
| S3 (storage) | ~50 GB | ~$1 |
| S3 (requests) | ~100K requests | ~$0.50 |
| ECS Fargate | 4 tasks × 30 min/week | ~$40 |
| RDS (Small) | t3.small instance | ~$40 |
| **Total** | | **~$90/month** |

### Optimization Opportunities

- **Spot Instances** for Fargate tasks (30% cost reduction)
- **S3 Intelligent-Tiering** for historical Gold data
- **Aurora Serverless** for RDS (pay-per-query in off-hours)
- **Data compression** (Parquet snappy is already applied)

---

## Future Enhancements

### Planned Improvements

1. **Probabilistic Models**
   - Monte Carlo simulations for uncertainty quantification
   - Confidence intervals on flow estimates

2. **Machine Learning Integration**
   - Neural network for flow prediction
   - Computer vision on street imagery for facade quality

3. **Real-time Streaming**
   - Kafka for continuous event ingestion
   - Real-time dashboard updates

4. **Advanced Analytics**
   - Scenario modeling (what-if analysis)
   - Price optimization models
   - Demand forecasting

---

## References

- AWS Architecture: https://docs.aws.amazon.com/prescriptive-guidance/latest/patterns/medallion-lakehouse-architecture/
- PostGIS Documentation: https://postgis.net/documentation/
- GDAL/OGR: https://gdal.org/
- NetworkX: https://networkx.org/
