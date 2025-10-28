# GIS Data Lake + Low-Latency PostGIS (Option B)

This architecture keeps **raw GIS files** in an S3 data lake (Bronze → Silver → Gold), while a **low-latency PostGIS** layer serves per-request operations (e.g., EGID lookup and `ST_Buffer`). It’s simple to operate and fast for single-building queries.

---

## High-Level Diagram

```mermaid
flowchart LR
  subgraph Ingestion & Lake
    U1[Raw .gpkg (Facades, Municipalities)] --> S3B[(S3 Bronze)]
    S3B --> GLUE[Glue ETL Job]
    GLUE --> S3S[(S3 Silver GeoParquet 2056)]
    S3S -. registered .-> CATALOG[Glue Data Catalog]
    CATALOG -. access .-> LF[Lake Formation]
  end

  subgraph Serving & Compute
    S3S --> SYNC[Lambda Sync Facades -> Aurora]
    SYNC --> AUR[(Aurora PostgreSQL + PostGIS)]
    API[API Gateway] --> LBD[Lambda Resolver]
    LBD --> AUR
    AUR --> LBD
    LBD --> APP[Dashboard (CloudFront + S3 or Streamlit on ECS)]
  end

  subgraph Persist & Analytics
    LBD --> WRG[Lambda Writer: Gold]
    WRG --> S3G[(S3 Gold GeoParquet)]
    S3G -. query .-> ATH[Athena/Trino Geospatial]
    S3G -. optional .-> VT[Vector Tiles Builder (Batch/ECS)]
    VT --> CDN[CloudFront Tile CDN]
    CDN --> APP
  end

  subgraph Platform
    SEC[Secrets Manager]
    MON[CloudWatch + X-Ray]
    IAM[IAM Roles/Policies]
    VPC[VPC + Subnets + SGs]
  end

```

# GIS Data Lake + Low-Latency PostGIS (Option B)

This architecture keeps **raw GIS files** in an S3 data lake (Bronze → Silver → Gold), while a **low-latency PostGIS** layer serves per-request operations (e.g., EGID lookup and `ST_Buffer`). It’s simple to operate and fast for single-building queries.

---

## Medallion Layout (S3 Buckets/Folders)

- **Bronze** (`s3://<bucket>/bronze/`): immutable raw files (e.g., `.gpkg` for facades and municipalities/Kreise).
- **Silver** (`s3://<bucket>/silver/`): cleaned, standardized **GeoParquet** (CRS **EPSG:2056** so distances are in meters).
- **Gold** (`s3://<bucket>/gold/`): analytics-ready outputs (e.g., per-facade buffers, views, KPIs), also GeoParquet.

**Why:** S3 is cheap, versioned, and durable. Parquet gives column pruning and works with Athena/Trino.

---

## Step-by-Step Pipeline (with AWS Services)

### 1) Raw Data Landing (Bronze)
- **What:** Store the original `.gpkg` files (facades; municipalities/Kreise).
- **AWS Service:** **Amazon S3**
- **How:** Upload via console/CLI/CI. Enable **S3 Versioning** and a **Lifecycle policy** (e.g., transition old versions to Glacier).

### 2) Catalog & Governance
- **What:** Make lake data discoverable and govern access.
- **AWS Services:** **AWS Glue Data Catalog** + **AWS Lake Formation**
- **How:** Create databases/tables for Bronze/Silver/Gold. Use **Lake Formation** to grant fine-grained access to teams and jobs.

### 3) Standardize to GeoParquet (Silver)
- **What:** Convert `.gpkg` → **GeoParquet**; unify CRS to **EPSG:2056**; clean field names; add `ingest_version`.
- **AWS Service:** **AWS Glue (Spark ETL)** (or **AWS Batch/ECS** with GeoPandas if you prefer Python)
- **How:** Glue job reads Bronze, reprojects to 2056, writes partitioned **GeoParquet** to Silver. Register tables in the Glue Catalog.

### 4) (One-time & Scheduled) Sync to PostGIS for Serving
- **What:** Load only the tables needed for **low-latency** queries (e.g., `facades`, `municipalities`) into PostGIS.
- **AWS Services:**  
  - **Amazon Aurora PostgreSQL** (with **PostGIS** extension)  
  - **AWS Lambda** (small sync function)  
- **How:** A Lambda job (triggered after Silver writes or on a schedule) reads Silver GeoParquet (via S3/fsspec) and **upserts** rows into Aurora (`COPY` or `pgcopy` for speed). Keep an `ingest_version` column for freshness.

### 5) Public API Entry Point
- **What:** Expose a simple HTTPS endpoint for the dashboard.
- **AWS Service:** **Amazon API Gateway** (REST or HTTP API)
- **How:** Define an endpoint `/facades/{egid}/buffer?m=2.0`. It invokes a **Lambda Resolver**.

### 6) Per-Request Resolver (Option B, Low Latency)
- **What:** Resolve EGID → ENIDs and compute **buffer(s)** on the fly; cache results for repeat calls.
- **AWS Service:** **AWS Lambda** (Resolver) + **Aurora PostgreSQL (PostGIS)**
- **How (logic):**
  1. **Check cache** table in Aurora: `(egid, enid, buffer_m, ingest_version)` as a composite key.
  2. **Cache miss → compute**:
     - Query `facades` by `egid` (and optional spatial filter by municipality).
     - Run `ST_Buffer(geom, :buffer_m)` (CRS=2056 ensures meters).
     - **UPSERT** results into `facade_buffers_cache`.
  3. **Return** geometries (GeoJSON) + metadata.
- **Notes:** Cold start is tiny; PostGIS returns in tens of ms for single EGID.

### 7) Persist Results to Gold (for Analytics & Reuse)
- **What:** Keep a durable copy of computed buffers for analytics (and future batch joins).
- **AWS Service:** **AWS Lambda** (Writer) → **S3 Gold**
- **How:** The Resolver publishes a small message (or directly calls a Writer Lambda) to write a **Gold** GeoParquet partition:
  - Path suggestion: `gold/facade_buffers/ingest_version=<yyyymmdd>/buffer_m=2/egid=<id>/part-....parquet`

### 8) Optional: Vector Tiles for Map Rendering
- **What:** Serve map layers efficiently.
- **AWS Services:** **AWS Batch/ECS** (tile builder such as tippecanoe/martin) + **Amazon CloudFront**
- **How:** Build **MBTiles/PMTiles** or serve vector tiles, store on S3, front by CloudFront CDN. The dashboard fetches tiles for smooth panning/zooming.

### 9) Dashboard Hosting
- **What:** A simple web app to submit EGID and view results.
- **AWS Services:**  
  - **Static:** **S3 (website) + CloudFront** (React/MapLibre)  
  - **or App:** **Amazon ECS Fargate** (e.g., Streamlit)
- **How:** The app calls the API Gateway; renders buffers and stats on a map.

### 10) Security, Networking, and Ops
- **Secrets:** **AWS Secrets Manager** for Aurora credentials and API keys.  
- **Networking:** Put **Aurora in a VPC**; use **Private Subnets** and **Security Groups** to allow only Lambda access.  
- **IAM:** Least privilege roles for Lambda/Glue/Athena/S3.  
- **Monitoring:** **CloudWatch Logs/Metrics**, **X-Ray** for tracing, **Enhanced Monitoring** for Aurora.  
- **Backups:** **Aurora automated backups**; S3 is versioned.  
- **Cost Controls:** Lifecycle rules on S3; scale down Aurora (Serverless v2 is convenient for dev).

---

## PostGIS Schema (Serving Layer)

```sql
-- Base tables
CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE IF NOT EXISTS facades (
  enid bigint PRIMARY KEY,
  egid bigint NOT NULL,
  ingest_version text NOT NULL,
  geom geometry(Polygon, 2056) NOT NULL
);

CREATE INDEX ON facades USING GIST (geom);
CREATE INDEX ON facades (egid);

-- Read-through cache
CREATE TABLE IF NOT EXISTS facade_buffers_cache (
  egid bigint,
  enid bigint,
  buffer_m integer,
  ingest_version text,
  geom geometry(Polygon, 2056),
  PRIMARY KEY (egid, enid, buffer_m, ingest_version)
);

CREATE INDEX ON facade_buffers_cache USING GIST (geom);
```

## Typical Request Flow (Example)

- Dashboard calls GET /facades/123456/buffer?m=2.0.
- API Gateway → Lambda Resolver.
- Lambda checks Aurora cache for (egid=123456, m=2.0, ingest_version=20250915).
- Miss → SELECT ... FROM facades WHERE egid=123456 then ST_Buffer.
- UPSERT into facade_buffers_cache; return GeoJSON to client.
- In parallel, Writer Lambda stores the result to S3 Gold (GeoParquet) for analytics.

**Why this Works Well**

- Fast per-EGID queries: PostGIS is optimized for small, targeted spatial ops.
- Lake remains the source of truth: All heavy analytics and historical versions live in S3.
- Reusability: Cache and Gold ensure “compute once, reuse many times.”
- Low friction AWS: Only a few managed services (S3, Glue, Lake Formation, Aurora, Lambda, API Gateway).

**Getting Started (Minimal)**
1. Create S3 bucket with bronze/, silver/, gold/. Enable versioning.
2. Upload .gpkg to bronze/.
3. Glue ETL job: convert to GeoParquet (2056) into silver/ and register tables.
4. Provision Aurora Postgres and enable PostGIS.
5. Lambda Sync: load facades (and municipalities) from Silver → Aurora.
6. API Gateway + Lambda Resolver with VPC access to Aurora.
7. Dashboard (S3+CloudFront) calling the API.