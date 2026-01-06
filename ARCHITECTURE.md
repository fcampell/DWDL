# FacADe System Architecture

## Overview

FacADe uses a **medallion lakehouse architecture** that separates data ingestion, transformation, and analytics into distinct layers. Heavy computation happens in the data lake; the warehouse is optimized for querying.

---

## System Layers

### 1. Data Lake (Bronze → Silver → Gold)

**Bronze Layer:** Raw data from Zürich OGD, stored unchanged
- Motorized traffic counts
- Pedestrian & bicycle counts
- VBZ passenger data
- Building facade geometries

**Silver Layer:** Standardized, normalized datasets
- Column naming conventions applied
- Timestamps aligned to hourly buckets
- Geometry converted to standard formats (GeoParquet)
- All data types enforced

**Gold Layer:** Business-ready, versioned outputs
- **Facade Visibility**: Computed visibility polygons for all buildings
- **Flow Networks**: Motorized, pedestrian, bicycle, and transit flows propagated onto road/path networks
- Models run in parallel, each tagged with version for reproducibility

### 2. Data Warehouse (PostgreSQL + PostGIS)

**Purpose:** Efficient querying and dashboard serving

**Design:**
- Geometry stored in Swiss coordinate system (EPSG:2056)
- Spatial indexes (GIST) for fast geometric operations
- Pre-computed materialized views for dashboard performance
- Schema includes reference tables (facades, edges) and fact tables (flows, impressions)

**Key Tables:**
- `v2_facade_visibility`: Building facades and visibility areas
- Flow tables: One row per edge/segment per hour across all modes
- Materialized views: Aggregated impressions by building

### 3. Transformation Pipeline

**Stage 1: Normalization (Lambda)**
- Bronze → Silver: Lightweight schema standardization
- Column naming, timestamp alignment, format conversion
- Runs as part of ingestion

**Stage 2: Geospatial Modeling (ECS Fargate)**
- Silver → Gold: Compute-intensive transformations
- 4 models run in parallel:
  1. Facade visibility polygon generation
  2. Motorized traffic flow propagation
  3. Pedestrian & bicycle flow modeling
  4. Public transport (VBZ) integration
- Each produces versioned outputs

---

## Technology Stack

| Component | Technology |
|-----------|-----------|
| **Storage** | Amazon S3 (Bronze/Silver/Gold) |
| **Metadata** | AWS Glue Data Catalog |
| **Ingestion** | AWS Lambda + EventBridge |
| **Transformation** | AWS ECS Fargate (Python containers) |
| **Database** | PostgreSQL 12+ with PostGIS |
| **Visualization** | Tableau Public |
| **Geospatial** | Geopandas, NetworkX, GDAL/PROJ |

---

## Data Flow

```
Zürich OGD
    ↓
AWS Lambda (Weekly Batch)
    ↓
S3 Bronze (Raw) → S3 Silver (Standardized)
    ↓
ECS Fargate (4 models in parallel)
    ↓
S3 Gold (Versioned outputs)
    ↓
PostgreSQL + PostGIS (Data Warehouse)
    ↓
Tableau (Dashboards)
```

---

## Key Design Decisions

### Why Separate Lake and Warehouse?

- **Lake**: Handles complex geospatial transformations (geopandas, GDAL, NetworkX) in containerized environment
- **Warehouse**: Optimized for fast queries and materialized views for BI tools
- **Result**: Clean separation of concerns, easier to scale and debug

### Why Medallion Architecture?

- **Bronze**: Full audit trail of raw data (schema-on-read)
- **Silver**: Consistent schema enables reusable downstream logic
- **Gold**: Modeled outputs are immutable and versioned
- **Result**: Easy to trace errors, replay historical data, update models independently

### Why Parallel ECS Tasks?

- Each model is independent Python container
- Can fail/retry without blocking others
- Easy to deploy model updates
- Simple Docker-based version management

### Why PostGIS?

- Native geometry types eliminate serialization overhead
- Spatial indexes (GIST) optimize intersection queries
- SQL-based spatial joins integrate cleanly with traditional BI tools
- Cost-effective compared to specialized spatial databases

---

## Data Quality & Validation

Quality is ensured through:

1. **Architectural safeguards**: Medallion layers separate raw, standardized, and modeled data
2. **Schema validation**: Column names, types enforced at Silver layer
3. **Watermark tracking**: Prevents silent data loss in ingestion
4. **Spatial validation**: Geometry checks catch invalid features
5. **Model execution logs**: Traceability of data lineage

## Scalability

**Current Capacity:**
- ~415K facades processed weekly
- ~50K-200K edges per mobility mode
- ~1M+ hourly flow records annually

**Growth Path:**
- Increased facade count: Add S3 partitioning
- More detailed flows: Expand temporal granularity
- Additional models: Deploy as new ECS tasks
- Warehouse scaling: Upgrade RDS instance size

---

## Deployment Environments

**Development:** Local setup with Docker containers and local PostgreSQL

**Production:** Full AWS deployment with:
- S3 buckets with encryption and versioning
- RDS Multi-AZ for high availability
- EventBridge scheduled jobs
- CloudWatch monitoring and alarms

See [SETUP.md](SETUP.md) for deployment instructions.

---

## Future Enhancements

- **Uncertainty quantification**: Monte Carlo simulations for flow estimates
- **Computer vision**: Estimate effective advertising area from street imagery
- **Real-time streaming**: Kafka integration for live updates
- **Machine learning**: Neural networks for flow prediction

---

## References

- [README.md](README.md) - Project overview
- [SETUP.md](SETUP.md) - Deployment guide
- Project Report - Detailed methodology and results
- AWS Docs: Medallion architecture pattern
- PostGIS Docs: Spatial operations and indexing
