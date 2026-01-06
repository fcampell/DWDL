# FacADe

FacADe is a data engineering platform that quantifies facade advertising visibility and monetization potential across Zürich by combining mobility data (motorized traffic, pedestrian flows, cyclists, public transport) with geospatial modeling.

## Table of Contents
1. [Goals](#goals)
2. [File Descriptions](#file-descriptions)
3. [How to Run the Code](#how-to-run-the-code)
4. [Architecture](#architecture)
5. [Authors](#authors)

---

## Goals

The project aims to:
- Estimate facade visibility across all buildings in Zürich
- Integrate multiple mobility data sources (vehicles, pedestrians, cyclists, transit)
- Provide visibility metrics for marketing agencies and property owners
- Estimate monetization potential through revenue modeling
- Demonstrate technical feasibility of city-scale geospatial analysis using AWS cloud infrastructure

---

## File Descriptions

### Root Directory
- **README.md** - This file. Complete project documentation.
- **.gitignore** - Git configuration excluding data files, notebooks, caches, and IDE files.

### `AWS/` Directory
Cloud infrastructure and pipeline code for AWS deployment.

#### `AWS/Lambda/`
Batch ingestion functions triggered weekly by EventBridge to fetch fresh data from Zürich OGD API.

- **lambda-traffic-ingestion.py** - Main Lambda handler function `lambda_handler(event, context)`. Fetches motorized traffic data from Zürich OGD CKAN API for specified month/year, normalizes to consistent format, and uploads to S3 Bronze bucket.
- **facade-pedestrian-velo/** - Module for pedestrian and bicycle count data ingestion from Zürich OGD.
- **facade-vbz-reisende-batch/** - Module for VBZ (Zürich transit authority) passenger data ingestion.
- **facade-vbz-small-tables/** - Module for VBZ reference tables and metadata.
- **lambda-vbz-ingestion/** - Alternative VBZ data ingestion implementation.

#### `AWS/containers/`
ECS Fargate containerized models for geospatial transformations. Four independent models run in parallel, each processing Silver layer data to produce Gold layer outputs. Each model can also be tested locally with provided shell scripts.

**Facade Visibility Model:**
- **facade/run_visibility_model.py** - Generates visibility polygons for building facades. Creates quad polygons representing the spatial region from which each facade can be visually exposed. Takes environment variables: `INPUT_BUCKET`, `INPUT_KEY`, `OUTPUT_BUCKET`, `OUTPUT_KEY`, `VERSION_TAG`, `VISIBILITY_DISTANCE_M` (default 30.0), `VISIBILITY_HALF_ANGLE_DEG` (default 45.0). Outputs GeoPackage and GeoParquet formats.
- **facade/requirements.txt** - Python dependencies: geopandas, pyarrow, boto3, shapely.
- **facade/Dockerfile** - Container image configuration for deployment to Amazon ECR.
- **facade/run_locally.sh** - Shell script executing facade model locally using Docker. Uses local data from `local_bucket/` directory without AWS access.

**Motorized Traffic Flow Model:**
- **traffic-flows/model_traffic_flows.py** - Propagates motorized traffic counts from fixed counting stations onto street network using distance decay and directional weighting. Loads Parquet files from input location, aggregates observations into typical day profiles by (station_id, direction, hour), propagates flows using OSM road network, outputs results in GeoPackage, GeoParquet, and normalized formats.
- **traffic-flows/requirements.txt** - Python dependencies for network analysis and geospatial operations.
- **traffic-flows/Dockerfile** - Container configuration for ECR deployment.
- **traffic-flows/run_locally.sh** - Local execution script using Docker and cached data.

**Pedestrian & Bicycle Flow Model:**
- **bike-peds-flows/model_bike_peds_flows.py** - Models pedestrian and bicycle flows on path networks. Propagates sparse counting station measurements across the city path network to estimate flows everywhere.
- **bike-peds-flows/requirements.txt** - Python dependencies: numpy, pandas, geopandas, shapely, pyproj, rtree, osmnx, networkx, boto3, scipy.
- **bike-peds-flows/Dockerfile** - Container configuration.
- **bike-peds-flows/run_locally.sh** - Local execution script.

**VBZ Public Transport Model:**
- **vbz-flows/model_vbz_flows.py** - Integrates VBZ (Zurich transit authority) passenger data onto tram and bus networks.
- **vbz-flows/requirements.txt** - Python dependencies.
- **vbz-flows/Dockerfile** - Container configuration.
- **vbz-flows/run_locally.sh** - Local execution script.

**Local Development Data:**
- **local_bucket/gold/motorized_traffic/2025_new/** - Local test data cache for development, validation, and testing without AWS cloud access.

#### `AWS/step-functions/`
Pipeline orchestration and workflow configuration.

- **step-function-parallel-2025.json** - AWS Step Functions state machine definition. Orchestrates parallel execution of all 4 models, manages task dependencies, handles errors, and coordinates data flow through the pipeline.

#### `AWS/` Root Files
- **glue_loader_jdbc.py** - AWS Glue utility script. Loads Gold layer output data from S3 into PostgreSQL data warehouse using JDBC connection.
- **production_schema.sql** - PostgreSQL/PostGIS database schema for data warehouse. Defines facade visibility tables (v2_facade_visibility), flow tables by mode (v2_motorized_traffic_flows, v2_bike_flows, etc.), materialized views for dashboard queries, and GIST spatial indexes for geographic query performance.

### `notebooks/` Directory
Jupyter notebooks for exploratory data analysis, validation, and visualization.

- **plots_facade_visibility.ipynb** - Analyzes facade visibility model outputs. Loads GeoPackage facades, computes visibility quad polygons with configurable distance (30m) and angle (45°), plots on basemap using contextily. Dependencies: geopandas, shapely, matplotlib, contextily.
- **plots_bike.ipynb** - Bicycle flow data analysis. Explores spatial patterns, validates propagation model outputs, examines peak flow times and locations.
- **plots_motorized_traffic.ipynb** - Motorized traffic flow analysis. Validates traffic propagation model, examines temporal patterns, flow distribution across network.
- **plots_public_transport.ipynb** - VBZ passenger flow analysis. Examines transit patterns, line-specific flows, integration with other mobility modes.

---

## How to Run the Code

### Prerequisites
- Python 3.8+
- PostgreSQL 12+ with PostGIS extension (for data warehouse queries only)
- Docker (for running containerized models locally)
- AWS CLI v2 (for cloud deployment only)
- AWS Account (for production deployment)
- Jupyter (for running notebooks)
- Git with SSH configured for GitHub

### Running Notebooks Locally

Jupyter notebooks analyze and visualize model outputs. Each notebook is self-contained with embedded data or references to local test data.

```bash
# Clone repository
git clone git@github.com:fcampell/DWDL.git
cd DWDL

# Open Jupyter and navigate to notebooks/
jupyter notebook

# Each notebook loads data, processes, and visualizes locally
# Dependencies are documented in notebook imports
```

Notebooks use:
- **geopandas** - Spatial data manipulation
- **shapely** - Geometric operations
- **matplotlib** - Visualization
- **contextily** - Basemap tiles (requires internet)

### Running Models Locally

Each containerized model can be tested locally before AWS deployment using provided shell scripts. Local scripts use Docker and test data in `AWS/containers/local_bucket/`.

```bash
# For facade visibility model
cd AWS/containers/facade
bash run_locally.sh

# For traffic flow model
cd AWS/containers/traffic-flows
bash run_locally.sh

# For pedestrian/bicycle model
cd AWS/containers/bike-peds-flows
bash run_locally.sh

# For VBZ model
cd AWS/containers/vbz-flows
bash run_locally.sh
```

Each `run_locally.sh` script:
1. Sets environment variables (input/output paths, parameters)
2. Builds Docker image from Dockerfile in the directory
3. Mounts local_bucket for data access
4. Executes the model Python script inside container
5. Outputs results to local_bucket/gold/

### AWS Deployment

For cloud deployment, refer to the project report for comprehensive AWS setup instructions. Key steps:

1. Create S3 buckets for Bronze/Silver/Gold data lake layers
2. Set up PostgreSQL RDS instance with PostGIS
3. Package Lambda functions and deploy to AWS Lambda
4. Build container images and push to Amazon ECR
5. Configure ECS Fargate for model execution
6. Set up EventBridge for weekly ingestion schedule
7. Use Step Functions to orchestrate parallel model execution
8. Load Gold layer outputs into PostgreSQL using glue_loader_jdbc.py

Lambda handler function: `lambda_handler(event, context)` in `AWS/Lambda/lambda-traffic-ingestion.py`

---

## Architecture

FacADe implements a **medallion lakehouse architecture** with clear separation of data layers and responsibilities:

```
Data Sources → Lambda (Ingestion) → S3 Bronze → ECS Models → S3 Gold → PostgreSQL → Tableau
```

### Data Flow

1. **Ingestion (AWS Lambda, Weekly)**
   - [`AWS/Lambda/lambda-traffic-ingestion.py`](AWS/Lambda/lambda-traffic-ingestion.py) - Fetches fresh data from Zürich OGD CKAN API
   - Data stored unchanged in S3 Bronze layer (audit trail)

2. **Normalization (Lambda)**
   - Data standardized into Silver layer
   - Consistent column naming, timestamp alignment, format conversion

3. **Transformation (AWS ECS Fargate, 4 Models in Parallel)**
   - **Facade Visibility** [`AWS/containers/facade/run_visibility_model.py`](AWS/containers/facade/run_visibility_model.py) - Generate visibility quad polygons for each building facade
   - **Traffic Flows** [`AWS/containers/traffic-flows/model_traffic_flows.py`](AWS/containers/traffic-flows/model_traffic_flows.py) - Propagate vehicle counts onto street network
   - **Bike/Pedestrian Flows** [`AWS/containers/bike-peds-flows/model_bike_peds_flows.py`](AWS/containers/bike-peds-flows/model_bike_peds_flows.py) - Propagate active mobility counts onto path network
   - **VBZ Flows** [`AWS/containers/vbz-flows/model_vbz_flows.py`](AWS/containers/vbz-flows/model_vbz_flows.py) - Integrate transit passenger data onto transit network
   - All outputs versioned and stored in S3 Gold layer

4. **Data Warehouse (PostgreSQL + PostGIS)**
   - [`AWS/production_schema.sql`](AWS/production_schema.sql) - Gold layer data loaded into relational warehouse
   - Spatial indexes (GIST) for fast geographic queries
   - Materialized views pre-computed for dashboard performance

5. **Visualization (Tableau)**
   - Interactive dashboards query PostgreSQL warehouse
   - Two user perspectives: Marketing agencies (demand-side) and property owners (supply-side)

### Technology Stack

| Component | Technology |
|-----------|-----------|
| Storage | Amazon S3 (Bronze/Silver/Gold layers) |
| Ingestion | AWS Lambda + EventBridge |
| Transformation | AWS ECS Fargate (Python containers) |
| Orchestration | AWS Step Functions |
| Database | PostgreSQL 12+ with PostGIS |
| Visualization | Tableau Public |
| Geospatial Libraries | Geopandas, NetworkX, GDAL, Shapely, Pyproj, OSMnx |
| Container Runtime | Docker, Amazon ECR |

### Code Structure

**Local Execution:**
- Notebooks can be run on local machine with Jupyter
- Models can be tested locally with Docker using `run_locally.sh` scripts
- Test data in `local_bucket/` enables development without AWS

**Cloud Execution:**
- Lambda functions deployed to AWS Lambda
- Models deployed as Docker containers to Amazon ECR and run on ECS Fargate
- S3 stores data across Bronze/Silver/Gold layers
- PostgreSQL RDS stores queryable warehouse
- Step Functions orchestrates parallel model execution

---

## Authors

- Marcel Amrein
- Fadri Campell
- Leonard Dost
