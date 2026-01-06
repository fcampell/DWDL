# FacADe: A Data-Driven Framework for City-Wide Facade Advertising Visibility and Monetization

[![GitHub](https://img.shields.io/badge/GitHub-fcampell%2FDWDL-blue)](https://github.com/fcampell/DWDL)
[![License](https://img.shields.io/badge/License-MIT-green)](LICENSE)

## Overview

**FacADe** is a comprehensive, end-to-end data engineering and analytics platform designed to quantify facade advertising visibility and monetization potential across Zürich. By combining heterogeneous mobility data sources (motorized traffic, pedestrian flows, bicycle traffic, and public transport) with geospatial modeling, FacADe enables marketing agencies to identify high-potential advertising surfaces and allows property owners to transparently assess revenue opportunities.

### Project Context

This project was developed as a Master's thesis project by the **Dataholics** team at **Hochschule Luzern (HSLU)**, completed in December 2025.

**Team Members:**
- Marcel Amrein (21-175-989)
- Fadri Campell (20-924-122)
- Leonard Dost (24-861-239)

---

## Key Features

-  **City-Scale Visibility Analysis** - Estimates facade visibility across all buildings in Zürich
- **Multi-Modal Mobility Modeling** - Integrates motorized traffic, pedestrians, cyclists, and public transport
- **Temporal Granularity** - Provides hourly flow estimates and time-of-day insights
- **Dual-Use Dashboards** - Separate views for marketing agencies (demand-side) and property owners (supply-side)
- **Revenue Estimation** - Translates visibility metrics into indicative monetization potential
- **Scalable Architecture** - AWS-based cloud infrastructure supporting continuous data ingestion
- **Reproducible Pipeline** - Versioned models and data lineage for transparency

---

## System Architecture

FacADe follows a **medallion lakehouse architecture** with clear separation of concerns:

### Layers

1. **Data Lake (Bronze → Silver → Gold)**
   - **Bronze**: Raw ingested data from Zürich OGD, unchanged
   - **Silver**: Standardized, normalized datasets
   - **Gold**: Business-ready, modeled outputs

2. **Data Transformation**
   - Facade visibility polygon generation
   - Network-based flow propagation (motorized, pedestrian, bicycle, public transport)
   - Temporal alignment and aggregation

3. **Data Warehouse**
   - PostgreSQL with PostGIS for spatial analytics
   - Optimized star schema for facade exposure metrics
   - Materialized views for dashboard performance

4. **Visualization**
   - Tableau dashboards for interactive analysis
   - Role-specific views for different stakeholders

### Technology Stack

| Component | Technology |
|-----------|-----------|
| **Data Lake Storage** | Amazon S3 |
| **Data Catalog** | AWS Glue Data Catalog |
| **Ingestion Orchestration** | AWS Lambda + EventBridge |
| **Transformation & Modeling** | AWS ECS Fargate (Python containers) |
| **Analytical Database** | PostgreSQL with PostGIS on AWS RDS |
| **Visualization** | Tableau Public |
| **Geospatial Libraries** | GDAL, Geopandas, NetworkX, Shapely |

---

## Data Sources

FacADe integrates data from multiple sources, all publicly available via Zürich's Open Government Data (OGD) platform:

| Source | Type | Update Frequency | Coverage | Purpose |
|--------|------|------------------|----------|---------|
| **Facade Geometries** | Static | Irregular | All buildings | Define advertising surfaces |
| **Motorized Traffic** | Dynamic | Daily | 30+ counting stations | Vehicle flow estimation |
| **Pedestrian & Bicycle** | Dynamic | Daily | 15+ counting stations | Active mobility flows |
| **VBZ Passenger Data** | Dynamic | Monthly | All tram/bus lines | Public transport exposure |
| **OpenStreetMap** | Static | On-demand | City network | Flow propagation reference |

**Data Limitations:**
- Pedestrian and bicycle measurements are sparse (only ~15 counting stations), requiring propagation modeling that introduces uncertainty in peripheral areas
- Motorized traffic and public transport data provides more comprehensive coverage

---

## Project Structure

```
DWDL/
├── README.md                          # This file
├── ARCHITECTURE.md                    # Detailed system architecture
├── SETUP.md                           # Deployment and setup instructions
├── .gitignore                         # Git ignore rules
│
├── AWS/
│   ├── Lambda/                        # Lambda function for batch data ingestion
│   │   ├── lambda-traffic-ingestion.py
│   │   ├── facade-pedestrian-velo/
│   │   ├── facade-vbz-reisende-batch/
│   │   └── facade-vbz-small-tables/
│   │
│   ├── containers/                    # ECS Fargate containers for geospatial modeling
│   │   ├── facade/                    # Facade visibility polygon generation
│   │   ├── traffic-flows/             # Motorized traffic flow propagation
│   │   ├── bike-peds-flows/           # Pedestrian and bicycle flow modeling
│   │   ├── vbz-flows/                 # Public transport flow modeling
│   │   └── local_bucket/              # Local development data
│   │
│   ├── step-functions/                # AWS Step Functions orchestration
│   │   └── step-function-parallel-2025.json
│   │
│   └── production_schema.sql          # PostgreSQL schema for data warehouse
│
├── notebooks/                         # Jupyter notebooks for analysis and visualization
│   ├── plots_facade_visibility.ipynb
│   ├── plots_bike.ipynb
│   └── plots_motorized_traffic.ipynb
│
└── docs/                              # Additional documentation
```

---

## Key Concepts

### Facade Visibility Model

The facade visibility model transforms 2D facade geometries into 3D visibility polygons by:
1. Projecting visibility regions outward from each facade based on orientation
2. Defining a cone-shaped visibility area within a configurable distance
3. Computing which mobility flows could potentially see each facade

### Flow Propagation

Since mobility measurements come from sparse counting stations, the system propagates flows onto the street/path network:

- **Mass-balance constraints** ensure flow is redistributed, not multiplied
- **Distance decay** reduces flow impact with distance from measurement points
- **Mode-specific networks** use walking paths for pedestrians, cycling networks for bikes, road networks for vehicles
- **Temporal alignment** aggregates hourly measurements into consistent time buckets

### Impression Metrics

For each facade, FacADe calculates:
- **Total daily impressions** across all mobility modes
- **Mode-specific impressions** (pedestrian, bicycle, motorized, public transport)
- **Temporal patterns** (morning rush, midday, evening rush)
- **Aggregated metrics** at building level (EGID) and portfolio level

---

## Getting Started

### Prerequisites

- Python 3.8+
- PostgreSQL 12+ with PostGIS extension
- AWS account (for cloud deployment)
- Git with SSH configured for GitHub

### Local Development Setup

1. **Clone the repository:**
   ```bash
   git clone git@github.com:fcampell/DWDL.git
   cd DWDL
   ```

2. **Create a Python virtual environment:**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure AWS credentials** (for cloud operations):
   ```bash
   aws configure
   ```

5. **Run models locally** (see individual container directories for details)

### Deployment

For full AWS deployment instructions, see [SETUP.md](SETUP.md).

---

## Dashboards & Outputs

### Dashboard 1: Facade Visibility for Marketing Agencies
**Purpose:** Help marketing agencies identify and rank facades by advertising potential

**Key Features:**
- Search and filter buildings by total impressions
- Analyze exposure by mobility mode (pedestrian, cyclist, vehicle, transit)
- Adjust mode-specific weighting to reflect campaign priorities
- View spatial context and street-level imagery

**Link:** [Facade Impressions Dashboard](https://public.tableau.com/app/profile/leonard.dost/viz/FacADeDashbaord/FacADeDashboard)

### Dashboard 2: Revenue Potential for Real Estate Owners
**Purpose:** Assess facade advertising revenue opportunities for property portfolios

**Key Features:**
- Select individual buildings or entire portfolios
- Configure cost-per-mille (CPM) pricing assumptions
- View indicative revenue estimates (daily, weekly, monthly)
- Benchmark buildings against portfolio averages

**Link:** [Facade Revenue Dashboard](https://public.tableau.com/app/profile/leonard.dost/viz/FacADeRevenueDashbaord/FacADeRevenueDashboard)

---

## Data Pipeline Overview

```
Zürich OGD
   ↓
┌─────────────────────────────────────────────┐
│         AWS Lambda (Weekly Batch)            │
│  - Fetch data from CKAN API                 │
│  - Validate and normalize                   │
└─────────────────────────────────────────────┘
   ↓
┌─────────────────────────────────────────────┐
│     Amazon S3 - Bronze Layer                │
│  (Raw data, unchanged)                      │
└─────────────────────────────────────────────┘
   ↓
┌─────────────────────────────────────────────┐
│     Amazon S3 - Silver Layer                │
│  (Standardized, normalized)                 │
└─────────────────────────────────────────────┘
   ↓
┌──────────────────────────────────────────────────────────┐
│      AWS ECS Fargate (Geospatial Modeling)               │
│  - Facade Visibility Model                              │
│  - Motorized Traffic Flow Propagation                   │
│  - Pedestrian & Bicycle Flow Modeling                   │
│  - VBZ Public Transport Integration                     │
└──────────────────────────────────────────────────────────┘
   ↓
┌─────────────────────────────────────────────┐
│     Amazon S3 - Gold Layer                  │
│  (Business-ready, versioned outputs)       │
└─────────────────────────────────────────────┘
   ↓
┌─────────────────────────────────────────────┐
│   PostgreSQL with PostGIS (RDS)             │
│   (Analytical warehouse)                    │
└─────────────────────────────────────────────┘
   ↓
┌─────────────────────────────────────────────┐
│        Tableau Public (Dashboards)          │
│  (Interactive visualization & analysis)    │
└─────────────────────────────────────────────┘
```

---

## Key Findings & Limitations

### Strengths
- Successfully demonstrates technical feasibility of city-scale visibility analysis
- Clear end-to-end pipeline with consistent data lineage
- Effective separation of concerns (modeling in lake, serving in warehouse)
- Dual-use dashboards serving both demand and supply perspectives

### Limitations
- **Pedestrian & Bicycle Data Sparsity** - Only 15 counting stations citywide; peripheral areas have high uncertainty
- **Flow Propagation Assumptions** - Models rely on simplifying assumptions about movement behavior and decay with distance
- **OpenStreetMap Dependency** - Results sensitive to network completeness and accuracy
- **Pricing Assumptions** - Revenue estimates based on configurable CPM values, not empirical market data

### Future Work
- **Probabilistic modeling** using Monte Carlo simulations to quantify uncertainty
- **Computer vision** on street imagery to estimate effective advertising area
- **Market integration** by collecting empirical pricing data from real campaigns
- **Advanced propagation** using agent-based or neural network models

---

## Data Quality & Validation

The system ensures data quality through:
- **Medallion architecture** providing clear quality boundaries
- **Schema validation** at normalization step
- **Watermark tracking** preventing silent data loss
- **Model execution logs** for traceability
- **Spatial validation** catching invalid geometries
- **Mass-balance safeguards** preventing unrealistic flow estimates

---

## Configuration & Parameters

Key configuration parameters (found in container requirements and model scripts):

| Parameter | Purpose | Default |
|-----------|---------|---------|
| Visibility distance | Radius of facade visibility cone | ~50-100m (configurable) |
| Flow decay exponent | Distance-based flow decay | 2.0 |
| Propagation max distance | Maximum distance flows propagate | Mode-specific |
| Temporal granularity | Aggregation interval | Hourly |

---

## Contributing

This project was completed as a Master's thesis. For inquiries, improvements, or collaboration opportunities, please contact the development team through GitHub.

---

## License

This project is provided under the MIT License. See LICENSE file for details.

---

## Citation

If you use FacADe in academic work, please cite:

```bibtex
@thesis{FacADe2025,
  title={FacADe: A Data-Driven Framework for City-Wide Facade Advertising Visibility and Monetization},
  author={Amrein, Marcel and Campell, Fadri and Dost, Leonard},
  school={Hochschule Luzern},
  year={2025},
  month={December}
}
```

---

## Additional Resources

- **Project Report**: See documentation in Zürich OGD references
- **Data Sources**: https://data.stadt-zuerich.ch (Zürich Open Government Data)
- **Tableau Dashboards**: Links provided above
- **Architecture Details**: See [ARCHITECTURE.md](ARCHITECTURE.md)
- **Setup Instructions**: See [SETUP.md](SETUP.md)

---

## Questions & Support

For questions about:
- **Architecture & Design**: See ARCHITECTURE.md
- **AWS Deployment**: See SETUP.md
- **Data Sources & Quality**: See project report section on Data Lake
- **Models & Methodology**: See individual container READMEs and project report sections 5-6
