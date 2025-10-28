# Facade Ad Revenue: Zürich (EGID-based Potential & DCF)

> Estimate the daily view potential of building facades in the City of Zürich and convert it into advertising revenue, incl. a Discounted Cash Flow (DCF) that accounts for approvals & installation CAPEX.

---

## 1) Business Idea

### Problem
Real estate owners struggle to quantify the advertising value of their facades. Without a defensible “views → value” model, the opportunity is ignored or underpriced.

### Solution (What we build)
A dashboard where a user enters an **EGID** or **address** and receives:
- Estimated **daily potential views** per facade segment (street-level & PT flows),
- **Ad KPIs** (CPM-based revenue, occupancy scenarios)
- **DCF** over a planning horizon, incl. permitting/installation costs.

### Stakeholders
- **Supply:** Building owners, asset managers, developers  
- **Demand:** Media agencies, OOH (out-of-home) advertisers  
- **Public sector:** City planners (sightline & clutter policies)

### USP
- **Geometric precision:** Uses façade geometry & **modeled viewing points** (lines of sight) rather than only addresses.
- **Local suitability:** Calibrated on Zürich’s open pedestrian, traffic, and transit flows.
- **Finance-ready:** Converts flows to **per-facade KPIs** (addition and **DCF** (CAPEX, OPEX, pricing, utilization).)

---

## 2) Data Sources

### Core geometry (facades)
- **Solarenergie: Eignung Fassaden** (Geo-facades with attributes)  
  https://opendata.swiss/de/dataset/eignung-von-hausfassaden-fur-die-nutzung-von-sonnenenergie

### Viewer flows (dynamic)
- **Pedestrian & bike counts (quarter-hourly, City of Zürich)** – sensors across the city; CSV/Parquet + locations for joins.  
  - Catalog landing: https://data.stadt-zuerich.ch/dataset?tags=fussverkehr  
  - OGD dataset page: https://opendata.swiss/de/dataset/daten-der-automatischen-fussganger-und-velozahlung-viertelstundenwerte2

- **Motor traffic counters – real-time (ASTRA / opentransportdata.swiss)** – API with API key; counters on major roads (volume, speed, classes).  
  - Dataset: https://data.opentransportdata.swiss/de/dataset/trafficcountersrealtime  
  - Platform docs: https://opentransportdata.swiss/de/strassenverkehr/

- **VBZ stop passenger frequencies (example: Hardbrücke)** – directional counts, useful to weight PT sightlines around stops/bridges.  
  - Zürich catalog: https://data.stadt-zuerich.ch/dataset/vbz_frequenzen_hardbruecke  
  - OGD mirror: https://opendata.swiss/de/dataset/fahrgastfrequenzen-an-der-vbz-haltestelle-hardbrucke

### Helpful portals
- City of Zürich Open Data catalog: https://data.stadt-zuerich.ch/  
- OGD City of Zürich org page: https://opendata.swiss/de/organization/stadt-zurich  
- Kanton Zürich traffic fundamentals: https://www.zh.ch/de/mobilitaet/gesamtverkehrsplanung/verkehrsgrundlagen/verkehrsdaten.html

> 🔎 **Dynamic source requirement:** This project consumes at least **two dynamic sources** (e.g., ASTRA real-time traffic counters and Zürich quarter-hourly pedestrian counts), fulfilling the course guideline for dynamic data.  

---

## 3) Method (Backwards from the Business Case)

1. **User Request (EGID / address)**  
   Normalize input → resolve EGID → fetch building polygon & facade segments.

2. **Viewing Point Modeling**  
   - Derive street-side **viewing lines** from pedestrian paths, sidewalks, crossings, and PT platforms;  
   - Apply **visibility constraints** (distance bands, half-angles, occlusion heuristics, min facade size).

4. **Spatial Join with Flows**  
   - Snap viewing lines to sensor buffers (pedestrian, traffic, PT) and interpolate flow between sensors;  
   - Weight flows by **exposure time** (dwell/stopping areas), speed, and **field-of-view** to the facade.

5. **Views → Ad KPIs**  
   - Daily views → **impressions**; apply **viewability** factor; convert to **CPM revenue** and **expected occupancy** by season.

6. **DCF (per facade)**  
   - Inputs: permit time/cost, mounting cost, maintenance, pricing curve, occupancy;  
   - Outputs: **NPV, IRR, payback**; sensitivity: price ±x%, occupancy ±y%.

7. **Dashboard**  
   - Lookup by **EGID / address**, display per-facade KPIs, map, and DCF.

---

## 4) Metrics & Assumptions

- **Impressions/day** = Σ(flow at viewing segment × visibility factor × dwell/view time × directional weight)  
- **Revenue/day** = Impressions × (CPM/1000) × booked share  
- **DCF**: horizon `T` years, discount `r`, with CAPEX at t=0 & OPEX annually:
- NPV = -CAPEX + Σ_{t=1..T} (Revenue_t - OPEX_t) / (1 + r)^t

- **Key inputs** (configurable): CPM, booked %, seasonality, visibility decay by distance, angle cutoff, speed factor, illumination policy windows.

MVP: Very simplified viewability calculation and matching with flow data 

---

## 5) System Architecture (AWS Data Lake First)

```mermaid
flowchart LR
U[User (EGID/Address)] --> API[API Gateway]
API --> LBD[Lambda Resolver (EGID->facades)]
subgraph Data Lake (S3)
  S3B[(Bronze/raw)]
  S3S[(Silver/clean)]
  S3G[(Gold/analytics)]
end
SRC1[CityZH Ped/Bike (CSV/Parquet)] --> GLUE1[Glue ETL]
SRC2[ASTRA Traffic Realtime (API)] --> GLUE2[Glue ETL Streaming/Batch]
SRC3[VBZ Frequencies] --> GLUE3[Glue ETL]
GLUE1 --> S3S
GLUE2 --> S3S
GLUE3 --> S3S
FACADE[Facades Geo (GeoPackage/GeoParquet)] --> GLUE4[Glue ETL]
GLUE4 --> S3S
S3S --> SPATIAL[Batch Geospatial (AWS Batch/ECS + Docker + GeoPandas/pygeos)]
SPATIAL --> S3G
CATALOG[Glue Data Catalog/Lake Formation] --- S3B
CATALOG --- S3S
CATALOG --- S3G
ATH[Athena/Trino Geospatial] --- S3G
DB[(Aurora PostgreSQL + PostGIS)] <--> SPATIAL
API --> Q[Query Layer]
Q --> ATH
Q --> DB
Q --> CCH[Cache (ElastiCache)]
API --> APP[Dashboard (Amplify/CloudFront or Streamlit on ECS)]
```

**Why these components**

- S3 (Bronze/Silver/Gold): cheap, versioned lake; store GeoParquet for columnar + spatial.
- Glue / Lake Formation: catalog, schema evolution, permissions; Glue ETL for batch/stream.
- ASTRA real-time is ingested via API Gateway + Lambda or Glue streaming job; pedestrian/bike via scheduled batch.
- PostGIS (Aurora) for heavy spatial joins/visibility ops (line-of-sight, buffers, bearings); alternatively Athena geospatial where possible.
- Batch/ECS + Docker for GeoPandas/Shapely jobs (visibility cones, façade segmentation).
- API Gateway + Lambda serve the dashboard; Amplify/CloudFront for a React app or Streamlit on ECS for course speed.


## 6) Data Lake Architecture & Pipelines

**Zones & Formats**

- Bronze: raw source drops (CSV, JSON, GeoPackage, API snapshots).
- Silver: cleaned & normalized (CRS unified, timestamps to CET/CEST, deduped, enriched with EGID & façade IDs).
- Gold: analytics tables (daily façade views, impressions, KPIs, DCF inputs/outputs) in Parquet/GeoParquet.

**Ingestion**

- Ped/Bike (Zürich): daily/quarter-hourly pull to Bronze → clean to Silver with sensor metadata join.
- ASTRA Real-time: incremental fetch (API key), store both raw JSON and hourly aggregates.
- VBZ frequencies: batch pull; normalize to stop/line/dir and geocode buffers.
- Facades: import from GeoPackage/WFS → split into facade segments; compute normals/orientations.

**Processing (examples)**

- Visibility modeling: create viewing rays/cones per path segment, filter by angle & distance.
- Flow interpolation: between sensors along network edges; speed-adjusted exposure.
- Join: facade viewing buffer x flow x viewing factor → impressions/day. (very simplified)
- Finance: DCF in PySpark (Glue) or SQL (Athena) with parameter tables.

**Quality & Governance**
- Schema evolution via Glue; partition by date/sensor_type.
- Unit tests (PyTest) for geometry ops; Great Expectations for data checks.
- Lineage via job metadata & tags in the Catalog.

## 7) MVP (for mid-term)

- Scope: Single EGID query, single facade side, city sensors within 250 m.
- Inputs: EGID, CPM, occupancy %, discount rate, CAPEX.
- Outputs: Map (facade + viewing buffers), impressions/day, baseline revenue/day, NPV (5y).
- No persistence of DCF (compute on request).
- Simple dashboard (Streamlit on ECS or Amplify-hosted React calling API).

## 8) KPIs & DCF (v1 formulas)

- impressions_day = Σ(flow_qh × vis_factor(distance, angle) × dwell_weight)
- revenue_day = impressions_day × CPM / 1000 × booked_share
- NPV_5y = -CAPEX + Σ_{t=1..5} (revenue_t - OPEX_t) / (1+r)^t
- Sensitivities: CPM ±20%, booked_share 40–80%, CAPEX ±30%.



---

### Architecture guidance (quick, opinionated)

- **Spatial engine:** Use **Aurora PostgreSQL + PostGIS** for sightlines, buffers, bearings, and view-cone tests; keep **GeoParquet** in S3 as the lake truth; use **Athena/Trino** for light geospatial and all analytics joins.  
- **Batch geospatial**: Put heavy Python geometry (GeoPandas/Shapely/pygeos) in a **Docker image** and run in **AWS Batch** or **ECS Fargate**; emit results back to **S3 Gold**.  
- **Ingestion:**  
  - CityZürich ped/bike: **Glue Spark** scheduled job → S3 Bronze → clean to Silver.  
  - ASTRA realtime: **API Gateway + Lambda** (small bursts) or **Glue streaming** if you want stateful micro-batching.  
- **Serving:** **API Gateway + Lambda** over **Athena**/**PostGIS**; add **ElastiCache** for hot EGIDs.  
- **Dashboard:** quickest win is **Streamlit on ECS Fargate**, then later migrate to **React+MapLibre** on **Amplify/CloudFront**.

---

#### Sources (for your notes)
City of Zürich open data catalog and ped/bike counts, VBZ Hardbrücke, ASTRA traffic realtime, and mobility platform documentation were used to validate availability, APIs, and update frequency. :contentReference[oaicite:0]{index=0}

Also see Kanton & ASTRA overviews for background. :contentReference[oaicite:1]{index=1}

> Course dynamic-data requirement acknowledged. :contentReference[oaicite:2]{index=2}

If you want, I can also drop in a minimal **CDK** skeleton and a **GeoPandas** Dockerfile next.
::contentReference[oaicite:3]{index=3}
