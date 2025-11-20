# Pedestrian & Cyclist Flow Simulation – Zürich

- **1. Data loading and daily aggregation**  
  The notebook loads raw pedestrian and cyclist counting data from `2025_verkehrszaehlungen_werte_fussgaenger_velo.csv`. It parses timestamps, computes daily totals for each station (VELO_DAY, FUSS_DAY), and derives directional shares from IN/OUT counts.

- **2. Station-level typical day profiles**  
  Daily data is aggregated to a “typical day” per station by averaging across all available days. Each station is represented by a single row with mean daily flows and directional shares for cyclists and pedestrians.

- **3. Building bike and pedestrian networks from OSM**  
  Using OpenStreetMap via osmnx, the notebook creates separate directed networks for cycling and walking in Zürich. It enriches edges with attributes such as length, speed, bearings, and simple comfort flags (e.g., cycleway, residential, footway, pedestrian street).

- **4. Snapping stations to the networks and defining seeds**  
  Counting stations are converted to GeoDataFrames and snapped to their nearest edges in the corresponding bike or pedestrian network. For each station, a seed is created that attaches the observed daily flow (and directional shares) to a specific edge (u, v, key).

- **5. Flow propagation through the networks**  
  For cyclists and pedestrians separately, flows are propagated from seed edges through the networks using a graph-based algorithm. At each step, flows are decayed with distance and split across outgoing edges according to direction consistency and mode-specific comfort weights.

- **6. Attaching modelled flows to edges and visualization**  
  The resulting per-edge flows are joined back onto the bike and pedestrian edge GeoDataFrames, and normalized flow columns are created for styling. The notebook produces static maps (Matplotlib) and an interactive map (Folium) that visualize estimated daily flows and station locations.

- **7. Exporting flow-enriched layers for reuse**  
  Finally, the notebook exports the flow-enriched bike and pedestrian networks as GeoPackage files, along with station layers. These outputs can be opened in GIS tools or used in other analyses without rerunning the full simulation.
