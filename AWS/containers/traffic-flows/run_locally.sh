docker run --rm \
  -v "$(pwd)/local_bucket:/data" \
  -e LOCAL_BUCKET_ROOT=/data \
  -e INPUT_KEY="bronze/sid_dav_verkehrszaehlung_miv_od2031_2025.csv" \
  -e OUTPUT_KEY="silver/traffic_edge_flows.gpkg" \
  -e OUTPUT_FORMAT="gpkg" \
  traffic-flow:latest
