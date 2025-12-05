docker run --rm \
  -v "$(pwd)/local_bucket:/data" \
  -e LOCAL_BUCKET_ROOT=/data \
  -e INPUT_KEY="bronze/sid_dav_verkehrszaehlung_miv_od2031_2025.csv" \
  -e OUTPUT_KEY="silver/traffic_edge_flows.gpkg" \
  -e OUTPUT_FORMAT="gpkg" \
  traffic-flow:latest

docker run --rm \
  -v "$(pwd)/local_bucket:/data" \
  -e LOCAL_BUCKET_ROOT=/data \
  -e INPUT_PREFIX="silver/motorized_traffic/sample" \
  -e INPUT_FILTER="motorized_traffic" \
  -e OUTPUT_KEY="gold/motorized_traffic" \
  traffic-flow:latest

docker run --rm \
  -v "$(pwd)/local_bucket:/data" \
  -e LOCAL_BUCKET_ROOT=/data \
  -e INPUT_PREFIX="silver/motorized_traffic/2025" \
  -e INPUT_FILTER="motorized_traffic" \
  -e OUTPUT_KEY="gold/motorized_traffic/2025" \
  traffic-flow:latest
