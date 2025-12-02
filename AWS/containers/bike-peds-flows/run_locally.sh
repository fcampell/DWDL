docker run --rm \
  -v "$(pwd)/local_bucket:/data" \
  -e LOCAL_BUCKET_ROOT=/data \
  -e INPUT_KEY="bronze/2025_verkehrszaehlungen_werte_fussgaenger_velo.csv" \
  -e OUTPUT_KEY="silver/zurich_pedbike_flows.gpkg" \
  -e OUTPUT_FORMAT="gpkg" \
  bike-peds-flows:latest
