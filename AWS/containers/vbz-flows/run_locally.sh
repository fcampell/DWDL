docker run --rm \
  -v "$(pwd)/local_bucket:/data" \
  -e LOCAL_BUCKET_ROOT=/data \
  -e INPUT_PREFIX="bronze" \
  -e OUTPUT_KEY="silver/vbz_flows_daily.gpkg" \
  -e OUTPUT_FORMAT="gpkg" \
  -e OUTPUT_LEVEL="daily" \
  vbz-flows:latest
