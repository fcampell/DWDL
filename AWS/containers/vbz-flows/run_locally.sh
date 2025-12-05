
docker run --rm \
  -v "$(pwd)/local_bucket:/data" \
  -e LOCAL_BUCKET_ROOT="/data" \
  -e INPUT_PREFIX="silver/public_transport_vbz" \
  -e OUTPUT_KEY="gold/vbz_flows" \
  -e OUTPUT_LEVEL="daily" \
  vbz-flows:latest


docker run --rm \
  -v "$(pwd)/local_bucket:/data" \
  -e LOCAL_BUCKET_ROOT="/data" \
  -e INPUT_PREFIX="silver/public_transport_vbz" \
  -e OUTPUT_KEY="gold/vbz_flows" \
  -e OUTPUT_LEVEL="hourly" \
  vbz-flows:latest