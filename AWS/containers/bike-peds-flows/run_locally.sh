
docker run --rm \
  -v "$(pwd)/local_bucket:/data" \
  -e LOCAL_BUCKET_ROOT=/data \
  -e INPUT_DIR="silver/pedestrian_bicycle/2025" \
  -e INPUT_FILENAME_SUBSTR="pedestrian" \
  -e OUTPUT_DIR="gold/zurich_pedbike_flows/2025" \
  pedbike-flows:latest

docker run --rm \
  -v "$(pwd)/local_bucket:/data" \
  -e LOCAL_BUCKET_ROOT=/data \
  -e INPUT_DIR="silver/pedestrian_bicycle/2025" \
  -e INPUT_FILENAME_SUBSTR="pedestrian" \
  -e OUTPUT_DIR="gold/zurich_pedbike_flows/2025_new" \
  -e VERSION_TAG="V1"\
  pedbike-flows:latest