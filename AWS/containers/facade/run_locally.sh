docker run --rm \
  -v "$(pwd)/local_bucket:/data" \
  -e LOCAL_BUCKET_ROOT=/data \
  -e INPUT_KEY="bronze/facade_geometries/fassaden_zürich.gpkg" \
  -e OUTPUT_KEY="gold/facade_visibility" \
  -e VERSION_TAG="v1" \
  -e VISIBILITY_DISTANCE_M=30.0 \
  -e VISIBILITY_HALF_ANGLE_DEG=45.0 \
  facade-visibility:latest