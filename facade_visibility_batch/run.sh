docker run --rm \
  -v "$(pwd)/local_bucket:/data" \
  -e LOCAL_BUCKET_ROOT=/data \
  -e INPUT_KEY="bronze/fassaden_zuerich.gpkg" \
  -e OUTPUT_KEY="silver/facade_visibility.gpkg" \
  -e VISIBILITY_BUFFER_M=50.0 \
  -e LINE_EXTENSION_M=25.0 \
  -e ENDPOINT_CIRCLE_R=50.0 \
  facade:ubuntu
