docker run --rm \
  -v "$(pwd)/local_bucket:/data" \
  -e INPUT_KEY="bronze/fassaden_zuerich.gpkg" \
  -e MAX_ROWS="100" \
  -e OUTPUT_FORMAT="parquet" \
  facade:parquet


#/ABSOLUTE/PATH/TO/YOUR/CURRENT/DIRECTORY/local_bucket  →  /data  (inside container)
