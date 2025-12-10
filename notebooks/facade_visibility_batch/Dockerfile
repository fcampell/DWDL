FROM ghcr.io/osgeo/gdal:ubuntu-small-latest

# Install Python + tools + spatial index (rtree)
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-venv \
    build-essential \
    libspatialindex-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .

# Create and populate a virtualenv
RUN python3 -m venv /venv \
    && /venv/bin/pip install --upgrade pip \
    && /venv/bin/pip install --no-cache-dir -r requirements.txt

# Use venv Python/pip by default
ENV PATH="/venv/bin:$PATH"
ENV PYTHONUNBUFFERED=1

COPY run_visibility_batch.py .

ENTRYPOINT ["python", "run_visibility_batch.py"]
