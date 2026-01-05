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

# Copy Python dependencies
COPY requirements.txt .

# Create virtualenv and install dependencies
RUN python3 -m venv /venv \
    && /venv/bin/pip install --upgrade pip \
    && /venv/bin/pip install --no-cache-dir -r requirements.txt

# Use venv Python
ENV PATH="/venv/bin:$PATH"
ENV PYTHONUNBUFFERED=1

# Copy the VBZ flows batch script
COPY model_vbz_flows.py .

# Default entrypoint
ENTRYPOINT ["python", "model_vbz_flows.py"]
