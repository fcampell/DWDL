# Facade Visibility Batch – Container & AWS Architecture

## 1. Goal

We want a **repeatable batch process** that:

- Takes a **facade geometry dataset** as input (GPKG).
- Computes a **visibility buffer geometry** for each facade.
- Writes a **single output dataset** (GPKG) with:
  - all original attributes,
  - original facade geometry,
  - new visibility geometry as active geometry.

We do this **in a Docker container first (locally)** and later run **the same container on AWS (ECS Fargate)** with S3 as storage (medallion: bronze → silver).

---

## 2. High-Level Architecture

### Local (dev)

1. We build a **Docker image** with:
   - Python + GeoPandas, Shapely, etc.
   - Our batch script `run_visibility_batch.py`.

2. We simulate an S3 bucket with a **local folder**:

```
local_bucket/
├─ input/   # mimics s3://bucket/input/
└─ silver/  # mimics s3://bucket/silver/
```

3. We run the container with this folder mounted into the container and configure input/output via **environment variables**.

### AWS (prod)

1. We push the same Docker image to **ECR**.
2. We create an **ECS Fargate task** that runs the image.
3. The task:
   - Downloads the input file from **S3**.
   - Runs the batch script.
   - Uploads the output file back to **S3 (silver layer)**.
4. A **Lambda or EventBridge rule** can trigger the Fargate task (e.g. nightly or on-demand).

---

## 3. Local Container Workflow

### 3.1 Repository structure

```
facade_visibility_batch/
├─ Dockerfile
├─ requirements.txt
├─ run_visibility_batch.py
└─ local_bucket/
   ├─ input/
   │   └─ fassaden_zuerich.gpkg
   └─ silver/
```

### 3.2 Dockerfile

```
FROM python:3.11-slim

RUN apt-get update && apt-get install -y     build-essential     libspatialindex-dev  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip     && pip install --no-cache-dir -r requirements.txt

COPY run_visibility_batch.py .

ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["python", "run_visibility_batch.py"]
```

### 3.3 requirements.txt

```
geopandas[all]
shapely
pyproj
rtree
networkx
boto3
```

### 3.4 Running locally

Build image:

```
docker build -t facade_visibility:latest .
```

Run batch job:

```
docker run --rm   -v "$(pwd)/local_bucket:/data"   -e LOCAL_BUCKET_ROOT=/data   -e INPUT_KEY="input/fassaden_zuerich.gpkg"   -e OUTPUT_KEY="silver/facade_visibility.gpkg"   -e VISIBILITY_BUFFER_M=50.0   -e LINE_EXTENSION_M=25.0   -e ENDPOINT_CIRCLE_R=50.0   facade_visibility:latest
```

---

## 4. Mapping to AWS

### 4.1 Push image to ECR

```
docker tag facade_visibility:latest <AWS_ACCOUNT_ID>.dkr.ecr.<REGION>.amazonaws.com/facade-visibility:latest
aws ecr get-login-password --region <REGION> |   docker login --username AWS --password-stdin <AWS_ACCOUNT_ID>.dkr.ecr.<REGION>.amazonaws.com
docker push <AWS_ACCOUNT_ID>.dkr.ecr.<REGION>.amazonaws.com/facade-visibility:latest
```

### 4.2 ECS Fargate Task

Environment variables:

```
INPUT_KEY="input/fassaden_zuerich.gpkg"
OUTPUT_KEY="silver/facade_visibility.gpkg"
BUCKET="<our-bucket>"
USE_S3=true
```

### 4.3 Orchestration

- **EventBridge** → scheduled runs  
- **Lambda** → on-demand runs (calls RunTask)

---

## 5. Summary

- We containerize everything once using Docker.
- Local dev uses mounted folders that mimic S3.
- AWS uses the same container but pulls/pushes from S3.
- ECS Fargate runs the batch fully serverless and stateless.
