# FacADe Deployment & Setup Guide

This guide covers setting up FacADe for development, testing, and production deployment on AWS.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Local Development Setup](#local-development-setup)
3. [AWS Infrastructure Setup](#aws-infrastructure-setup)
4. [Data Ingestion Configuration](#data-ingestion-configuration)
5. [Running Models](#running-models)
6. [Database Setup](#database-setup)
7. [Dashboard Configuration](#dashboard-configuration)
8. [Monitoring & Troubleshooting](#monitoring--troubleshooting)

---

## Prerequisites

### Local Machine Requirements

- **Python**: 3.8 or higher
- **Git**: SSH configured for GitHub
- **Docker**: For running containerized models locally
- **PostgreSQL**: 12+ with PostGIS (for local testing)
- **GDAL/PROJ**: System libraries for geospatial operations
- **AWS CLI**: v2 for interacting with AWS services
- **Node.js**: 14+ (for AWS CDK, if using Infrastructure as Code)

### AWS Account Requirements

- Permissions to create/manage:
  - S3 buckets
  - Lambda functions
  - ECS clusters and tasks
  - RDS instances
  - IAM roles and policies
  - EventBridge rules
  - VPC/networking (optional, for private deployments)

### macOS Setup

```bash
# Install Homebrew dependencies
brew install python gdal proj postgresql postgis docker aws-cli

# Verify installations
python --version       # Should be 3.8+
gdal-config --version # Should be 2.4.0+
aws --version        # Should be AWS CLI 2.x
```

### Ubuntu/Debian Setup

```bash
# System dependencies
sudo apt-get install -y \
  python3-dev python3-pip \
  libgdal-dev libproj-dev \
  postgresql postgresql-contrib postgis \
  gdal-bin proj-bin \
  docker.io awscli

# Python virtual environment
python3 -m venv venv
source venv/bin/activate
```

---

## Local Development Setup

### 1. Clone and Initialize Repository

```bash
# Clone the repository
git clone git@github.com:fcampell/DWDL.git
cd DWDL

# Create Python virtual environment
python -m venv venv
source venv/bin/activate  # macOS/Linux
# or on Windows:
venv\Scripts\activate

# Install Python dependencies
pip install --upgrade pip setuptools wheel
pip install -r requirements.txt
```

### 2. Configure Local Database (PostgreSQL)

```bash
# Start PostgreSQL (macOS with Homebrew)
brew services start postgresql

# Or using Docker
docker run --name facade-postgres \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=facade_db \
  -p 5432:5432 \
  -d postgis/postgis

# Connect to database
psql -U postgres -d facade_db

# Enable PostGIS extension
CREATE EXTENSION postgis;
CREATE EXTENSION postgis_topology;
\q
```

### 3. Initialize Database Schema

```bash
# Create tables and indexes
psql -U postgres -d facade_db -f AWS/production_schema.sql

# Verify schema created
psql -U postgres -d facade_db -c "\dt"
# Should show tables: v2_facade_visibility, v2_motorized_traffic_flows, etc.
```

### 4. Configure AWS Credentials

```bash
# Configure AWS CLI with your credentials
aws configure

# Verify configuration
aws sts get-caller-identity

# Expected output:
# {
#   "UserId": "AIDAI...",
#   "Account": "123456789012",
#   "Arn": "arn:aws:iam::123456789012:user/your-name"
# }
```

### 5. Test Local Model Execution

```bash
# Navigate to a container directory
cd AWS/containers/facade

# Install container dependencies
pip install -r requirements.txt

# Run model locally with sample data
python run_locally.sh

# Check output
ls -la /local_bucket/gold/  # Should have generated outputs
```

---

## AWS Infrastructure Setup

### 1. Create S3 Buckets

```bash
# Create main data lake bucket
aws s3 mb s3://facade-project-dev \
  --region eu-central-1

# Create partition for data layers
aws s3api put-object --bucket facade-project-dev --key bronze/
aws s3api put-object --bucket facade-project-dev --key silver/
aws s3api put-object --bucket facade-project-dev --key gold/

# Enable versioning (for data recovery)
aws s3api put-bucket-versioning \
  --bucket facade-project-dev \
  --versioning-configuration Status=Enabled

# Create access logs bucket
aws s3 mb s3://facade-project-logs --region eu-central-1

# Enable encryption
aws s3api put-bucket-encryption \
  --bucket facade-project-dev \
  --server-side-encryption-configuration '{
    "Rules": [{
      "ApplyServerSideEncryptionByDefault": {
        "SSEAlgorithm": "AES256"
      }
    }]
  }'
```

### 2. Create IAM Roles

```bash
# Create Lambda execution role
aws iam create-role \
  --role-name facade-lambda-role \
  --assume-role-policy-document '{
    "Version": "2012-10-17",
    "Statement": [{
      "Effect": "Allow",
      "Principal": {"Service": "lambda.amazonaws.com"},
      "Action": "sts:AssumeRole"
    }]
  }'

# Attach S3 access policy
aws iam put-role-policy \
  --role-name facade-lambda-role \
  --policy-name S3Access \
  --policy-document '{
    "Version": "2012-10-17",
    "Statement": [{
      "Effect": "Allow",
      "Action": ["s3:*"],
      "Resource": ["arn:aws:s3:::facade-project-dev/*"]
    }]
  }'

# Create ECS task execution role (similar process)
aws iam create-role \
  --role-name facade-ecs-task-role \
  --assume-role-policy-document '{
    "Version": "2012-10-17",
    "Statement": [{
      "Effect": "Allow",
      "Principal": {"Service": "ecs-tasks.amazonaws.com"},
      "Action": "sts:AssumeRole"
    }]
  }'
```

### 3. Set Up RDS PostgreSQL Instance

```bash
# Create security group for RDS
aws ec2 create-security-group \
  --group-name facade-rds-sg \
  --description "Security group for FacADe RDS"

# Get your IP (to allow local connections)
MY_IP=$(curl -s http://checkip.amazonaws.com)

# Allow PostgreSQL traffic
aws ec2 authorize-security-group-ingress \
  --group-name facade-rds-sg \
  --protocol tcp --port 5432 \
  --cidr ${MY_IP}/32

# Create RDS instance
aws rds create-db-instance \
  --db-instance-identifier facade-db \
  --db-instance-class db.t3.small \
  --engine postgres \
  --engine-version 14.7 \
  --master-username postgres \
  --master-user-password <STRONG_PASSWORD> \
  --allocated-storage 100 \
  --vpc-security-group-ids sg-xxxxxxxx \
  --publicly-accessible \
  --enable-cloudwatch-logs-exports postgresql

# Wait for instance to be available (5-10 minutes)
aws rds describe-db-instances --db-instance-identifier facade-db

# Once available, connect and setup PostGIS
psql -h facade-db.xxxxx.eu-central-1.rds.amazonaws.com \
  -U postgres \
  -d postgres

# In psql:
CREATE DATABASE facade_db;
\c facade_db
CREATE EXTENSION postgis;
CREATE EXTENSION postgis_topology;
```

### 4. Create Lambda Functions

```bash
# Package Lambda function
cd AWS/Lambda
zip -r lambda-traffic-ingestion.zip lambda-traffic-ingestion.py

# Create Lambda function
aws lambda create-function \
  --function-name facade-traffic-ingest \
  --runtime python3.9 \
  --role arn:aws:iam::ACCOUNT_ID:role/facade-lambda-role \
  --handler lambda-traffic-ingestion.handler \
  --zip-file fileb://lambda-traffic-ingestion.zip \
  --timeout 300 \
  --memory-size 1024

# Add environment variables
aws lambda update-function-configuration \
  --function-name facade-traffic-ingest \
  --environment Variables="{
    S3_BUCKET=facade-project-dev,
    CKAN_API_URL=https://data.stadt-zuerich.ch/api/3/action
  }"
```

### 5. Set Up ECS Cluster for Models

```bash
# Create ECS cluster
aws ecs create-cluster --cluster-name facade-models

# Create task definition for facade model
aws ecs register-task-definition --cli-input-json file://AWS/containers/facade/task-definition.json

# Create container image and push to ECR
aws ecr create-repository --repository-name facade-models

cd AWS/containers/facade
docker build -t facade-models:latest .
docker tag facade-models:latest ACCOUNT_ID.dkr.ecr.eu-central-1.amazonaws.com/facade-models:latest

# Login to ECR and push
aws ecr get-login-password | docker login --username AWS --password-stdin ACCOUNT_ID.dkr.ecr.eu-central-1.amazonaws.com
docker push ACCOUNT_ID.dkr.ecr.eu-central-1.amazonaws.com/facade-models:latest
```

### 6. Create EventBridge Schedule

```bash
# Create EventBridge rule for weekly execution
aws events put-rule \
  --name facade-weekly-ingest \
  --schedule-expression "cron(0 2 ? * SAT *)" \
  --state ENABLED \
  --description "Weekly data ingestion for FacADe"

# Add Lambda as target
aws events put-targets \
  --rule facade-weekly-ingest \
  --targets "Id"="1","Arn"="arn:aws:lambda:eu-central-1:ACCOUNT_ID:function:facade-traffic-ingest"

# Grant EventBridge permission to invoke Lambda
aws lambda add-permission \
  --function-name facade-traffic-ingest \
  --statement-id AllowEventBridgeInvoke \
  --action lambda:InvokeFunction \
  --principal events.amazonaws.com \
  --source-arn arn:aws:events:eu-central-1:ACCOUNT_ID:rule/facade-weekly-ingest
```

---

## Data Ingestion Configuration

### 1. Download Initial Data

```bash
# Create local data directory
mkdir -p data/{raw,processed}

# Download facade geometries from GeoCat
# https://opendata.swiss/en/dataset/eignung-von-hausfassaden-fur-die-nutzung-von-sonnenenergie

# Download Zürich OGD datasets
python scripts/download_ogd_data.py \
  --output-dir data/raw \
  --datasets traffic pedestrian vbz

# Verify downloads
ls -lah data/raw/
```

### 2. Upload to S3 Bronze Layer

```bash
# Upload facade geometries
aws s3 cp data/raw/facades.gpkg \
  s3://facade-project-dev/bronze/facades/

# Upload traffic data
aws s3 sync data/raw/traffic/ \
  s3://facade-project-dev/bronze/traffic/ \
  --exclude "*" --include "*.csv"

# Verify uploads
aws s3 ls s3://facade-project-dev/bronze/ --recursive
```

### 3. Configure Lambda Ingestion

Edit `AWS/Lambda/lambda-traffic-ingestion.py`:

```python
# Environment configuration
S3_BUCKET = os.environ.get('S3_BUCKET', 'facade-project-dev')
CKAN_API_BASE = 'https://data.stadt-zuerich.ch/api/3/action'

# Dataset IDs from Zürich OGD
DATASETS = {
    'traffic': 'sid_dav_verkehrszaehlung_miv_od2031',
    'pedestrian': 'ted_taz_verkehrszaehlungen_werte_fussgaenger_velo',
    'vbz': 'vbz_fahrgastzahlen_ogd'
}

def handler(event, context):
    """Weekly batch ingestion"""
    for source, dataset_id in DATASETS.items():
        print(f"Ingesting {source}...")
        # Fetch from CKAN API
        # Normalize and upload to Bronze
        # See AWS/Lambda/ for full implementation
    return {'statusCode': 200, 'body': 'Ingestion complete'}
```

---

## Running Models

### 1. Run Facade Visibility Model Locally

```bash
cd AWS/containers/facade

# Install dependencies
pip install -r requirements.txt

# Run model
python run_visibility_model.py \
  --input-facades s3://facade-project-dev/silver/facades/2025/01/facades.gpkg \
  --output-dir s3://facade-project-dev/gold/facade_visibility/v2_2025/2025/01/ \
  --visibility-distance 50 \
  --visibility-angle 120

# Check output
aws s3 ls s3://facade-project-dev/gold/facade_visibility/v2_2025/2025/01/
```

### 2. Run Traffic Flow Model

```bash
cd AWS/containers/traffic-flows

python model_traffic_flows.py \
  --input-counts s3://facade-project-dev/silver/traffic/2025/01/ \
  --output-dir s3://facade-project-dev/gold/motorized_traffic/v2_2025/2025/01/ \
  --osm-bounds "8.3,47.3,8.6,47.5" \
  --max-propagation-distance 5000
```

### 3. Run All Models via Step Functions (AWS)

```bash
# Start Step Functions execution
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:eu-central-1:ACCOUNT_ID:stateMachine:facade-pipeline \
  --input '{
    "year": 2025,
    "month": 1,
    "run_models": true
  }'

# Monitor execution
aws stepfunctions describe-execution \
  --execution-arn arn:aws:states:eu-central-1:ACCOUNT_ID:execution:facade-pipeline:run-20250104
```

---

## Database Setup

### 1. Create Schema

```bash
# Load production schema
psql -h RDS_ENDPOINT -U postgres -d facade_db -f AWS/production_schema.sql

# Verify tables
psql -h RDS_ENDPOINT -U postgres -d facade_db -c "\dt"
```

### 2. Create Spatial Indexes

```bash
# Run index creation script
psql -h RDS_ENDPOINT -U postgres -d facade_db << EOF
CREATE INDEX idx_facade_visibility_geom
  ON v2_facade_visibility USING GIST(geometry);

CREATE INDEX idx_motorized_edges_geom
  ON v2_motorized_traffic_edges USING GIST(geometry);

CREATE INDEX idx_ped_edges_geom
  ON v2_ped_edges USING GIST(geometry);

-- Analyze tables for query planning
ANALYZE v2_facade_visibility;
ANALYZE v2_motorized_traffic_flows;
EOF

# Verify indexes
psql -h RDS_ENDPOINT -U postgres -d facade_db -c "\di+"
```

### 3. Load Data from Gold Layer

```bash
# Create Glue job for loading
python AWS/glue_loader_jdbc.py \
  --s3-input s3://facade-project-dev/gold/motorized_traffic/v2_2025/2025/01/flows.parquet \
  --jdbc-url jdbc:postgresql://RDS_ENDPOINT:5432/facade_db \
  --user postgres \
  --password <PASSWORD> \
  --table v2_motorized_traffic_flows \
  --mode overwrite_partition

# Or use psql for direct load
aws s3 cp s3://facade-project-dev/gold/facade_visibility/v2_2025/2025/01/visibility.parquet .

# Convert and load (requires parquet-tools)
parquet-to-csv visibility.parquet visibility.csv
psql -h RDS_ENDPOINT -U postgres -d facade_db \
  -c "COPY v2_facade_visibility FROM STDIN CSV" < visibility.csv
```

### 4. Create Materialized Views for Dashboard

```bash
psql -h RDS_ENDPOINT -U postgres -d facade_db << EOF
CREATE MATERIALIZED VIEW facade_impressions_by_mode AS
SELECT
    f.objectid,
    f.egid,
    f.geometry,
    SUM(CASE WHEN flows.mode = 'motorized' THEN flows.flow ELSE 0 END) as motorized_impressions,
    SUM(CASE WHEN flows.mode = 'pedestrian' THEN flows.flow ELSE 0 END) as ped_impressions,
    SUM(CASE WHEN flows.mode = 'bicycle' THEN flows.flow ELSE 0 END) as bike_impressions,
    SUM(CASE WHEN flows.mode = 'transit' THEN flows.flow ELSE 0 END) as transit_impressions,
    SUM(flows.flow) as total_impressions
FROM v2_facade_visibility f
LEFT JOIN facade_flow_intersections flows ON f.objectid = flows.objectid
GROUP BY f.objectid, f.egid, f.geometry;

CREATE INDEX idx_impressions_egid ON facade_impressions_by_mode(egid);
EOF
```

---

## Dashboard Configuration

### 1. Connect Tableau to PostgreSQL

In Tableau Public:

1. Create new data source → PostgreSQL
2. Configure connection:
   - Server: `facade-db.xxxxx.eu-central-1.rds.amazonaws.com`
   - Port: `5432`
   - Database: `facade_db`
   - Username: `tableau_user` (create dedicated read-only user)
   - Password: (secure password)

3. Select tables:
   - `facade_impressions_by_mode`
   - `v2_facade_visibility`
   - `building_metadata`

### 2. Create Demand-Side Dashboard

In Tableau:

```
1. Create calculated fields:
   - Total Impressions = [motorized] + [ped] + [bike] + [transit]
   - Ped Ratio = [ped_impressions] / [total_impressions]
   - CPM Score = [total_impressions] / 1000

2. Build sheets:
   - Map: Facade locations with total impressions heatmap
   - Table: Top 100 facades by mode-weighted score
   - Bar: Impressions by district
   - Line: Temporal patterns (hourly)

3. Add filters:
   - Mobility mode weights (slider: 0-100 per mode)
   - District/location
   - Impression threshold
   - Building type

4. Create dashboard combining sheets
```

### 3. Create Supply-Side Dashboard

Similar structure, but:

```
1. Add calculated fields:
   - Estimated Daily Revenue = [total_impressions] / 1000 * [CPM]
   - Weekly Revenue = [Estimated Daily Revenue] * 7
   - Monthly Revenue = [Weekly Revenue] * 4.33

2. Add parameter:
   - CPM Slider (50-500 CHF, default 100)

3. Build portfolio view:
   - Allow selection of multiple buildings
   - Show aggregated revenue potential
   - Compare to portfolio average
```

---

## Monitoring & Troubleshooting

### 1. Monitor Ingestion

```bash
# Check CloudWatch logs
aws logs tail /aws/lambda/facade-traffic-ingest --follow

# Check S3 upload progress
aws s3 ls s3://facade-project-dev/bronze/traffic --recursive --human-readable --summarize

# Review ingestion audit table
psql -U postgres -d facade_db \
  -c "SELECT * FROM ingestion_audit ORDER BY run_time DESC LIMIT 10;"
```

### 2. Monitor Transformation Jobs

```bash
# Check ECS task logs
aws logs tail /ecs/facade-models --follow

# Monitor task status
aws ecs describe-tasks \
  --cluster facade-models \
  --tasks task-arn \
  --query 'tasks[0].[lastStatus,stoppedReason]'

# Get execution metrics
aws cloudwatch get-metric-statistics \
  --namespace AWS/ECS \
  --metric-name CPUUtilization \
  --dimensions Name=ServiceName,Value=facade-models \
  --start-time 2025-01-04T00:00:00Z \
  --end-time 2025-01-05T00:00:00Z \
  --period 3600 \
  --statistics Average
```

### 3. Query Performance Troubleshooting

```bash
# Check query execution plan
EXPLAIN ANALYZE
SELECT f.objectid, SUM(flows.flow)
FROM v2_facade_visibility f
JOIN v2_motorized_traffic_flows flows ON ST_Intersects(...)
GROUP BY f.objectid;

# Check table/index statistics
SELECT schemaname, tablename, pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
FROM pg_tables
WHERE schemaname = 'public'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;

# Reindex if needed
REINDEX INDEX idx_facade_visibility_geom;
```

### 4. Common Issues

| Issue | Solution |
|-------|----------|
| Lambda timeout | Increase timeout, optimize code, check S3 throughput |
| ECS task OOM | Increase memory allocation, check data size |
| RDS connection errors | Check security groups, verify credentials, check connections count |
| Missing data in Gold | Check ingestion logs, verify Silver data exists, rerun model |
| Slow dashboard queries | Create materialized views, add indexes, check RDS instance size |

---

## Production Checklist

- [ ] S3 buckets configured with encryption and versioning
- [ ] IAM roles follow least-privilege principle
- [ ] RDS instance has automated backups enabled
- [ ] All Lambda functions have CloudWatch alarms configured
- [ ] ECS tasks monitored for CPU/memory usage
- [ ] Ingestion schedule verified (runs weekly)
- [ ] Model outputs validated (row counts, geometry validity)
- [ ] Database backups tested and verified
- [ ] Dashboards connected and updated
- [ ] Documentation updated with actual AWS resource ARNs
- [ ] Cost monitoring set up (budget alarms)
- [ ] Team members trained on operations

---

## Useful Commands Reference

```bash
# Monitor overall stack health
aws cloudwatch list-metrics --namespace FacADe --output table

# Clean up old data from S3 (if needed)
aws s3 rm s3://facade-project-dev/silver/traffic/2024/ --recursive

# Export database for backup
pg_dump -h RDS_ENDPOINT -U postgres facade_db > backup.sql

# Restore from backup
psql -h RDS_ENDPOINT -U postgres facade_db < backup.sql

# Check AWS costs
aws ce get-cost-and-usage --time-period Start=2025-01-01,End=2025-01-31 \
  --granularity DAILY --metrics UnblendedCost --group-by Type=DIMENSION,Key=SERVICE
```

---

## Support & Resources

- **AWS Documentation**: https://docs.aws.amazon.com/
- **PostGIS Docs**: https://postgis.net/documentation/
- **Tableau Docs**: https://help.tableau.com/
- **Project Issues**: GitHub Issues in this repository
