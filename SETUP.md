# FacADe Setup Guide

This guide covers setting up FacADe for local development and AWS deployment.

## Prerequisites

### Local Machine
- Python 3.8+
- Git (with SSH configured for GitHub)
- Docker (for running containers locally)
- PostgreSQL 12+ with PostGIS
- AWS CLI v2

### AWS Account
- Permissions to create: S3, Lambda, ECS, RDS, IAM roles, EventBridge
- Estimated monthly cost: ~$100

## Local Development

### 1. Clone and Setup

```bash
git clone git@github.com:fcampell/DWDL.git
cd DWDL

python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Configure AWS
aws configure
```

### 2. Local Database

```bash
# Using Docker
docker run --name facade-postgres \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=facade_db \
  -p 5432:5432 \
  -d postgis/postgis

# Initialize schema
psql -U postgres -d facade_db -f AWS/production_schema.sql
```

### 3. Run Models Locally

```bash
cd AWS/containers/facade
pip install -r requirements.txt
python run_locally.sh
```

## AWS Deployment

### 1. Create S3 Buckets

```bash
aws s3 mb s3://facade-project-dev --region eu-central-1
aws s3api put-object --bucket facade-project-dev --key bronze/
aws s3api put-object --bucket facade-project-dev --key silver/
aws s3api put-object --bucket facade-project-dev --key gold/
```

### 2. Set Up Database

```bash
# Create RDS instance
aws rds create-db-instance \
  --db-instance-identifier facade-db \
  --db-instance-class db.t3.small \
  --engine postgres \
  --engine-version 14.7 \
  --master-username postgres \
  --master-user-password <STRONG_PASSWORD> \
  --allocated-storage 100 \
  --publicly-accessible

# Once available, initialize schema
psql -h <RDS_ENDPOINT> -U postgres -d postgres \
  -c "CREATE DATABASE facade_db;"
psql -h <RDS_ENDPOINT> -U postgres -d facade_db \
  -f AWS/production_schema.sql
```

### 3. Create Lambda Functions

```bash
# Package and deploy Lambda for traffic data ingestion
cd AWS/Lambda
zip -r lambda.zip .
aws lambda create-function \
  --function-name facade-traffic-ingest \
  --runtime python3.9 \
  --role arn:aws:iam::ACCOUNT_ID:role/lambda-role \
  --handler lambda-traffic-ingestion.handler \
  --zip-file fileb://lambda.zip
```

### 4. Set Up ECS for Models

```bash
# Create cluster
aws ecs create-cluster --cluster-name facade-models

# Build and push container images
cd AWS/containers/facade
docker build -t facade-models:latest .
aws ecr create-repository --repository-name facade-models
docker tag facade-models:latest ACCOUNT_ID.dkr.ecr.eu-central-1.amazonaws.com/facade-models:latest
docker push ACCOUNT_ID.dkr.ecr.eu-central-1.amazonaws.com/facade-models:latest
```

### 5. Create Scheduler

```bash
# Schedule weekly runs via EventBridge
aws events put-rule \
  --name facade-weekly-ingest \
  --schedule-expression "cron(0 2 ? * SAT *)" \
  --state ENABLED

aws events put-targets \
  --rule facade-weekly-ingest \
  --targets "Id"="1","Arn"="arn:aws:lambda:eu-central-1:ACCOUNT_ID:function:facade-traffic-ingest"

aws lambda add-permission \
  --function-name facade-traffic-ingest \
  --statement-id AllowEventBridgeInvoke \
  --action lambda:InvokeFunction \
  --principal events.amazonaws.com \
  --source-arn arn:aws:events:eu-central-1:ACCOUNT_ID:rule/facade-weekly-ingest
```

## Dashboard Setup

### Connect Tableau to PostgreSQL

1. Create new data source in Tableau Public
2. Select PostgreSQL and configure:
   - Server: `facade-db.<region>.rds.amazonaws.com`
   - Port: `5432`
   - Database: `facade_db`
   - Username/Password: Your RDS credentials
3. Create two dashboards:
   - **Dashboard 1** (Demand-side): Facade impressions, mode weighting, district filters
   - **Dashboard 2** (Supply-side): Revenue estimates with CPM slider, portfolio view

## Monitoring

```bash
# Check ingestion logs
aws logs tail /aws/lambda/facade-traffic-ingest --follow

# Monitor ECS tasks
aws ecs describe-tasks \
  --cluster facade-models \
  --tasks <task-arn>

# Database query performance
EXPLAIN ANALYZE SELECT ... FROM v2_facade_visibility ...
```

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Lambda timeout | Increase timeout in function config or optimize code |
| ECS task OOM | Increase task memory allocation |
| Database connection error | Check RDS security group, verify credentials |
| Missing data in Gold | Check S3 Bronze layer, review ingestion logs, rerun model |
| Dashboard slow | Create materialized views, check RDS instance size |

## Production Checklist

- [ ] S3 buckets encrypted with versioning enabled
- [ ] IAM roles follow least-privilege principle
- [ ] RDS backups automated
- [ ] CloudWatch alarms configured
- [ ] Ingestion schedule verified
- [ ] Model outputs validated
- [ ] Team trained on operations
- [ ] Documentation updated with actual ARNs

## Additional Resources

- AWS Documentation: https://docs.aws.amazon.com/
- PostgreSQL + PostGIS: https://postgis.net/documentation/
- Project Code: AWS/ and notebooks/ directories
- Report Details: Original PDF documentation for methodology
