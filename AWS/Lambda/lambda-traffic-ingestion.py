import json
import urllib.request
import boto3
from datetime import datetime

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    """
    Fetch traffic data for ONE MONTH and save to S3
    Called by Step Function for each month in parallel

    Parameters:
    - year: Year (e.g., 2025)
    - month: Month (1-12)

    Example: {"year": 2025, "month": 1}
    """

    API_ENDPOINT = "https://data.stadt-zuerich.ch/api/3/action/datastore_search"
    RESOURCE_ID = "89b2179c-19a5-41dc-baac-3ee3d6a42927"
    BUCKET_NAME = "facadebronzebucket"

    try:
        # Get year and month from event
        year = event.get('year', 2025)
        month = event.get('month', 1)

        # Format date range for this month
        month_str = str(month).zfill(2)
        start_date = f"{year}-{month_str}-01"

        # Calculate end date (last day of month)
        if month == 12:
            end_date = f"{year}-12-31"
        else:
            next_month = str(month + 1).zfill(2)
            # Use day 0 of next month (last day of current month)
            end_date = f"{year}-{next_month}-01"

        print(f"Loading traffic data for {year}-{month_str}")
        print(f"Date range: {start_date} to {end_date}")

        # Fetch data from API (get all records, will filter by date)
        url = f"{API_ENDPOINT}?resource_id={RESOURCE_ID}&limit=20000"
        print(f"Fetching from API: {url}")

        with urllib.request.urlopen(url, timeout=120) as response:
            data = response.read()
            api_response = json.loads(data)

        # Extract all records from API
        all_records = api_response.get('result', {}).get('records', [])
        print(f"Retrieved {len(all_records)} records from API")

        # Filter records for this month
        filtered_records = []
        for record in all_records:
            record_date = record.get('MessungDatZeit', '')[:10]  # Extract YYYY-MM-DD
            if start_date <= record_date < end_date:
                filtered_records.append(record)

        total_in_range = len(filtered_records)
        print(f"After filtering: {total_in_range} records for {year}-{month_str}")

        if total_in_range == 0:
            print(f"⚠️ No records found for {year}-{month_str}")
            return {
                'statusCode': 204,
                'body': json.dumps({
                    'message': 'No records found',
                    'year': year,
                    'month': month
                })
            }

        # Get current timestamp for filename
        now = datetime.utcnow()
        day = str(now.day).zfill(2)
        hour = str(now.hour).zfill(2)
        minute = str(now.minute).zfill(2)
        timestamp = now.isoformat()

        # Save to S3: traffic/2025/01/12_0916_zurich_traffic_2025-01.json
        filename = f"{day}_{hour}{minute}_zurich_traffic_{year}-{month_str}_{timestamp.replace(':', '-')}.json"
        s3_key = f"traffic/{year}/{month_str}/{filename}"

        # Prepare data for S3
        output_data = {
            'fetch_timestamp': timestamp,
            'year': year,
            'month': month,
            'records_fetched': total_in_range,
            'data': filtered_records
        }

        # Upload to S3
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=json.dumps(output_data, indent=2),
            ContentType='application/json'
        )

        print(f"✅ Saved {total_in_range} records to s3://{BUCKET_NAME}/{s3_key}")

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Success',
                'year': year,
                'month': month,
                'records_fetched': total_in_range,
                's3_location': f"s3://{BUCKET_NAME}/{s3_key}"
            })
        }

    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': f'Error: {str(e)}',
                'year': event.get('year'),
                'month': event.get('month')
            })
        }
