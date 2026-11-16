import gzip
import json
import os
from collections import Counter

import boto3
from dotenv import load_dotenv

load_dotenv()

s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("MINIO_ENDPOINT"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
)

bucket = os.getenv("S3_BUCKET")
date = os.getenv("GH_ARCHIVE_DATE")

key = (
    f"landing/dt={date}/"
    f"hour=00/{date}-0.json.gz"
)

print(f"Reading: s3://{bucket}/{key}\n")

response = s3.get_object(Bucket=bucket, Key=key)

event_types = Counter()
total_records = 0
samples = []

with gzip.GzipFile(fileobj=response["Body"]) as archive:
    for line in archive:
        if not line.strip():
            continue

        event = json.loads(line)
        total_records += 1

        event_types[event.get("type", "UNKNOWN")] += 1

        if len(samples) < 3:
            samples.append(event)

print("========== SAMPLE EVENTS ==========")

for index, event in enumerate(samples, 1):
    print(f"\nEvent {index}")
    print(json.dumps(event, indent=2))

print("\n========== DATASET SUMMARY ==========")
print(f"Total records: {total_records}")
print(f"Unique event types: {len(event_types)}")

print("\nEvent type distribution:")

for event_type, count in event_types.most_common():
    print(f"{event_type}: {count:,}")
