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

key = f"landing/dt={date}/hour=00/{date}-0.json.gz"

print(f"Profiling s3://{bucket}/{key}\n")

response = s3.get_object(Bucket=bucket, Key=key)

event_counts = Counter()
samples = {}

with gzip.GzipFile(fileobj=response["Body"]) as archive:
    for line in archive:
        if not line.strip():
            continue

        event = json.loads(line)
        event_type = event.get("type", "UNKNOWN")

        event_counts[event_type] += 1

        # Keep one representative event per type
        if event_type not in samples:
            samples[event_type] = event

print(f"Total events scanned: {sum(event_counts.values())}")
print(f"Event types discovered: {len(samples)}")

print("\n========== EVENT SCHEMA PROFILES ==========")

for event_type in sorted(samples):
    event = samples[event_type]
    payload = event.get("payload") or {}

    print(f"\n{'=' * 60}")
    print(f"EVENT TYPE: {event_type}")
    print(f"COUNT: {event_counts[event_type]:,}")

    print("\nTop-level fields:")
    print(list(event.keys()))

    print("\nActor fields:")
    print(list((event.get("actor") or {}).keys()))

    print("\nRepository fields:")
    print(list((event.get("repo") or {}).keys()))

    print("\nOrganization fields:")
    print(list((event.get("org") or {}).keys()))

    print("\nPayload fields:")
    print(list(payload.keys()))

    # Inspect commonly nested payload objects
    for field in (
        "pull_request",
        "issue",
        "comment",
        "release",
        "review",
        "member",
        "forkee",
    ):
        value = payload.get(field)

        if isinstance(value, dict):
            print(f"\n{field} nested fields:")
            print(list(value.keys()))

    # Inspect a sample commit without printing the entire array
    commits = payload.get("commits")

    if isinstance(commits, list) and commits:
        print("\nFirst commit fields:")
        print(list(commits[0].keys()))

    print("\nSample payload (truncated):")
    print(json.dumps(payload, indent=2)[:1800])

print("\n========== END OF PROFILE ==========")
