import os
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import boto3
import requests
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv

# Load configuration
load_dotenv()

ENDPOINT = os.getenv("MINIO_ENDPOINT")
ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
BUCKET = os.getenv("S3_BUCKET", "lake")
ARCHIVE_DATE = os.getenv("GH_ARCHIVE_DATE", "2024-01-15")

MAX_WORKERS = 4
MAX_RETRIES = 3

# Validate the date
datetime.strptime(ARCHIVE_DATE, "%Y-%m-%d")

# Create S3 client
s3 = boto3.client(
    "s3",
    endpoint_url=ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    region_name=REGION,
    config=Config(signature_version="s3v4"),
)


def object_exists(key):
    """Check whether an archive already exists in Silo."""
    try:
        s3.head_object(Bucket=BUCKET, Key=key)
        return True

    except ClientError as error:
        status = error.response.get("ResponseMetadata", {}).get(
            "HTTPStatusCode"
        )
        code = error.response.get("Error", {}).get("Code")

        if status == 404 or code in ("404", "NoSuchKey", "NotFound"):
            return False

        raise


def download_and_upload(hour):
    """Download one GH Archive file and upload it to Silo."""

    filename = f"{ARCHIVE_DATE}-{hour}.json.gz"

    url = f"https://data.gharchive.org/{filename}"

    key = (
        f"landing/dt={ARCHIVE_DATE}/"
        f"hour={hour:02d}/{filename}"
    )

    # Skip files that have already been uploaded
    if object_exists(key):
        return f"SKIPPED: {filename} already exists"

    # Retry failed downloads
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                local_path = os.path.join(temp_dir, filename)

                # Stream the download to disk
                with requests.get(
                    url,
                    stream=True,
                    timeout=(15, 180),
                ) as response:

                    if response.status_code == 429 or response.status_code >= 500:
                        response.raise_for_status()

                    response.raise_for_status()

                    with open(local_path, "wb") as file:
                        for chunk in response.iter_content(
                            chunk_size=1024 * 1024
                        ):
                            if chunk:
                                file.write(chunk)

                # Upload the original compressed file to Silo
                s3.upload_file(
                    local_path,
                    BUCKET,
                    key,
                    ExtraArgs={"ContentType": "application/gzip"},
                )

            return f"UPLOADED: {filename}"

        except requests.RequestException:
            if attempt == MAX_RETRIES:
                raise

        time.sleep(2 ** attempt)

    raise RuntimeError(f"Failed to process {filename}")


def main():
    print(f"Starting GH Archive ingestion for {ARCHIVE_DATE}")
    print(f"Destination: s3://{BUCKET}/landing/dt={ARCHIVE_DATE}/")
    print(f"Parallel workers: {MAX_WORKERS}\n")

    results = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(download_and_upload, hour): hour
            for hour in range(24)
        }

        for future in as_completed(futures):
            hour = futures[future]

            try:
                result = future.result()
                results.append(result)
                print(result)

            except (
                requests.RequestException,
                BotoCoreError,
                OSError,
                RuntimeError,
            ) as error:
                print(f"FAILED: hour {hour:02d} — {error}")

    uploaded = sum(r.startswith("UPLOADED") for r in results)
    skipped = sum(r.startswith("SKIPPED") for r in results)
    failed = 24 - len(results)

    print("\n========== INGESTION SUMMARY ==========")
    print(f"Uploaded: {uploaded}")
    print(f"Skipped:  {skipped}")
    print(f"Failed:   {failed}")
    print("=======================================")

    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
