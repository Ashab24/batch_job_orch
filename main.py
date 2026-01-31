import json
import csv
from io import StringIO
import os
import logging
from google.cloud import storage
from datetime import datetime, timezone, timedelta
from collections import defaultdict


logging.basicConfig(level=logging.INFO)

def get_env_var(name: str, required: bool = True, default=None):
    value = os.getenv(name, default)
    if required and value is None:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value

def list_eligible_json_files(bucket_name, prefix, lookback_hours):
    logging.info("Connecting to GCS...")
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    cutoff_time = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
    logging.info("Filtering files updated before: %s", cutoff_time)

    eligible_files = []

    for blob in bucket.list_blobs(prefix=prefix):
        if not blob.name.endswith(".json"):
            continue

        logging.info(
        "Found file: %s | updated: %s",
        blob.name,
        blob.updated
    )

        if blob.updated >= cutoff_time:
            continue

        eligible_files.append(blob)


    logging.info("Total eligible files: %d", len(eligible_files))
    return eligible_files



def download_and_parse_events(blobs):
    """
    Downloads JSON files from GCS and parses event records.

    Returns a list of event dictionaries.
    """
    all_events = []

    for blob in blobs:
        logging.info("Downloading file: %s", blob.name)

        # Download file content as bytes
        data = blob.download_as_bytes()

        try:
            content = data.decode("utf-8")
        except UnicodeDecodeError:
            logging.error("Failed to decode file %s as UTF-8", blob.name)
            raise

        try:
            events = json.loads(content)
        except json.JSONDecodeError:
            logging.error("Invalid JSON in file %s", blob.name)
            raise

        if not isinstance(events, list):
            raise ValueError(f"Expected JSON array in {blob.name}")

        logging.info("Parsed %d events from %s", len(events), blob.name)

        for event in events:
            all_events.append(event)

    logging.info("Total events parsed across all files: %d", len(all_events))
    return all_events


def normalize_events(events):
    """
    Converts raw event records into normalized records with hourly buckets.

    Returns a list of dictionaries with:
    - hour (datetime, truncated to hour, UTC)
    - event_type (string)
    """
    normalized = []

    for event in events:
        try:
            ts = datetime.fromisoformat(
                event["event_timestamp"].replace("Z", "+00:00")
            )
        except Exception:
            raise ValueError(f"Invalid timestamp format: {event}")

        # Truncate to hour
        hour_bucket = ts.replace(minute=0, second=0, microsecond=0)

        normalized.append({
            "hour": hour_bucket,
            "event_type": event["event_type"]
        })

    logging.info("Normalized %d events into hourly buckets", len(normalized))
    return normalized




def aggregate_events(normalized_events):
    """
    Aggregates events by (hour, event_type).

    Returns a list of dictionaries with:
    - hour (datetime)
    - event_type (string)
    - total_events (int)
    """
    aggregation = defaultdict(int)

    for record in normalized_events:
        key = (record["hour"], record["event_type"])
        aggregation[key] += 1

    results = []
    for (hour, event_type), total in aggregation.items():
        results.append({
            "hour": hour,
            "event_type": event_type,
            "total_events": total
        })

    logging.info("Aggregated into %d hourly records", len(results))
    return results


def write_hourly_csv_summaries(aggregated_results, output_bucket):
    """
    Writes one CSV file per hour bucket to GCS.
    """
    client = storage.Client()
    bucket = client.bucket(output_bucket)

    # Group records by hour
    records_by_hour = {}

    for record in aggregated_results:
        hour = record["hour"]
        records_by_hour.setdefault(hour, []).append(record)

    for hour, records in records_by_hour.items():
        year = hour.strftime("%Y")
        month = hour.strftime("%m")
        day = hour.strftime("%d")
        hour_str = hour.strftime("%H")

        output_path = (
            f"summaries/{year}/{month}/{day}/hour={hour_str}/summary.csv"
        )

        logging.info("Writing CSV for hour %s to %s", hour, output_path)

        # Write CSV to in-memory buffer
        buffer = StringIO()
        writer = csv.writer(buffer)

        writer.writerow(["hour", "event_type", "total_events"])

        for r in records:
            writer.writerow([
                r["hour"].isoformat(),
                r["event_type"],
                r["total_events"]
            ])

        blob = bucket.blob(output_path)
        blob.upload_from_string(
            buffer.getvalue(),
            content_type="text/csv"
        )

    logging.info("Finished writing CSV summaries")





def main():
    logging.info("Cloud Run Job started")

    input_bucket = get_env_var("INPUT_BUCKET")
    output_bucket = get_env_var("OUTPUT_BUCKET")
    input_prefix = get_env_var("INPUT_PREFIX", default="events/")
    lookback_hours = int(get_env_var("LOOKBACK_HOURS", default="24"))

    logging.info("Configuration loaded:")
    logging.info("INPUT_BUCKET=%s", input_bucket)
    logging.info("OUTPUT_BUCKET=%s", output_bucket)
    logging.info("INPUT_PREFIX=%s", input_prefix)
    logging.info("LOOKBACK_HOURS=%d", lookback_hours)


    eligible_files = list_eligible_json_files(
        bucket_name=input_bucket,
        prefix=input_prefix,
        lookback_hours=lookback_hours
    )

    logging.info("Job finished file discovery phase")

    if not eligible_files:
        logging.info("No eligible files to process. Exiting job.")
        return

    events = download_and_parse_events(eligible_files)

    logging.info("Job finished JSON parsing phase")



    normalized_events = normalize_events(events)

    logging.info("Job finished event normalization phase")




    aggregated_results = aggregate_events(normalized_events)



    for record in aggregated_results:
        logging.info(
            "Hour=%s | Type=%s | Count=%d",
            record["hour"],
            record["event_type"],
            record["total_events"]
        )

    logging.info("Job finished aggregation phase")

    write_hourly_csv_summaries(
        aggregated_results=aggregated_results,
        output_bucket=output_bucket
    )

    logging.info("Job finished CSV writing phase")




    logging.info("Cloud Run Job completed successfully")

if __name__ == "__main__":
    main()
