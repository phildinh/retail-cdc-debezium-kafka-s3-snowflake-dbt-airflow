# =============================================================
# Retail CDC Pipeline — Kafka Consumer
# =============================================================
# Reads CDC events from Kafka topics
# Batches messages and writes to S3 as JSON files
# Partitioned by table and timestamp for incremental loading
# =============================================================

import json
import time
import boto3
from datetime import datetime
from confluent_kafka import Consumer, KafkaError

from ingestion.core.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_ORDERS,
    KAFKA_TOPIC_ORDER_ITEMS,
    KAFKA_CONSUMER_GROUP,
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    AWS_REGION,
    S3_BUCKET_NAME,
    get_s3_key
)


# -------------------------------------------------------------
# Config
# -------------------------------------------------------------
BATCH_SIZE    = 100   # write to S3 after every 100 messages
BATCH_TIMEOUT = 30    # or after 30 seconds — whichever comes first


# -------------------------------------------------------------
# Block 1 — Build Kafka consumer
# auto.offset.reset = earliest means if no bookmark exists yet
# start from the very beginning of the topic
# enable.auto.commit = false means WE control when to commit
# so we never lose messages if the script crashes mid-batch
# -------------------------------------------------------------
def get_consumer():
    return Consumer({
        "bootstrap.servers":  KAFKA_BOOTSTRAP_SERVERS,
        "group.id":           KAFKA_CONSUMER_GROUP,
        "auto.offset.reset":  "earliest",
        "enable.auto.commit": False
    })


# -------------------------------------------------------------
# Block 2 — Build S3 client
# Uses credentials from config
# -------------------------------------------------------------
def get_s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )


# -------------------------------------------------------------
# Block 3 — Write batch to S3
# Groups messages by table so each file contains one table only
# Filename includes timestamp + microseconds for uniqueness
# -------------------------------------------------------------
def write_batch_to_s3(s3_client, batch):
    # Group messages by table name
    # So orders and order_items go to separate S3 files
    grouped = {}
    for message in batch:
        table = message.get("_table", "unknown")
        if table not in grouped:
            grouped[table] = []
        grouped[table].append(message)

    # Write one file per table in this batch
    for table, records in grouped.items():
        timestamp   = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
        filename    = f"{table}_{timestamp}.json"
        s3_key      = get_s3_key(table, filename)

        # Convert list of records to newline-delimited JSON
        # One JSON object per line — standard format for data lakes
        body = "\n".join(json.dumps(record) for record in records)

        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=s3_key,
            Body=body.encode("utf-8"),
            ContentType="application/json"
        )

        print(f"[{datetime.now()}] Wrote {len(records)} {table} records to s3://{S3_BUCKET_NAME}/{s3_key}")


# -------------------------------------------------------------
# Block 4 — Parse incoming Kafka message
# Decodes the raw bytes from Kafka into a Python dictionary
# Adds _table and _consumed_at metadata fields
# so downstream knows which table this record came from
# -------------------------------------------------------------
def parse_message(message):
    topic = message.topic()
    value = json.loads(message.value().decode("utf-8"))

    # Derive table name from topic
    # postgres.public.orders -> orders
    table = topic.split(".")[-1]

    # Add metadata fields
    value["_table"]       = table
    value["_topic"]       = topic
    value["_consumed_at"] = datetime.utcnow().isoformat()

    return value


# -------------------------------------------------------------
# Block 5 — Main consumer loop
# Polls Kafka continuously
# Flushes batch to S3 when size OR time threshold is reached
# Commits offsets only after successful S3 write
# -------------------------------------------------------------
def run_consumer():
    print(f"[{datetime.now()}] Starting Kafka consumer...")

    consumer  = get_consumer()
    s3_client = get_s3_client()

    # Subscribe to both CDC topics
    consumer.subscribe([KAFKA_TOPIC_ORDERS, KAFKA_TOPIC_ORDER_ITEMS])

    batch         = []
    last_flush    = time.time()

    try:
        while True:

            # Ask Kafka if there are any new messages
            # Waits up to 1 second for a message before returning None
            message = consumer.poll(timeout=1.0)

            if message is None:
                # No new messages this poll — check if batch needs flushing
                pass

            elif message.error():
                # Kafka reported an error on this message
                if message.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition — not a real error, just means
                    # we've caught up to the latest message
                    print(f"[{datetime.now()}] Reached end of partition — waiting for new messages")
                else:
                    print(f"[{datetime.now()}] Kafka error: {message.error()}")

            else:
                # Valid message — parse and add to batch
                record = parse_message(message)
                batch.append(record)

            # -----------------------------------------------------
            # Flush batch to S3 if:
            # 1. Batch has reached 100 messages, OR
            # 2. 30 seconds have passed since last flush
            # This ensures we never lose more than 30 seconds
            # of CDC events even during quiet periods
            # -----------------------------------------------------
            time_since_flush = time.time() - last_flush
            batch_full       = len(batch) >= BATCH_SIZE
            timeout_reached  = time_since_flush >= BATCH_TIMEOUT

            if batch and (batch_full or timeout_reached):
                write_batch_to_s3(s3_client, batch)

                # Commit offsets AFTER successful S3 write
                # If write fails we don't commit — Kafka replays on restart
                consumer.commit()

                print(f"[{datetime.now()}] Flushed {len(batch)} records — resetting batch")
                batch      = []
                last_flush = time.time()

    except KeyboardInterrupt:
        print(f"\n[{datetime.now()}] Consumer stopped by user")

    finally:
        # Flush any remaining messages before shutdown
        if batch:
            write_batch_to_s3(s3_client, batch)
            consumer.commit()
            print(f"[{datetime.now()}] Final flush — {len(batch)} records written")

        consumer.close()
        print(f"[{datetime.now()}] Consumer closed cleanly")


if __name__ == "__main__":
    run_consumer()