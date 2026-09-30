import csv
import json
import time
from kafka import KafkaProducer

# Kafka configuration
KAFKA_SERVER = "localhost:9092"
TOPIC = "digital-payments"

# Dataset
CSV_FILE = "sample_digital_payments_15000.csv"

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)

print("Kafka Producer Started")
print(f"Topic: {TOPIC}")
print(f"Dataset: {CSV_FILE}")
print("-" * 80)

try:
    with open(CSV_FILE, mode="r", encoding="utf-8") as file:

        reader = csv.DictReader(file)

        for row in reader:

            # Convert numeric fields
            row["amount_inr"] = float(row["amount_inr"])
            row["response_time_ms"] = int(row["response_time_ms"])
            row["risk_score"] = float(row["risk_score"])
            row["fraud_flag"] = int(row["fraud_flag"])
            row["hour"] = int(row["hour"])

            # Send transaction to Kafka
            producer.send(
                TOPIC,
                value=row,
                key=row["transaction_id"].encode("utf-8")
            )

            producer.flush()

            print(
                f"Sent: {row['transaction_id']} | "
                f"{row['payment_method']} | "
                f"₹{row['amount_inr']} | "
                f"{row['status']} | "
                f"{row['city']}"
            )

            # Simulate real-time streaming
            time.sleep(0.5)

except FileNotFoundError:
    print(f"ERROR: Could not find {CSV_FILE}")

except KeyboardInterrupt:
    print("\nProducer stopped by user.")

finally:
    producer.close()
    print("\nKafka Producer Closed.")