import json
import time
from datetime import datetime

from kafka import KafkaConsumer
import mysql.connector
from mysql.connector import Error


# ============================================================
# CONFIGURATION
# ============================================================

KAFKA_SERVER = "localhost:9092"
TOPIC = "digital-payments"
CONSUMER_GROUP = "digital-payments-consumer"

# MySQL is running in Docker, but this Python file runs on
# Windows, so the published MySQL port is accessed through localhost.
MYSQL_HOST = "localhost"
MYSQL_PORT = 3306
MYSQL_DATABASE = "sda_course_A3"
MYSQL_USER = "root"
MYSQL_PASSWORD = "Kirtibehl18@"


# ============================================================
# MYSQL CONNECTION
# ============================================================

def create_database_and_table():
    """Create the database and transactions table if they do not exist."""
    connection = None
    cursor = None

    try:
        connection = mysql.connector.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD
        )
        cursor = connection.cursor()

        cursor.execute(
            f"CREATE DATABASE IF NOT EXISTS `{MYSQL_DATABASE}`"
        )
        cursor.execute(f"USE `{MYSQL_DATABASE}`")

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS transactions (
                transaction_id VARCHAR(50) PRIMARY KEY,
                timestamp DATETIME,
                payment_method VARCHAR(30),
                payer_id VARCHAR(50),
                payee_id VARCHAR(50),
                amount_inr DECIMAL(12,2),
                status VARCHAR(20),
                city VARCHAR(50),
                merchant_category VARCHAR(50),
                bank VARCHAR(50),
                device_type VARCHAR(30),
                channel VARCHAR(30),
                response_time_ms INT,
                risk_score DECIMAL(10,2),
                fraud_flag INT,
                date DATE,
                hour INT,
                day_of_week VARCHAR(20),
                month VARCHAR(20),
                ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_ingested_at (ingested_at),
                INDEX idx_city (city),
                INDEX idx_status (status),
                INDEX idx_fraud_flag (fraud_flag)
            )
        """)

        connection.commit()
        print("MySQL database/table ready.")

    except Error as e:
        print(f"MySQL setup error: {e}")
        raise

    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None and connection.is_connected():
            connection.close()


def connect_mysql():
    """Connect to the assignment database."""
    return mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        database=MYSQL_DATABASE,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD
    )


# ============================================================
# DATA CONVERSION
# ============================================================

def parse_datetime(value):
    """Convert the Kafka timestamp string into a Python datetime."""
    if not value:
        return None

    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).replace(
            tzinfo=None
        )
    except ValueError:
        # Fallback for common timestamp format
        return datetime.strptime(str(value), "%Y-%m-%d %H:%M:%S")


def parse_date(value):
    """Convert YYYY-MM-DD into a Python date."""
    if not value:
        return None

    return datetime.strptime(str(value), "%Y-%m-%d").date()


# ============================================================
# INSERT INTO MYSQL
# ============================================================

INSERT_SQL = """
    INSERT IGNORE INTO transactions (
        transaction_id,
        timestamp,
        payment_method,
        payer_id,
        payee_id,
        amount_inr,
        status,
        city,
        merchant_category,
        bank,
        device_type,
        channel,
        response_time_ms,
        risk_score,
        fraud_flag,
        date,
        hour,
        day_of_week,
        month
    )
    VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    )
"""


def insert_transaction(connection, row):
    """Insert one Kafka transaction into MySQL."""
    values = (
        row.get("transaction_id"),
        parse_datetime(row.get("timestamp")),
        row.get("payment_method"),
        row.get("payer_id"),
        row.get("payee_id"),
        float(row["amount_inr"]) if row.get("amount_inr") not in (None, "") else None,
        row.get("status"),
        row.get("city"),
        row.get("merchant_category"),
        row.get("bank"),
        row.get("device_type"),
        row.get("channel"),
        int(row["response_time_ms"]) if row.get("response_time_ms") not in (None, "") else None,
        float(row["risk_score"]) if row.get("risk_score") not in (None, "") else None,
        int(row["fraud_flag"]) if row.get("fraud_flag") not in (None, "") else None,
        parse_date(row.get("date")),
        int(row["hour"]) if row.get("hour") not in (None, "") else None,
        row.get("day_of_week"),
        row.get("month")
    )

    cursor = connection.cursor()
    try:
        cursor.execute(INSERT_SQL, values)
        connection.commit()
        return cursor.rowcount
    finally:
        cursor.close()


# ============================================================
# KAFKA CONSUMER
# ============================================================

def create_consumer():
    return KafkaConsumer(
        TOPIC,
        bootstrap_servers=[KAFKA_SERVER],
        group_id=CONSUMER_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda message: json.loads(message.decode("utf-8")),
        key_deserializer=lambda key: key.decode("utf-8") if key else None
    )


def main():
    print("=" * 70)
    print("Digital Payments Kafka Consumer")
    print("=" * 70)
    print(f"Kafka server : {KAFKA_SERVER}")
    print(f"Kafka topic  : {TOPIC}")
    print(f"MySQL        : {MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}")
    print("=" * 70)

    # Make sure the database and table exist.
    create_database_and_table()

    # Connect to MySQL.
    mysql_connection = connect_mysql()
    print("Connected to MySQL.")

    # Connect to Kafka.
    consumer = create_consumer()
    print("Connected to Kafka.")
    print("Waiting for transactions...\n")

    processed = 0

    try:
        for message in consumer:
            row = message.value

            try:
                inserted = insert_transaction(mysql_connection, row)

                if inserted == 1:
                    processed += 1
                    print(
                        f"Consumed & stored: "
                        f"{row.get('transaction_id')} | "
                        f"{row.get('payment_method')} | "
                        f"₹{row.get('amount_inr')} | "
                        f"{row.get('status')} | "
                        f"{row.get('city')} | "
                        f"Fraud={row.get('fraud_flag')} | "
                        f"Total stored={processed}"
                    )
                else:
                    print(
                        f"Skipped duplicate: {row.get('transaction_id')}"
                    )

            except Error as e:
                print(f"MySQL insert error: {e}")

                # Reconnect if MySQL connection was lost.
                try:
                    if not mysql_connection.is_connected():
                        mysql_connection = connect_mysql()
                        print("Reconnected to MySQL.")
                except Error as reconnect_error:
                    print(f"MySQL reconnect error: {reconnect_error}")
                    time.sleep(3)

    except KeyboardInterrupt:
        print("\nConsumer stopped by user.")

    finally:
        consumer.close()

        if mysql_connection.is_connected():
            mysql_connection.close()

        print("Kafka consumer and MySQL connection closed.")


if __name__ == "__main__":
    main()
