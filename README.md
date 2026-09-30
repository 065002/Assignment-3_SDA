# Digital Payments Live Streaming Dashboard

## Overview
Real-time digital payments analytics using **Kafka, Python, MySQL, and Grafana**.

## Data Flow
CSV Dataset → Kafka Producer → Kafka Topic → Kafka Consumer → MySQL → Grafana

## Technologies
- Python
- Apache Kafka
- MySQL
- Grafana
- Docker

## Dataset
The project uses a dataset of **15,000 digital payment transactions** containing transaction, payment, location, risk, fraud, and response-time information.

## Dashboard
The Grafana dashboard provides live monitoring of:
- Transaction Volume
- Transaction Value
- Payment Status
- Fraud Rate
- Risk Score
- Transaction Value by City
- Response Time
- High-Risk Transactions

The dashboard uses a **5-second refresh interval** and focuses on the **latest 5 minutes** of streaming data.

## Project Structure

```text
Assignment_2_Kafka/
├── producer.py
├── consumer.py
├── sample_digital_payments_15000.csv
└── README.md
