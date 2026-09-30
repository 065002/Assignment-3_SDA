Digital Payments Live Streaming & Analytics Dashboard

1. Project Overview

This project demonstrates an end-to-end real-time digital payments
analytics pipeline using Python, Apache Kafka, MySQL, Docker, and
Grafana.

A dataset containing 15,000 digital payment transactions is streamed
record-by-record through Kafka. A Python consumer receives the
transactions and stores them in MySQL. Grafana connects to MySQL and
provides an interactive dashboard for monitoring transaction activity,
financial value, payment outcomes, risk, fraud indicators, processing
performance, and statistical variability.

2. Project Objective

Simulate a real-time digital payment transaction stream.

Publish transaction records to an Apache Kafka topic.

Consume and process streamed records using Python.

Store processed transactions in MySQL.

Connect MySQL with Grafana.

Build an interactive dashboard for real-time monitoring and
analysis.

Use statistical measures such as average, standard deviation, and
variance.

Present both aggregate-level and transaction-level insights.

3. Technology Stack

Technology     Purpose

Python         Producer and consumer programs
Apache Kafka   Real-time transaction streaming
MySQL          Persistent storage and analytics
Grafana        Dashboard and visualization
Docker         Containerized infrastructure
CSV            Source transaction dataset

4. End-to-End Data Flow

CSV Dataset
    ↓
Python Producer
    ↓
Kafka Topic: digital-payments
    ↓
Python Consumer
    ↓
MySQL Database: sda_course
    ↓
Table: transactions
    ↓
Grafana
    ↓
Interactive Analytics Dashboard

Flow Explanation

1. Dataset
The source file contains 15,000 digital payment transactions with fields
covering transaction details, payment information, location, risk, fraud
indicators, and response time.

2. Python Producer
producer.py reads the CSV file and publishes each transaction as a
JSON message to the Kafka topic digital-payments. A short delay
between records simulates live streaming.

3. Apache Kafka
Kafka acts as the streaming layer between the producer and consumer.

4. Python Consumer
consumer.py receives messages from Kafka, processes the transaction
fields, and inserts the records into MySQL.

5. MySQL
Transactions are stored in the sda_course database and transactions
table. The ingested_at field records when a transaction reaches the
database.

6. Grafana
Grafana reads the MySQL data and presents live KPIs, gauges, charts, and
transaction-level information.

5. Project Structure

Assignment_2_Kafka/
│
├── producer.py
├── consumer.py
├── sample_digital_payments_15000.csv
├── digital_payments_dashboard.json
├── requirements.txt
├── README.md
│
└── screenshots/
    └── dashboard.png

File Description

producer.py --- Reads the CSV and publishes transactions to
Kafka.

consumer.py --- Consumes Kafka messages and stores
transactions in MySQL.

sample_digital_payments_15000.csv --- Source dataset
containing 15,000 transactions.

digital_payments_dashboard.json --- Exported Grafana dashboard
configuration.

requirements.txt --- Python dependencies.

README.md --- Project documentation.

screenshots/dashboard.png --- Screenshot of the completed
dashboard.

6. Database

Database: sda_course
Table: transactions

Important fields include:

transaction_id

timestamp

payment_method

payer_id

payee_id

amount_inr

status

city

merchant_category

bank

device_type

channel

response_time_ms

risk_score

fraud_flag

date

hour

day_of_week

month

ingested_at

7. Grafana Dashboard

The dashboard combines KPI gauges, analytical charts, statistical
measures, and a transaction-level table.

KPI Gauges

Total Transactions
Count of transaction_id.

Total Transaction Value
Sum of amount_inr.

Fraud Rate
Uses the 0/1 fraud_flag to monitor the proportion of transactions
flagged by the dataset.

Average Risk Score
Average of risk_score.

Risk Score Standard Deviation
Standard deviation of risk_score, showing how dispersed risk scores
are around their average.

The fraud flag is treated as a monitoring indicator and does not by
itself establish that a transaction is fraudulent.

Dashboard Visualizations

Live Transaction Volume --- Time Series
Tracks transaction count over time using ingested_at.

Transaction Value by City --- Bar Chart
Groups transactions by city and calculates total transaction value.

Payment Status Distribution --- Donut Chart
Shows the distribution of transaction statuses.

Average Response Time --- Time Series
Tracks average response_time_ms over time.

High-Risk Transaction Monitor --- Table
Displays transaction ID, amount, city, merchant category, status, risk
score, fraud flag, and response time for transaction-level monitoring.

8. Statistical Analysis

The dashboard uses statistical measures to provide deeper analysis:

Average (AVG): Measures the central tendency of risk, response
time, or transaction amount.

Standard Deviation (STDDEV): Measures the dispersion of values
around their average.

Variance (VARIANCE): Measures the squared dispersion of
numerical observations.

These measures help identify variability in transaction risk and
transaction amounts.

9. Live Dashboard Settings

Recommended settings:

Time Range: Last 5 minutes
Refresh Rate: 5 seconds

For live time-series panels, ingested_at is used because it represents
when the transaction reaches MySQL.

10. How to Run the Project

Step 1 --- Start the Infrastructure

Make sure the Kafka, MySQL, and Grafana Docker containers are running.

Step 2 --- Start the Producer

From the project directory:

python producer.py

The producer reads the CSV and sends transactions to Kafka.

Step 3 --- Start the Consumer

Open a second terminal:

python consumer.py

The consumer receives Kafka messages and stores them in MySQL.

Step 4 --- Open Grafana

http://localhost:3000

Log in using the credentials configured for the local Grafana
environment.

Step 5 --- Open the Dashboard

Open the saved dashboard and monitor the incoming transactions.

11. Dashboard Architecture

┌──────────────────────────────┐
│ Digital Payments CSV         │
│ 15,000 Transactions          │
└──────────────┬───────────────┘
               │
               ▼
┌──────────────────────────────┐
│ Python Producer              │
│ producer.py                  │
└──────────────┬───────────────┘
               │
               ▼
┌──────────────────────────────┐
│ Apache Kafka                 │
│ Topic: digital-payments      │
└──────────────┬───────────────┘
               │
               ▼
┌──────────────────────────────┐
│ Python Consumer              │
│ consumer.py                  │
└──────────────┬───────────────┘
               │
               ▼
┌──────────────────────────────┐
│ MySQL                        │
│ sda_course.transactions      │
└──────────────┬───────────────┘
               │
               ▼
┌──────────────────────────────┐
│ Grafana                      │
│ Interactive Dashboard        │
│                              │
│ KPIs • Gauges • Charts       │
│ Risk • Fraud • Performance   │
│ High-Risk Transactions       │
└──────────────────────────────┘

12. Business Insights

The dashboard can be used to examine:

Overall transaction volume and value.

Changes in transaction activity over time.

Cities contributing the highest transaction value.

Payment success and failure distribution.

Overall transaction risk.

Variability in risk scores.

Changes in processing response time.

Transactions requiring closer risk review.

13. Key Learning Outcomes

This project demonstrates:

Real-time data streaming.

Kafka producer-consumer architecture.

JSON message processing.

MySQL data ingestion.

Docker-based infrastructure.

Grafana dashboard development.

KPI and gauge design.

Time-series and categorical visualization.

Statistical analysis using average, standard deviation, and
variance.

Transaction-level risk monitoring.

Conversion of streaming data into business insights.

14. Conclusion

The project demonstrates an end-to-end real-time analytics pipeline for
digital payment transactions. Transaction records move from the CSV
source through a Python producer, Kafka, and a Python consumer into
MySQL, where Grafana retrieves and visualizes the data.

The resulting dashboard combines live monitoring, financial metrics,
payment outcomes, risk indicators, statistical analysis, and
transaction-level review to demonstrate how streaming data can be
transformed into meaningful business insights.
