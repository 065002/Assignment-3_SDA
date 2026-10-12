Digital Payments Live Streaming & Analytics Dashboard
1. Project Overview

This project demonstrates an end-to-end real-time digital payments analytics pipeline using Python, Apache Kafka, MySQL, Docker, and Grafana.

A dataset containing 15,000 digital payment transactions (1-31 August 2026, no missing values and no duplicate transaction IDs) is streamed record-by-record through Kafka. A Python consumer receives the transactions and stores them in MySQL. Grafana connects to MySQL and provides an interactive dashboard for monitoring transaction activity, financial value, payment outcomes, risk, fraud indicators, processing performance, and statistical variability.

2. Project Objective
Simulate a real-time digital payment transaction stream.
Publish transaction records to an Apache Kafka topic.
Consume and process streamed records using Python.
Store processed transactions in MySQL.
Connect MySQL with Grafana.
Build an interactive dashboard for real-time monitoring and analysis.
Use statistical measures such as average, standard deviation, and variance.
Present both aggregate-level and transaction-level insights.
3. Technology Stack
Technology	Purpose
Python	Producer and consumer programs
Apache Kafka	Real-time transaction streaming
MySQL	Persistent storage and analytics
Grafana	Dashboard and visualization
Docker	Containerized infrastructure
CSV	Source transaction dataset
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

1. Dataset The source file contains 15,000 digital payment transactions with fields covering transaction details, payment information, location, risk, fraud indicators, and response time. Payment methods are UPI (9,811), CARD (3,628) and WALLET (1,561). Statuses are SUCCESS (13,989), FAILED (762) and REVIEW (249). Transactions span 10 cities, 10 merchant categories and 8 banks.

2. Python Producer producer.py reads the CSV file and publishes each transaction as a JSON message to the Kafka topic digital-payments. A short delay between records simulates live streaming.

3. Apache Kafka Kafka acts as the streaming layer between the producer and consumer.

4. Python Consumer consumer.py receives messages from Kafka, processes the transaction fields, and inserts the records into MySQL.

5. MySQL Transactions are stored in the sda_course database and transactions table. The ingested_at field records when a transaction reaches the database.

6. Grafana Grafana reads the MySQL data and presents live KPIs, gauges, charts, and transaction-level information.

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
producer.py — Reads the CSV and publishes transactions to Kafka.
consumer.py — Consumes Kafka messages and stores transactions in MySQL.
sample_digital_payments_15000.csv — Source dataset containing 15,000 transactions.
digital_payments_dashboard.json — Exported Grafana dashboard configuration.
requirements.txt — Python dependencies.
README.md — Project documentation.
screenshots/dashboard.png — Screenshot of the completed dashboard.
6. Database

Database: sda_course Table: transactions

Important fields include:

transaction_id, timestamp, payment_method, payer_id, payee_id, amount_inr, status, city, merchant_category, bank, device_type, channel, response_time_ms, risk_score, fraud_flag, date, hour, day_of_week, month, ingested_at

Data notes

amount_inr ranges from 50 to 55,263 and is heavily right-skewed (median about 1,190 vs mean about 1,973).
response_time_ms ranges from 150 to 1,901 (mean about 852). risk_score ranges from 1 to 100.
fraud_flag is 1 for about 2.6% of rows (395 transactions): every REVIEW row is flagged, about 19% of FAILED rows are flagged, and no SUCCESS row is flagged.
The CSV's own hour column does not always match timestamp (for example, the first rows show hour 0 for times around 09:14). Derive the hour from timestamp (for example HOUR(timestamp)) in any hourly analysis.
timestamp holds the original transaction time (August 2026). ingested_at is added when the record reaches MySQL.
7. Grafana Dashboard

The dashboard combines KPI gauges, analytical charts, statistical measures, and a transaction-level table.

KPI Gauges

Total Transactions — Count of transaction_id.

Total Transaction Value — Sum of amount_inr (INR 2.96 crore, about 29.59 million, once all 15,000 records are ingested).

Fraud Rate — Uses the 0/1 fraud_flag to monitor the proportion of transactions flagged by the dataset (about 2.63% overall).

Average Risk Score — Average of risk_score (about 36.6 overall).

Risk Score Standard Deviation — Standard deviation of risk_score, showing how dispersed risk scores are around their average (about 19.5 overall).

The fraud flag is treated as a monitoring indicator and does not by itself establish that a transaction is fraudulent.

Dashboard Visualizations

Live Transaction Volume — Time Series Tracks transaction count over time using ingested_at.

Transaction Value by City — Bar Chart Groups transactions by city and calculates total transaction value.

Payment Status Distribution — Donut Chart Shows the distribution of transaction statuses.

Average Response Time — Time Series Tracks average response_time_ms over time.

High-Risk Transaction Monitor — Table Displays transaction ID, amount, city, merchant category, status, risk score, fraud flag, and response time for transaction-level monitoring.

8. Statistical Analysis

The dashboard uses statistical measures to provide deeper analysis:

Average (AVG): Measures the central tendency of risk, response time, or transaction amount.
Standard Deviation (STDDEV): Measures the dispersion of values around their average.
Variance (VARIANCE): Measures the squared dispersion of numerical observations.

These measures help identify variability in transaction risk and transaction amounts. For the full dataset, risk_score has a standard deviation of about 19.5 (variance about 378), and amount_inr has a standard deviation of about 2,665.

Because amount_inr is heavily right-skewed, the average is pulled up by a small number of large payments. Use the median alongside the average when describing a typical transaction amount.

9. Live Dashboard Settings

Recommended settings:

Time Range: Last 5 minutes
Refresh Rate: 5 seconds

For live time-series panels, ingested_at is used because it represents when the transaction reaches MySQL. (The original timestamp values are from August 2026, so they would not fall inside a "last 5 minutes" window.)

10. How to Run the Project

Step 1 — Start the Infrastructure Make sure the Kafka, MySQL, and Grafana Docker containers are running.

Step 2 — Start the Producer From the project directory:

python producer.py

The producer reads the CSV and sends transactions to Kafka.

Step 3 — Start the Consumer Open a second terminal:

python consumer.py

The consumer receives Kafka messages and stores them in MySQL.

Step 4 — Open Grafana http://localhost:3000 Log in using the credentials configured for the local Grafana environment.

Step 5 — Open the Dashboard Open the saved dashboard and monitor the incoming transactions.

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

Findings from the full dataset (once all 15,000 records are ingested):

Payment mix: UPI carries about 65% of transactions and value. Overall success rate is 93.3% (5.1% failed, 1.7% in review).
Value: Chennai has the highest value among cities (about INR 31.7 lakh); Fuel is the top merchant category by value (about INR 31.2 lakh).
Bank reliability: Bank of Baroda has the highest failure rate (5.52%) and HDFC Bank the lowest (4.37%).
Risk: Flagged transactions have a much higher average risk score (about 58.9) than unflagged ones (about 35.9). Payments above INR 10,000 are 1.7% of volume but 13.7% of value and are flagged about twice as often (5.1%).
Performance: Average response time is about 850 ms for every bank and channel, so no bank or channel is clearly slower.
Timing: Hourly volume is nearly flat (595-677 transactions per hour); weekends and Mondays run roughly 25% higher than Tuesday-Thursday.
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
Statistical analysis using average, standard deviation, and variance.
Transaction-level risk monitoring.
Conversion of streaming data into business insights.
14. Conclusion

The project demonstrates an end-to-end real-time analytics pipeline for digital payment transactions. Transaction records move from the CSV source through a Python producer, Kafka, and a Python consumer into MySQL, where Grafana retrieves and visualizes the data.

The resulting dashboard combines live monitoring, financial metrics, payment outcomes, risk indicators, statistical analysis, and transaction-level review to demonstrate how streaming data can be transformed into meaningful business insights.
