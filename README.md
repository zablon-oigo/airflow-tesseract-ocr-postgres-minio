### Building an Intelligent Invoice OCR Pipeline with Airflow, MinIO, Tesseract, and PostgreSQL


A scalable document intelligence pipeline capable of turning unstructured financial documents into reliable analytics-ready datasets.




#### Archetecture Diagram

<img width="1189" height="505" alt="ocr excalidraw" src="https://github.com/user-attachments/assets/cb0750e0-5932-4540-a31b-1558e5d43fa9" />



#### Prerequisites

| Tool            | Version                        | Purpose                                               |
| --------------- | ------------------------------ | ----------------------------------------------------- |
| Apache Airflow  | 3.2.1                          | Pipeline Orchestration              |
| Minio          | Latest                         | Object Storage                      |
| PostgreSQL        | 15+                         | Structured storage and Querying                     |
| Tesseract         | 4.0.0                      | OCR for text extraction                    |




#### Project Setup

**Clone the repository**

```sh
git clone https://github.com/zablon-oigo/streamlining-data-events-minio-postgres.git 
cd streamlining-data-events-minio-postgres
```


#### Initializing Airflow


Initialize Airflow configuration:

```sh
docker compose run airflow-cli airflow config list
```


Initialize the database:
```sh
docker compose up airflow-init
```


Start all services:

```sh
docker compose up -d --build
```


Creating the Invoice Database

```sh
docker compose exec -it postgres bash 
psql -U airflow

```

Create database:
```sh
CREATE DATABASE invoice_db;
```

Connect to database:
```sh
\c invoice_db;
```

Create invoices table:

```sh
CREATE TABLE invoices ( 
id SERIAL PRIMARY KEY, 
invoice_number VARCHAR(100), 
invoice_date VARCHAR(50), 
total_amount NUMERIC(10,2) 
);
```

#### Minio Command-Line

```sh
mc alias set local http://localhost:9000 admin password
```

Expected output:

> Added local successfully.

List objects in bucket
```sh
mc ls local/warehouse/data
 ```



