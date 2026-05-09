## Building an Intelligent Invoice OCR Pipeline with Airflow, MinIO, Tesseract, and PostgreSQL


A scalable document intelligence pipeline capable of turning unstructured financial documents into reliable analytics-ready datasets.

![Airflow](https://img.shields.io/badge/Apache%20Airflow-Orchestration-017CEE?logo=apache-airflow&logoColor=white)
![MinIO](https://img.shields.io/badge/MinIO-Object%20Storage-C72E49?logo=minio&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Database-336791?logo=postgresql&logoColor=white)
![Tesseract](https://img.shields.io/badge/Tesseract-OCR-5C5C5C)
![Docker](https://img.shields.io/badge/Docker-Containerized-2496ED?logo=docker&logoColor=white)
![Python](https://img.shields.io/badge/Python-Data%20Processing-3776AB?logo=python&logoColor=white)



### Archetecture Diagram

<img width="1189" height="505" alt="ocr excalidraw" src="https://github.com/user-attachments/assets/cb0750e0-5932-4540-a31b-1558e5d43fa9" />



### Prerequisites

| Tool | Version | Purpose |
|------|--------|--------|
| Apache Airflow | 3.2.1 | Workflow orchestration |
| MinIO | Latest | Object storage (S3-compatible) |
| PostgreSQL | 15+ | Structured data storage |
| Tesseract OCR | 4.0.0 | Text extraction from images |
| Docker | - | Containerized environment |




### Project Setup

**Clone the repository**

```sh
git clone https://github.com/zablon-oigo/airflow-tesseract-ocr-postgres-minio.git
cd airflow-tesseract-ocr-postgres-minio
```


### Initializing Airflow


**Initialize Airflow configuration**

```sh
docker compose run airflow-cli airflow config list
```


**Initialize metadata database**

```sh
docker compose up airflow-init
```


**Start all services**

```sh
docker compose up -d --build
```

### PostgreSQL Setup


**Access PostgreSQL container**

```sh
docker compose exec -it postgres bash 
psql -U airflow

```

**Create database**

```sh
CREATE DATABASE invoice_db;
```

**Connect to database**

```sh
\c invoice_db;
```

**Create invoices table**

```sh
CREATE TABLE invoices ( 
id SERIAL PRIMARY KEY, 
invoice_number VARCHAR(100), 
invoice_date VARCHAR(50), 
total_amount NUMERIC(10,2) 
);
```

### MinIO Setup

**Configure MinIO client**

```sh
mc alias set local http://localhost:9000 admin password
```

Expected output:

> Added local successfully.

**List stored files**

```sh
mc ls local/warehouse/data
 ```

### Medium Blog

[Building an Intelligent Invoice OCR Pipeline with Airflow, MinIO, Tesseract, and PostgreSQL](https://medium.com/@zablon-oigo/building-an-intelligent-invoice-ocr-pipeline-with-airflow-minio-tesseract-and-postgresql-ace67fd2c2a7)

