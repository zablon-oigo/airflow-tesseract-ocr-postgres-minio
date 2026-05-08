-- create database 
CREATE DATABASE invoice_db;

-- use invoice database
\c invoice_db;

-- create table
CREATE TABLE invoices (
    id SERIAL PRIMARY KEY,
    invoice_number VARCHAR(100),
    invoice_date VARCHAR(50),
    total_amount NUMERIC(10,2)
);