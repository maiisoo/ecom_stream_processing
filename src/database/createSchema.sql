CREATE DATABASE project;

DROP SCHEMA IF EXISTS e_commerce CASCADE;
DROP SCHEMA IF EXISTS result CASCADE;

CREATE SCHEMA e_commerce;
CREATE SCHEMA result;

CREATE TABLE e_commerce.users (
    userID VARCHAR(255) PRIMARY KEY,
    fullname VARCHAR(255),
    email VARCHAR(255),
    gender VARCHAR(2),
    DOB DATE,
    location VARCHAR(255),
    phone VARCHAR(255),
    registration_date DATE,
    daily_time_spend DECIMAL
);

CREATE TABLE e_commerce.products (
    productID VARCHAR(355) PRIMARY KEY,
    productName VARCHAR(355),
    category VARCHAR(355),
    brand VARCHAR(355),
    price DECIMAL,
    commission_rate DECIMAL
);

CREATE TABLE result.checkouts (
    product_id VARCHAR(255),
    user_id VARCHAR(255),
    purchase_id VARCHAR(255) PRIMARY KEY,
    total_cost DOUBLE PRECISION,
    payment_method VARCHAR(255),
    quantity DOUBLE PRECISION,
    payment_status VARCHAR(255),
    description VARCHAR(255),
    purchase_time TIMESTAMP,
    fullname VARCHAR(255),
    email VARCHAR(255),
    gender VARCHAR(255),
    birth_day DATE,
    location VARCHAR(255),
    phone VARCHAR(255),
    registration_date DATE,
    product_name VARCHAR(255),
    category VARCHAR(255),
    brand VARCHAR(255),
    profit DOUBLE PRECISION,
    click_id VARCHAR(255),
    click_time TIMESTAMP,
    click_at VARCHAR(255),
    source VARCHAR(255),
    ip VARCHAR(255)
);

CREATE TABLE result.source_count (
    Source VARCHAR(255),
    Count INT,
    time TIMESTAMP,
    PRIMARY KEY(time, Source)
);