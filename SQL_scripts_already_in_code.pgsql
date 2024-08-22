-- CREATE DATABASE stockDB;


CREATE TABLE companies (
    company_id SERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    symbol VARCHAR(50) NOT NULL,
    DESCRIPTION TEXT NOT NULL,
    date_listing DATE NOT NULL
);

CREATE TABLE stock_rate (
    stock_rate_id SERIAL PRIMARY KEY,
    company_id INTEGER REFERENCES companies (company_id) ON DELETE CASCADE,
    date DATE NOT NULL,
    open INTEGER NOT NULL,
    high INTEGER NOT NULL,
    low INTEGER NOT NULL,
    close INTEGER NOT NULL,
    volume INTEGER NOT NULL
);
