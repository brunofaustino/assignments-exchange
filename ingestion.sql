-- 1. DDL Tables

-- Create currency_d table
CREATE TABLE currency_d (
    currency_key INT PRIMARY KEY,
    currency_code CHAR(3) NOT NULL,
    currency_name VARCHAR(200),
    currency_symbol CHAR(4)
);

-- Create currency_conversion_f table
CREATE TABLE currency_conversion_f (
    conversion_date DATE NOT NULL,
    source_currency_key INT NOT NULL,
    destination_currency_key INT NOT NULL,
    source_destination_exchrate NUMERIC,
    destination_source_exchrate NUMERIC,
    source_destination_month_avg NUMERIC,
    destination_source_month_avg NUMERIC,
    source_destination_year_avg NUMERIC,
    destination_source_year_avg NUMERIC,
    exchgrates_source VARCHAR(255),
    PRIMARY KEY (conversion_date, source_currency_key, destination_currency_key),
    FOREIGN KEY (source_currency_key) REFERENCES currency_d(currency_key),
    FOREIGN KEY (destination_currency_key) REFERENCES currency_d(currency_key)
);

--------------------------------------------------------------------------------
-- 2. ETL Script for Loading CSV Data

-- load currency_d
COPY currency_d(currency_key, currency_code, currency_name, currency_symbol)
FROM 'data/currency_d.csv' DELIMITER ',' CSV HEADER;

-- currency_conversion_f
COPY currency_conversion_f(conversion_date, source_currency_key, destination_currency_key, source_destination_exchrate, destination_source_exchrate, source_destination_month_avg, destination_source_month_avg, source_destination_year_avg, destination_source_year_avg, exchgrates_source)
FROM 'data/currency_conversion_f.csv' DELIMITER ',' CSV HEADER;
