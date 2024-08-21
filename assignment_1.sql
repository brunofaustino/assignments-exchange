DROP TABLE IF EXISTS currency_conversion_f;
DROP TABLE IF EXISTS currency_d;

--#####################################################################################################
-- 1. DDLs Creating Tables
--#####################################################################################################

CREATE TABLE currency_d (
    currency_key INT PRIMARY KEY,
    currency_code CHAR(3) NOT NULL,
    currency_name VARCHAR(200),
    currency_symbol CHAR(4));

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
    FOREIGN KEY (destination_currency_key) REFERENCES currency_d(currency_key));

--#####################################################################################################
-- 2. ETL Script for Loading CSV Data
--#####################################################################################################

\echo '================================================================================================================'
\echo 'Importing data from CSV files:\n'

COPY currency_d(currency_key, currency_code, currency_name, currency_symbol)
FROM '/data/currency_d.csv' DELIMITER ',' CSV HEADER;

COPY currency_conversion_f(conversion_date, source_currency_key, destination_currency_key,
                           source_destination_exchrate, destination_source_exchrate, source_destination_month_avg,
                           destination_source_month_avg, source_destination_year_avg,
                           destination_source_year_avg, exchgrates_source)
FROM '/data/currency_conversion_f.csv' DELIMITER ',' CSV HEADER;

--#####################################################################################################
-- 3.1 Given exchange rate from currency 1 to currency 2 must be calculated
-- and saved also the exchange rate from currency 2 to currency 1.
--#####################################################################################################

-- UPDATE currency_conversion_f
--   SET destination_source_exchrate = 1.0 / source_destination_exchrate
--    WHERE destination_source_exchrate IS NULL;

-- Saves data in all formats present in currency_conversion_F.
INSERT INTO currency_conversion_f (
       conversion_date,
       source_currency_key,
       destination_currency_key,
       source_destination_exchrate,
       destination_source_exchrate,
       source_destination_month_avg,
       destination_source_month_avg,
       source_destination_year_avg,
       destination_source_year_avg,
       exchgrates_source
)
SELECT
    conversion_date,
    destination_currency_key, -- reverse source and destination
    source_currency_key, -- reverse source and destination
    1.0 / source_destination_exchrate, -- reverse source and destination
    1.0 / destination_source_exchrate, -- reverse source and destination
    source_destination_month_avg,
    destination_source_month_avg,
    source_destination_year_avg,
    destination_source_year_avg,
    exchgrates_source
FROM currency_conversion_f;

------------------------------------------------------------------------------------
-- VALIDATION

\echo '================================================================================================================'
\echo 'Saved exchange rates from currency 1 to currency 2 and currency 2 to currency 1:\n'

SELECT * FROM currency_conversion_f LIMIT 100;

----------
-- We want to develop a SQL script which allows an external application to save in the table currency_conversion_F the
-- exchange rates from pound to €, and from $ to € (GBP->EUR e USD-> EUR)

INSERT INTO currency_conversion_f (
       conversion_date,
       source_currency_key,
       destination_currency_key,
       source_destination_exchrate,
       destination_source_exchrate,
       source_destination_month_avg,
       destination_source_month_avg,
       source_destination_year_avg,
       destination_source_year_avg,
       exchgrates_source
)
VALUES
    -- GBP (28) -> EUR (26) AND EUR (26) -> GBP (28)
    ('2024-08-20', 28, 26, 1.15, 1/1.15, NULL, NULL, NULL, NULL, 'EXTERNAL_APP'),
    ('2024-08-20', 26, 28, 1/1.15, 1.15, NULL, NULL, NULL, NULL, 'EXTERNAL_APP'),

    -- USD (72) -> EUR (26)
    ('2024-08-20', 72, 26, 0.85, 1/0.85, NULL, NULL, NULL, NULL, 'EXTERNAL_APP'),
    ('2024-08-20', 26, 72, 1.0/0.85, 0.85, NULL, NULL, NULL, NULL, 'EXTERNAL_APP');

------------------------------------------------------------------------------------
-- VALIDATION

\echo '================================================================================================================'
\echo 'Simulated the insertion of data from an external application, and exchange rates from pound to €, and from $ to € (GBP->EUR e USD-> EUR)\n'

SELECT * FROM currency_conversion_f WHERE exchgrates_source = 'EXTERNAL_APP' LIMIT 100;

--#####################################################################################################
-- 3.2 Given an amount, a date, and the pair (currency 1, currency 2), converts amount from currency 1
-- to currency 2, using the exchange rate, which is closer in time to date, independently if closer in
-- the past or in the future.
--#####################################################################################################

CREATE OR REPLACE FUNCTION convert_currency(
    amount INT,
    date DATE,
    source_currency INT,
    destination_currency INT
)
RETURNS NUMERIC AS $$
DECLARE
    closest_exchrate NUMERIC;
BEGIN
    -- Encontrar a taxa de câmbio mais próxima baseada na data fornecida
    SELECT
        source_destination_exchrate
    INTO closest_exchrate
    FROM currency_conversion_f
    WHERE source_currency_key = source_currency
      AND destination_currency_key = destination_currency
    ORDER BY ABS((conversion_date - date)) -- GET THE CLOSEST DATE
    LIMIT 1;

    RETURN amount * closest_exchrate;
END $$ LANGUAGE plpgsql;

-- Convert 100 EUR to USD on 2024-08-22
SELECT convert_currency(
       100,
       TO_DATE('2024-08-22', 'YYYY-MM-DD'),
       26,
       72
) AS converted_amount;

--------------------------------------------------------------------------------
-- VALIDATION

\echo '================================================================================================================'
\echo 'Check the exchange rate for EUR to USD on 2024-08-22 by manual calculation:\n'

-- Check the exchange rate for EUR to USD on 2024-08-22 by manual calculation
SELECT
    source_currency_key,
    destination_currency_key,
    source_destination_exchrate,
    destination_source_exchrate,
    convert_currency(
       100,
       TO_DATE('2019-12-02', 'YYYY-MM-DD'),
       26,
       72
    ) AS converted_amount,
    100 * source_destination_exchrate AS converted_amount_manual_check
FROM currency_conversion_f
WHERE conversion_date = '2019-12-02'
AND source_currency_key = 26
AND destination_currency_key = 72
LIMIT 10;

--#####################################################################################################
-- 3.3 In presence of DML operations on currency_conversion_F, updates mean monthly and mean yearly values of
-- exchange rates wherever present.|
--#####################################################################################################

-- Encapsulate the logic to update monthly and yearly averages in functions because we will use them multiple times
-- to update the monthly and yearly averages for both source > destination and destination > source exchange rates.
-- Function to update monthly averages
CREATE OR REPLACE FUNCTION update_monthly_avg_exchrate(
    p_exchrate_column TEXT,
    p_avg_column TEXT
)
RETURNS VOID AS $$
DECLARE
BEGIN
    -- Subquery to calculate monthly averages
    EXECUTE format('
        WITH monthly_avg AS (
            SELECT
                source_currency_key,
                destination_currency_key,
                EXTRACT(YEAR FROM conversion_date) AS year,
                EXTRACT(MONTH FROM conversion_date) AS month,
                AVG(%I) AS month_avg
            FROM currency_conversion_f
            GROUP BY
                source_currency_key,
                destination_currency_key,
                EXTRACT(YEAR FROM conversion_date),
                EXTRACT(MONTH FROM conversion_date)
        )
        UPDATE currency_conversion_f
        SET %I = monthly_avg.month_avg
        FROM monthly_avg
        WHERE currency_conversion_f.source_currency_key = monthly_avg.source_currency_key
          AND currency_conversion_f.destination_currency_key = monthly_avg.destination_currency_key
          AND EXTRACT(MONTH FROM currency_conversion_f.conversion_date) = monthly_avg.month
          AND EXTRACT(YEAR FROM currency_conversion_f.conversion_date) = monthly_avg.year;
    ', p_exchrate_column, p_avg_column);
END
$$ LANGUAGE plpgsql;

-- Function to update yearly averages
CREATE OR REPLACE FUNCTION update_yearly_avg_exchrate(
    p_exchrate_column TEXT,
    p_avg_column TEXT
)
RETURNS VOID AS $$
DECLARE
BEGIN
    -- Subquery to calculate monthly averages
    EXECUTE format('
        WITH yearly_avg AS (
            SELECT
                source_currency_key, destination_currency_key,
                EXTRACT(YEAR FROM conversion_date) AS year,
                AVG(%I) AS year_avg
            FROM currency_conversion_f
            GROUP BY
                source_currency_key,
                destination_currency_key,
                EXTRACT(YEAR FROM conversion_date)
        )
        UPDATE currency_conversion_f
        SET %I = yearly_avg.year_avg
        FROM yearly_avg
        WHERE currency_conversion_f.source_currency_key = yearly_avg.source_currency_key
          AND currency_conversion_f.destination_currency_key = yearly_avg.destination_currency_key
          AND EXTRACT(YEAR FROM currency_conversion_f.conversion_date) = yearly_avg.year;
    ', p_exchrate_column, p_avg_column);
END;
$$ LANGUAGE plpgsql;

-- SELECT update_yearly_avg_exchrate('source_destination_exchrate', 'source_destination_year_avg');
-- SELECT update_yearly_avg_exchrate('destination_source_exchrate', 'destination_source_year_avg');
-- SELECT update_monthly_avg_exchrate('source_destination_exchrate', 'source_destination_month_avg');
-- SELECT update_monthly_avg_exchrate('destination_source_exchrate', 'destination_source_month_avg');

-- drop FUNCTION update_avg_exchrate(TEXT, TEXT);

-- General function to update monthly and yearly averages
CREATE OR REPLACE FUNCTION update_avg_exchrate(
    p_period TEXT,
    p_type TEXT
)
RETURNS VOID AS $$
BEGIN
    IF p_period = 'month' THEN
        IF p_type = 'source_destination' THEN
            PERFORM update_monthly_avg_exchrate('source_destination_exchrate', 'source_destination_month_avg');
        ELSIF p_type = 'destination_source' THEN
            PERFORM update_monthly_avg_exchrate('destination_source_exchrate', 'destination_source_month_avg');
        ELSE
            RAISE EXCEPTION 'Invalid type. Use ''source_destination'' or ''destination_source''.';
        END IF;
    ELSIF p_period = 'year' THEN
        IF p_type = 'source_destination' THEN
            PERFORM update_yearly_avg_exchrate('source_destination_exchrate', 'source_destination_year_avg');
        ELSIF p_type = 'destination_source' THEN
            PERFORM update_yearly_avg_exchrate('destination_source_exchrate', 'destination_source_year_avg');
        ELSE
            RAISE EXCEPTION 'Invalid type. Use ''source_destination'' or ''destination_source''.';
        END IF;
    ELSE
        RAISE EXCEPTION 'Invalid period. Use ''month'' or ''year''.';
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Update yearly averages
SELECT update_avg_exchrate('year', 'source_destination');
SELECT update_avg_exchrate('year', 'destination_source');

-- Update monthly averages
SELECT update_avg_exchrate('month', 'source_destination');
SELECT update_avg_exchrate('month', 'destination_source');

--------------------------------------------------------------------------------
-- VALIDATION

\echo '================================================================================================================'
\echo 'CHECK MONTH: Check the monthly SOURCE > DESTINATION average for EUR to USD on 2019-12:\n'

-- CHECK MONTH: Check the monthly SOURCE > DESTINATION average for EUR to USD on 2019-12
SELECT
    source_currency_key,
    destination_currency_key,
    EXTRACT('MONTH' FROM conversion_date) AS month,
    EXTRACT('YEAR' FROM conversion_date) AS year,
    source_destination_month_avg,
    AVG(source_destination_exchrate) AS source_destination_month_avg_CHECK
FROM currency_conversion_f
WHERE EXTRACT('YEAR' FROM conversion_date) = 2019
AND EXTRACT('MONTH' FROM conversion_date) = 12
AND source_currency_key = 72
AND destination_currency_key = 26
GROUP BY
    source_currency_key,
    destination_currency_key,
    EXTRACT('MONTH' FROM conversion_date),
    EXTRACT('YEAR' FROM conversion_date),
    source_destination_month_avg;

\echo '================================================================================================================'
\echo 'CHECK MONTH: Check the monthly DESTINATION > SOURCE average for EUR to USD on 2019-12:\n'

-- CHECK MONTH: Check the monthly DESTINATION > SOURCE average for EUR to USD on 2019-12
SELECT
    source_currency_key,
    destination_currency_key,
    EXTRACT('MONTH' FROM conversion_date) AS month,
    EXTRACT('YEAR' FROM conversion_date) AS year,
    destination_source_exchrate,
    AVG(destination_source_exchrate) AS destination_source_month_avg_CHECK
FROM currency_conversion_f
WHERE EXTRACT('YEAR' FROM conversion_date) = 2019
AND EXTRACT('MONTH' FROM conversion_date) = 12
AND source_currency_key = 72
AND destination_currency_key = 26
GROUP BY
    source_currency_key,
    destination_currency_key,
    EXTRACT('MONTH' FROM conversion_date),
    EXTRACT('YEAR' FROM conversion_date),
    destination_source_exchrate;

\echo '================================================================================================================'
\echo 'CHECK YEAR: Check the yearly SOURCE > DESTINATION average for EUR to USD on 2019-12:\n'

-- CHECK YEAR: Check the yearly SOURCE > DESTINATION average for EUR to USD on 2019-12
SELECT
    source_currency_key,
    destination_currency_key,
    EXTRACT('YEAR' FROM conversion_date) AS year,
    source_destination_year_avg,
    AVG(source_destination_exchrate) AS source_destination_year_avg_CHECK
FROM currency_conversion_f
WHERE EXTRACT('YEAR' FROM conversion_date) = 2019
AND source_currency_key = 26
AND destination_currency_key = 72
GROUP BY
    source_currency_key,
    destination_currency_key,
    EXTRACT('YEAR' FROM conversion_date),
    source_destination_year_avg;


\echo '================================================================================================================'
\echo 'CHECK YEAR: Check the yearly DESTINATION > SOURCE average for EUR to USD on 2019-12:\n'

-- CHECK YEAR: Check the yearly DESTINATION > SOURCE average for EUR to USD on 2019-12
SELECT
    source_currency_key,
    destination_currency_key,
    EXTRACT('YEAR' FROM conversion_date) AS year,
    destination_source_exchrate,
    AVG(destination_source_exchrate) AS destination_source_year_avg_CHECK
FROM currency_conversion_f
WHERE EXTRACT('YEAR' FROM conversion_date) = 2019
AND source_currency_key = 72
AND destination_currency_key = 26
GROUP BY
    source_currency_key,
    destination_currency_key,
    EXTRACT('YEAR' FROM conversion_date),
    destination_source_exchrate;


\echo '================================================================================================================'
\echo 'Showing final results:\n'

SELECT * FROM currency_conversion_f LIMIT 5;

\echo 'Execution completed.'