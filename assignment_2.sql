DROP TABLE IF EXISTS stg_domain;
DROP TABLE IF EXISTS stg_hosting;
DROP TABLE IF EXISTS stg_invoice;

DROP TABLE IF EXISTS domain;
DROP TABLE IF EXISTS hosting;
DROP TABLE IF EXISTS invoice;

--#####################################################################################################
-- 1. DDLs Creating Tables
--#####################################################################################################

CREATE TABLE domain (
    id INT,
    userid INT NOT NULL,
    type VARCHAR(25),
    registrationdate DATE,
    domain VARCHAR(250),
    status VARCHAR(20),
    nextduedate DATE,
    PRIMARY KEY (id)
);

CREATE TABLE hosting (
    id INT,
    userid INT NOT NULL,
    packageid INT,
    regdate DATE,
    domain VARCHAR(250),
    domainstatus VARCHAR(50),
    nextduedate DATE,
    PRIMARY KEY (id, userid)
);

CREATE TABLE invoice (
    invoiceid INT,
    userid INT NOT NULL,
    type VARCHAR(50),
    relid INT,
    description VARCHAR(500),
    amount NUMERIC,
    duedate DATE,
    invoice_label VARCHAR(50)
);

CREATE TABLE stg_domain (
    id INT,
    userid INT NOT NULL,
    type VARCHAR(25),
    registrationdate VARCHAR(50),
    domain VARCHAR(250),
    status VARCHAR(20),
    nextduedate VARCHAR(50)
);

CREATE TABLE stg_hosting (
    id INT,
    userid INT NOT NULL,
    packageid INT,
    regdate VARCHAR(50),
    domain VARCHAR(250),
    domainstatus VARCHAR(50),
    nextduedate VARCHAR(50)
);

CREATE TABLE stg_invoice (
    invoiceid INT,
    userid INT NOT NULL,
    type VARCHAR(50),
    relid INT,
    description VARCHAR(500),
    amount NUMERIC,
    duedate VARCHAR(500),
    invoice_label VARCHAR(50)
);

--#####################################################################################################
-- 2. ETL Script for Loading CSV Data
--#####################################################################################################

\echo '================================================================================================================'
\echo 'Importing data from CSV files:\n'

COPY stg_domain(id, userid, type, registrationdate, domain, status, nextduedate)
FROM '/data/domain.csv' DELIMITER ',' CSV HEADER;

COPY stg_hosting(id, userid, packageid, regdate, domain, domainstatus, nextduedate)
FROM '/data/hosting.csv' DELIMITER ',' CSV HEADER;

COPY stg_invoice(invoiceid, userid, type, relid, description, amount, duedate, invoice_label)
FROM '/data/invoice.csv' DELIMITER ',' CSV HEADER;

---------

-- Convert date columns to a more appropriate data type
UPDATE stg_domain
SET registrationdate = TO_TIMESTAMP(registrationdate, 'DD/MM/YYYY HH24:MI:SS')::DATE,
    nextduedate = TO_TIMESTAMP(nextduedate, 'DD/MM/YYYY HH24:MI:SS')::DATE;

ALTER TABLE stg_domain ALTER COLUMN registrationdate TYPE DATE USING registrationdate::date;
ALTER TABLE stg_domain ALTER COLUMN nextduedate TYPE DATE USING nextduedate::date;

UPDATE stg_hosting
    SET regdate = TO_TIMESTAMP(regdate, 'DD/MM/YYYY HH24:MI:SS')::DATE,
    nextduedate = TO_TIMESTAMP(nextduedate, 'DD/MM/YYYY HH24:MI:SS')::DATE;

ALTER TABLE stg_hosting ALTER COLUMN regdate TYPE DATE USING regdate::date;
ALTER TABLE stg_hosting ALTER COLUMN nextduedate TYPE DATE USING nextduedate::date;

UPDATE stg_invoice
    SET duedate = TO_TIMESTAMP(duedate, 'DD/MM/YYYY HH24:MI:SS')::DATE;

ALTER TABLE stg_invoice ALTER COLUMN duedate TYPE DATE USING duedate::date;

-------

INSERT INTO domain (id, userid, type, registrationdate, domain, status, nextduedate)
SELECT id, userid, type, registrationdate, domain, status, nextduedate
FROM stg_domain;

INSERT INTO hosting (id, userid, packageid, regdate, domain, domainstatus, nextduedate)
SELECT id, userid, packageid, regdate, domain, domainstatus, nextduedate
FROM stg_hosting;

ALTER TABLE invoice ADD COLUMN purchase_type VARCHAR(10);
ALTER TABLE stg_invoice ADD COLUMN purchase_type VARCHAR(10);

INSERT INTO invoice (invoiceid, userid, type, relid, description, amount, duedate, invoice_label, purchase_type)
SELECT invoiceid, userid, type, relid, description, amount, duedate, invoice_label, purchase_type
FROM stg_invoice;

-------

-- 2513
-- SELECT COUNT(*) FROM domain;
-- 1955
-- SELECT COUNT(*) FROM hosting;
-- 51371
-- SELECT COUNT(*) FROM invoice;

-----

-- domain ID is unique
-- 2513
-- SELECT COUNT(DISTINCT id) FROM domain;
-- 2513
-- SELECT COUNT(id) FROM domain;


-------

-- Users have multiple domains
-- 2513
-- SELECT COUNT(*) FROM domain;
-- 1643
-- SELECT COUNT(DISTINCT userid) FROM domain;

-------

-- 1955
--nSELECT COUNT(*) FROM hosting;
-- 1762
-- SELECT COUNT(DISTINCT userid) FROM hosting;
-------

--#####################################################################################################
-- Identifying Relationships and Calculating Likelihood
--#####################################################################################################

\echo '------------------------------------------------------------------------------------------\n'
\echo 'Calculating the likelihood of relationships between tables:\n'

-- Despite I found some high percentage of matching, I was not able to find a relationship between the tables after
-- creating PRIMARY KEYS and FOREIGN KEYS. I tried to insert the data into the tables, but all the combinations
-- broke the referential integrity, ....
WITH  percentage as (
    SELECT
        'invoice.relid <> hosting.id' AS relation,
        COUNT(*) AS matching_count,
        COUNT(*)::FLOAT / (SELECT COUNT(*) FROM invoice) * 100.0 AS percentage
    FROM invoice i JOIN hosting h ON i.relid = h.id
    UNION ALL
    SELECT
        'invoice.relid <> hosting.userid' AS relation,
        COUNT(*) AS matching_count,
        COUNT(*)::FLOAT / (SELECT COUNT(*) FROM invoice) * 100.0 AS percentage
    FROM invoice i JOIN hosting h ON i.relid = h.userid
    UNION ALL
    SELECT
        'invoice.relid <> hosting.packageid' AS relation,
        COUNT(*) AS matching_count,
        COUNT(*)::FLOAT / (SELECT COUNT(*) FROM invoice) * 100.0 AS percentage
    FROM invoice i JOIN hosting h ON i.relid = h.packageid
    UNION ALL

    -- INVOCE.invoiceid = HOSTING.ID,
    -- INVOCE.invoiceid = HOSTING.USERID
    -- INVOCE.invoiceid = HOSTING.PACKAGEID
    SELECT
        'invoice.invoiceid <> hosting.id' AS relation,
        COUNT(*) AS matching_count,
        COUNT(*)::FLOAT / (SELECT COUNT(*) FROM invoice) * 100.0 AS percentage
    FROM invoice i JOIN hosting h ON i.invoiceid = h.id
    UNION ALL
    SELECT
        'invoice.invoiceid <> hosting.userid' AS relation,
        COUNT(*) AS matching_count,
        COUNT(*)::FLOAT / (SELECT COUNT(*) FROM invoice) * 100.0 AS percentage
    FROM invoice i JOIN hosting h ON i.invoiceid = h.userid
    UNION ALL
    SELECT
        'invoice.invoiceid <> hosting.packageid' AS relation,
        COUNT(*) AS matching_count,
        COUNT(*)::FLOAT / (SELECT COUNT(*) FROM invoice) * 100.0 AS percentage
    FROM invoice i JOIN hosting h ON i.invoiceid = h.packageid
    UNION ALL

    -- INVOCE.userid = HOSTING.ID,
    -- INVOCE.userid = HOSTING.USERID
    -- INVOCE.userid = HOSTING.PACKAGEID
    SELECT
        'invoice.userid <> hosting.id' AS relation,
        COUNT(*) AS matching_count,
        COUNT(*)::FLOAT / (SELECT COUNT(*) FROM invoice) * 100.0 AS percentage
    FROM invoice i JOIN hosting h ON i.userid = h.id
    UNION ALL
    SELECT
        'invoice.userid <> hosting.userid' AS relation,
        COUNT(*) AS matching_count,
        COUNT(*)::FLOAT / (SELECT COUNT(*) FROM invoice) * 100.0 AS percentage
    FROM invoice i JOIN hosting h ON i.userid = h.userid
    UNION ALL
    SELECT
        'invoice.userid <> hosting.packageid' AS relation,
        COUNT(*) AS matching_count,
        COUNT(*)::FLOAT / (SELECT COUNT(*) FROM invoice) * 100.0 AS percentage
    FROM invoice i JOIN hosting h ON i.userid = h.packageid
    UNION ALL

    -- >>>> DOMAIN
    -- > HOSTING (RELID, invoiceid, userid) X DOMAIN (ID, USERID)

    -- INVOCE.RELID = DOMAIN.ID,
    -- INVOCE.REDID = DOMAIN.USERID
    SELECT
        'invoice.relid <> domain.id' AS relation,
        COUNT(*) AS matching_count,
        COUNT(*)::FLOAT / (SELECT COUNT(*) FROM invoice) * 100.0 AS percentage
    FROM invoice i JOIN domain d ON i.relid = d.id
    UNION ALL
    SELECT
        'invoice.relid <> domain.USERID' AS relation,
        COUNT(*) AS matching_count,
        COUNT(*)::FLOAT / (SELECT COUNT(*) FROM invoice) * 100.0 AS percentage
    FROM invoice i JOIN domain d ON i.relid = d.USERID
    UNION ALL

    -- INVOCE.invoiceid = DOMAIN.ID,
    -- INVOCE.invoiceid = DOMAIN.USERID
    SELECT
        'invoice.invoiceid <> domain.id' AS relation,
        COUNT(*) AS matching_count,
        COUNT(*)::FLOAT / (SELECT COUNT(*) FROM invoice) * 100.0 AS percentage
    FROM invoice i JOIN domain d ON i.invoiceid = d.id
    UNION ALL
    SELECT
        'invoice.invoiceid <> domain.USERID' AS relation,
        COUNT(*) AS matching_count,
        COUNT(*)::FLOAT / (SELECT COUNT(*) FROM invoice) * 100.0 AS percentage
    FROM invoice i JOIN domain d ON i.invoiceid = d.USERID
    UNION ALL

    -- INVOCE.invoiceid = DOMAIN.ID,
    -- INVOCE.invoiceid = DOMAIN.USERID
    SELECT
        'invoice.userid <> domain.id' AS relation,
        COUNT(*) AS matching_count,
        COUNT(*)::FLOAT / (SELECT COUNT(*) FROM invoice) * 100.0 AS percentage
    FROM invoice i JOIN domain d ON i.userid = d.id
    UNION ALL
    SELECT
        'invoice.userid <> domain.USERID' AS relation,
        COUNT(*) AS matching_count,
        COUNT(*)::FLOAT / (SELECT COUNT(*) FROM invoice) * 100.0 AS percentage
    FROM invoice i JOIN domain d ON i.userid = d.USERID
    UNION ALL
    -----
    -- INVOICE.RELID + INVOICE.USERID = HOSTING.ID + HOSTING.USERID
    SELECT
        'invoice.relid + userid <> hosting.id + userid' AS relation,
        COUNT(*) AS matching_count,
        COUNT(*)::FLOAT / (SELECT COUNT(*) FROM invoice) * 100.0 AS percentage
    FROM invoice i
    JOIN hosting h ON i.relid = h.id AND i.userid = h.userid
    UNION ALL

    -- INVOICE.INVOICEID + INVOICE.USERID = HOSTING.ID + HOSTING.USERID
    SELECT
        'invoice.invoiceid + userid <> hosting.id + userid' AS relation,
        COUNT(*) AS matching_count,
        COUNT(*)::FLOAT / (SELECT COUNT(*) FROM invoice) * 100.0 AS percentage
    FROM invoice i
    JOIN hosting h ON i.invoiceid = h.id AND i.userid = h.userid
    UNION ALL


    -- INVOICE.USERID + INVOICE.RELID = DOMAIN.ID + DOMAIN.USERID
    SELECT
        'invoice.userid + relid <> domain.id + userid' AS relation,
        COUNT(*) AS matching_count,
        COUNT(*)::FLOAT / (SELECT COUNT(*) FROM invoice) * 100.0 AS percentage
    FROM invoice i
    JOIN domain d ON i.userid = d.userid AND i.relid = d.id
    UNION ALL

    -- INVOICE.USERID + INVOICE.RELID = DOMAIN.USERID + DOMAIN.ID
    SELECT
        'invoice.userid + relid <> domain.userid + id' AS relation,
        COUNT(*) AS matching_count,
        COUNT(*)::FLOAT / (SELECT COUNT(*) FROM invoice) * 100.0 AS percentage
    FROM invoice i
    JOIN domain d ON i.userid = d.id AND i.relid = d.userid
    UNION ALL

    -- INVOICE.INVOICEID + INVOICE.RELID = DOMAIN.ID + DOMAIN.USERID
    SELECT
        'invoice.invoiceid + relid <> domain.id + userid' AS relation,
        COUNT(*) AS matching_count,
        COUNT(*)::FLOAT / (SELECT COUNT(*) FROM invoice) * 100.0 AS percentage
    FROM invoice i
    JOIN domain d ON i.invoiceid = d.id AND i.relid = d.userid
    UNION ALL

    -- INVOICE.INVOICEID + INVOICE.USERID = DOMAIN.ID + DOMAIN.RELID
    SELECT
        'invoice.invoiceid + userid <> domain.id + relid' AS relation,
        COUNT(*) AS matching_count,
        COUNT(*)::FLOAT / (SELECT COUNT(*) FROM invoice) * 100.0 AS percentage
    FROM invoice i
    JOIN domain d ON i.invoiceid = d.userid AND i.userid = d.id
    UNION ALL
    -- INVOICE.USERID + INVOICE.INVOICEID = DOMAIN.ID + DOMAIN.RELID
    SELECT
        'invoice.userid + invoiceid <> domain.id + relid' AS relation,
        COUNT(*) AS matching_count,
        COUNT(*)::FLOAT / (SELECT COUNT(*) FROM invoice) * 100.0 AS percentage
    FROM invoice i
    JOIN domain d ON i.userid = d.userid AND i.invoiceid = d.id
    --------------------------------------------
    union all
    -- Comparações entre HOSTING e DOMAIN

    -- HOSTING.ID = DOMAIN.ID
    SELECT
        'hosting.id <> domain.id' AS relation,
        COUNT(*) AS matching_count,
        COUNT(*)::FLOAT / (SELECT COUNT(*) FROM hosting) * 100.0 AS percentage
    FROM hosting h
    JOIN domain d ON h.id = d.id
    UNION ALL

    -- HOSTING.ID = DOMAIN.USERID
    SELECT
        'hosting.id <> domain.userid' AS relation,
        COUNT(*) AS matching_count,
        COUNT(*)::FLOAT / (SELECT COUNT(*) FROM hosting) * 100.0 AS percentage
    FROM hosting h
    JOIN domain d ON h.id = d.userid
    UNION ALL

    -- HOSTING.USERID = DOMAIN.ID
    SELECT
        'hosting.userid <> domain.id' AS relation,
        COUNT(*) AS matching_count,
        COUNT(*)::FLOAT / (SELECT COUNT(*) FROM hosting) * 100.0 AS percentage
    FROM hosting h
    JOIN domain d ON h.userid = d.id
    UNION ALL

    -- HOSTING.USERID = DOMAIN.USERID
    SELECT
        'hosting.userid <> domain.userid' AS relation,
        COUNT(*) AS matching_count,
        COUNT(*)::FLOAT / (SELECT COUNT(*) FROM hosting) * 100.0 AS percentage
    FROM hosting h
    JOIN domain d ON h.userid = d.userid
    UNION ALL

    -- HOSTING.PACKAGEID = DOMAIN.ID
    SELECT
        'hosting.packageid <> domain.id' AS relation,
        COUNT(*) AS matching_count,
        COUNT(*)::FLOAT / (SELECT COUNT(*) FROM hosting) * 100.0 AS percentage
    FROM hosting h
    JOIN domain d ON h.packageid = d.id
    UNION ALL

    -- HOSTING.PACKAGEID = DOMAIN.USERID
    SELECT
        'hosting.packageid <> domain.userid' AS relation,
        COUNT(*) AS matching_count,
        COUNT(*)::FLOAT / (SELECT COUNT(*) FROM hosting) * 100.0 AS percentage
    FROM hosting h
    JOIN domain d ON h.packageid = d.userid
)
SELECT * FROM percentage ORDER BY percentage DESC;

-------

-- domain + hosting
--SELECT
--   d.id AS domain_id,
--    d.userid AS domain_userid,
--    d.type AS domain_type,
--    d.domain AS domain_name,
--    d.registrationdate AS domain_registrationdate,
--    d.status AS domain_status,
--    d.nextduedate AS domain_nextduedate,
--    h.id AS hosting_id,
--    h.packageid AS hosting_packageid,
--    h.regdate AS hosting_regdate,
--    h.domain AS hosting_domain,
--    h.domainstatus AS hosting_domainstatus,
--    h.nextduedate AS hosting_nextduedate
--FROM domain d
--JOIN hosting h ON d.userid = h.userid
--LIMIT 2;

-- domain + invoice
--SELECT
--    d.id AS domain_id,
--    d.userid AS domain_userid,
--    d.type AS domain_type,
--    d.domain AS domain_name,
--    d.registrationdate AS domain_registrationdate,
--    d.status AS domain_status,
--    d.nextduedate AS domain_nextduedate,
--   i.invoiceid AS invoice_id,
--    i.type AS invoice_type,
--    i.relid AS invoice_relid,
--    i.description AS invoice_description,
--    i.amount AS invoice_amount,
--    i.duedate AS invoice_duedate,
--    i.invoice_label AS invoice_label
--FROM domain d
--JOIN invoice i ON d.userid = i.userid
--LIMIT 2;

-----

-- We have more than one invoice can be emitted for a userid
--SELECT
--   userid,
--    COUNT(*) AS domain_
--FROM invoice
--WHERE
--   EXTRACT('YEAR' FROM duedate) = 2018
--    and EXTRACT('MONTH' FROM duedate) = 8
--GROUP BY userid ORDER BY COUNT(*) DESC
--LIMIT 10;

--
--SELECT * FROM invoice
--WHERE EXTRACT('YEAR' FROM duedate) = 2018
--and EXTRACT('MONTH' FROM duedate) = 8 AND userid = 74861
--LIMIT 10;

--#####################################################################################################
-- Order, from most to least recent, invoices for each product, labelling them as "new" or "renew" (where the
-- first purchase, given the customer and the product, will be flagged as "new"). The label should be added in
-- the original database.
--#####################################################################################################

\echo '================================================================================================================'
\echo 'New purchases:\n'

WITH ordered_invoices AS (
    SELECT
        invoiceid,
        userid,
        relid,
        type,
        description,
        amount,
        duedate,
        ROW_NUMBER() OVER (PARTITION BY userid, relid ORDER BY duedate ASC) AS p_order
    FROM invoice
)
UPDATE invoice
SET purchase_type = CASE
                        WHEN p_order = 1 THEN 'new'
                        ELSE 'renew'
                    END
FROM ordered_invoices oi
WHERE invoice.invoiceid = oi.invoiceid
AND invoice.userid = oi.userid
AND invoice.relid = oi.relid
AND invoice.type = oi.type
AND invoice.description = oi.description;

-------------------------------------------------------------------------------------------------------
-- VALIDATION
SELECT * FROM invoice WHERE userid = 92964 ORDER BY duedate LIMIT 10;

--#####################################################################################################
-- Produce a report with evidence of the revenue referring to invoices expiring on 01/10/2019, broken down
-- into product type, only for products with status = "Active" .
-- Provide:
-- • The SQL code to generate the report or a screenshot of report and detailed instruction to connect to
-- data and run the report with chosen tool.
--#####################################################################################################

\echo '================================================================================================================'
\echo 'Calculating the total revenue for active products on 01/10/2019:\n'

WITH active_products AS (
    SELECT
        i.invoiceid,
        d.userid,
        d.type AS product_type,
        d.domain AS product_name,
        i.duedate,
        i.amount,
        'domain' AS source_table
    FROM invoice i
    JOIN domain d on i.userid = d.userid
    WHERE d.status = 'Active'
)
SELECT
    product_type,
    SUM(amount) AS total_revenue,
    COUNT(invoiceid) AS total_invoices
FROM active_products
WHERE duedate = '2019-10-01'
GROUP BY product_type
ORDER BY total_revenue DESC
LIMIT 10;

\echo 'Execution completed.'