USE DATABASE CUSTOMER_ANALYTICS;

USE SCHEMA DQ;

--CUSTOMERS TABLE CHECKS
-- 1. Null ID check
INSERT INTO dq_results
SELECT 'customers_clean', 'null_id_check', COUNT(*), CURRENT_TIMESTAMP
FROM staging.customers_clean
WHERE ID IS NULL;

-- 2. Duplicate ID check
INSERT INTO dq_results
SELECT 'customers_clean', 'duplicate_id_check', COUNT(*), CURRENT_TIMESTAMP
FROM (
    SELECT ID
    FROM staging.customers_clean
    GROUP BY ID
    HAVING COUNT(*) > 1
);

-- 3. Invalid email check
INSERT INTO dq_results
SELECT 'customers_clean', 'invalid_email_check', COUNT(*), CURRENT_TIMESTAMP
FROM staging.customers_clean
WHERE EMAIL NOT LIKE '%@%.%';

-- 4. Invalid date of birth (failed conversion)
INSERT INTO dq_results
SELECT 'customers_clean', 'invalid_dob_check', COUNT(*), CURRENT_TIMESTAMP
FROM staging.customers_clean
WHERE dateofbirth IS NULL;

-- 5. Unknown country check
INSERT INTO dq_results
SELECT 'customers_clean', 'unknown_country_check', COUNT(*), CURRENT_TIMESTAMP
FROM staging.customers_clean
WHERE COUNTRY = 'Unknown';

--6. Missing timestamps check
INSERT INTO dq_results
SELECT 'customers_clean', 'null_timestamp_check', COUNT(*), CURRENT_TIMESTAMP
FROM staging.customers_clean
WHERE created_at IS NULL OR updated_at IS NULL;

--PRODUCTS TABLE CHECKS
-- 7. Null ID check
INSERT INTO dq_results
SELECT 'products_clean', 'null_id_check', COUNT(*), CURRENT_TIMESTAMP
FROM staging.products_clean
WHERE ID IS NULL;

-- 8. Duplicate ID check
INSERT INTO dq_results
SELECT 'products_clean', 'duplicate_id_check', COUNT(*), CURRENT_TIMESTAMP
FROM (
    SELECT ID
    FROM staging.products_clean
    GROUP BY ID
    HAVING COUNT(*) > 1
);

-- 9. Invalid price check
INSERT INTO dq_results
SELECT 'products_clean', 'invalid_price_check', COUNT(*), CURRENT_TIMESTAMP
FROM staging.products_clean
WHERE PRICE <= 0;

-- 10. Missing timestamps check
INSERT INTO dq_results
SELECT 'products_clean', 'null_timestamp_check', COUNT(*), CURRENT_TIMESTAMP
FROM staging.products_clean
WHERE created_at IS NULL OR updated_at IS NULL;

--COUNTRY TABLE CHECKS
-- 11. Null CODE check
INSERT INTO dq_results
SELECT 'country_clean', 'null_code_check', COUNT(*), CURRENT_TIMESTAMP
FROM staging.country_clean
WHERE CODE IS NULL;

-- 12. Duplicate CODE check
INSERT INTO dq_results
SELECT 'country_clean', 'duplicate_code_check', COUNT(*), CURRENT_TIMESTAMP
FROM (
    SELECT CODE
    FROM staging.country_clean
    GROUP BY CODE
    HAVING COUNT(*) > 1
);

--SALES TABLE CHECKS (MOST IMPORTANT)

-- 13. Invalid customer foreign key
INSERT INTO dq_results
SELECT 'sales_clean', 'invalid_customer_fk', COUNT(*), CURRENT_TIMESTAMP
FROM staging.sales_clean s
LEFT JOIN staging.customers_clean c 
ON s.CUSTOMER_ID = c.ID
WHERE c.ID IS NULL;

-- 14. Invalid product foreign key
INSERT INTO dq_results
SELECT 'sales_clean', 'invalid_product_fk', COUNT(*), CURRENT_TIMESTAMP
FROM staging.sales_clean s
LEFT JOIN staging.products_clean p
ON s.PRODUCT_ID = p.ID
WHERE p.ID IS NULL;

-- 15. Negative quantity check
INSERT INTO dq_results
SELECT 'sales_clean', 'negative_quantity_check', COUNT(*), CURRENT_TIMESTAMP
FROM staging.sales_clean
WHERE QUANTITY < 0;

-- 16. Negative revenue check
INSERT INTO dq_results
SELECT 'sales_clean', 'negative_revenue_check', COUNT(*), CURRENT_TIMESTAMP
FROM staging.sales_clean
WHERE TOTAL_AMOUNT < 0;

-- 17. Null sales date check
INSERT INTO dq_results
SELECT 'sales_clean', 'null_sales_date_check', COUNT(*), CURRENT_TIMESTAMP
FROM staging.sales_clean
WHERE SALES_DATE IS NULL;

--18 Missing timestamps check
INSERT INTO dq_results
SELECT 'sales_clean', 'null_timestamp_check', COUNT(*), CURRENT_TIMESTAMP
FROM staging.sales_clean
WHERE created_at IS NULL OR updated_at IS NULL;