
/*
==============================================================================
Quality Checks
==============================================================================
Script Purpose:
    This script performs quality checks to validate the Integrity, consistency,
    and accuracy of the Gold Layer. These Checks ensure:
      - Uniqueness of surrogate keys in dimension table.
      - Referenctial Integrity between fact and dimension table.
      - Validation of relationships in the date model found during the checks.

Usage Note:
      - Run these Checks after data loading Silver Layer.
      - Invesestigate and resolve any discrepancies found during the checks.
===============================================================================
*/

--=============================================================================
--checking 'gold.dim_customers'
--=============================================================================
--Check for Uniqueness of customer_key in gold.dim_customers
-- Expectation: No results
SELECT
  customer_key,
  COUNT(*) AS duplicate_count
FROM gold.dim.customers
GROUP BY customer_key
HAVING COUNT(*) >1;

--=============================================================================
--checking 'gold.dim_customers'
--=============================================================================
--Check for Uniqueness of product_key in gold.dim_products
-- Expectation: No results
SELECT
  prduct_key,
  COUNT(*) AS duplicate_count
FROM gold.dim.products
GROUP BY product_key
HAVING COUNT(*) >1;

--=============================================================================
--Checking 'gold.fact_sales'
--=============================================================================
-- Check the data model connectivity beteween fact and dimensions
SELECT *
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
  ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
  ON p.product_key = f.product_key
WHERE p.product_key IS NULL OR customer_key IS NULL
