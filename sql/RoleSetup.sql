USE MARKET.NATIONALMARKET;
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE SYS_WH;
-- Create roles for each module
CREATE ROLE IF NOT EXISTS ProductManager;
CREATE ROLE IF NOT EXISTS StoreManager;
CREATE ROLE IF NOT EXISTS SalesManager;
-- Grant privileges to ProductManager role
GRANT
SELECT,
INSERT,
UPDATE,
    DELETE ON TABLE MARKET.NATIONALMARKET.PRODUCT TO ROLE ProductManager;
GRANT
SELECT,
INSERT,
UPDATE,
    DELETE ON TABLE MARKET.NATIONALMARKET.PRODUCT_TEMP TO ROLE ProductManager;
GRANT READ,
    WRITE ON STAGE MARKET.NATIONALMARKET.STAGE_PRODUCTS TO ROLE ProductManager;
GRANT READ,
    WRITE ON STAGE MARKET.NATIONALMARKET.STAGE_HISTORIC_PRODUCTS TO ROLE ProductManager;
GRANT USAGE ON PROCEDURE MARKET.NATIONALMARKET.LOAD_PRODUCTS_FROM_STAGE() TO ROLE ProductManager;
GRANT USAGE ON PROCEDURE MARKET.NATIONALMARKET.COPY_PRODUCTS() TO ROLE ProductManager;
GRANT USAGE ON PROCEDURE MARKET.NATIONALMARKET.REMOVE_FILE_PRODUCTS() TO ROLE ProductManager;
GRANT OPERATE ON TASK MARKET.NATIONALMARKET.BULK_LOAD_PRODUCTS TO ROLE ProductManager;
-- Grant privileges to StoreManager role
    GRANT
SELECT,
INSERT,
UPDATE,
    DELETE ON TABLE MARKET.NATIONALMARKET.STORE TO ROLE StoreManager;
GRANT
SELECT,
INSERT,
UPDATE,
    DELETE ON TABLE MARKET.NATIONALMARKET.STORE_TEMP TO ROLE StoreManager;
GRANT READ,
    WRITE ON STAGE MARKET.NATIONALMARKET.STAGE_STORES TO ROLE StoreManager;
GRANT READ,
    WRITE ON STAGE MARKET.NATIONALMARKET.STAGE_HISTORIC_STORES TO ROLE StoreManager;
GRANT USAGE ON PROCEDURE MARKET.NATIONALMARKET.LOAD_STORES_FROM_STAGE() TO ROLE StoreManager;
GRANT USAGE ON PROCEDURE MARKET.NATIONALMARKET.COPY_STORES() TO ROLE StoreManager;
GRANT USAGE ON PROCEDURE MARKET.NATIONALMARKET.REMOVE_FILE_STORES() TO ROLE StoreManager;
GRANT OPERATE ON TASK MARKET.NATIONALMARKET.BULK_LOAD_STORES TO ROLE StoreManager;
-- Grant privileges to SalesManager role
    GRANT
SELECT,
INSERT,
UPDATE,
    DELETE ON TABLE MARKET.NATIONALMARKET.SALES TO ROLE SalesManager;
GRANT
SELECT,
INSERT,
UPDATE,
    DELETE ON TABLE MARKET.NATIONALMARKET.SALES_TEMP TO ROLE SalesManager;
GRANT READ,
    WRITE ON STAGE MARKET.NATIONALMARKET.STAGE_SALES TO ROLE SalesManager;
GRANT READ,
    WRITE ON STAGE MARKET.NATIONALMARKET.STAGE_HISTORIC_SALES TO ROLE SalesManager;
GRANT USAGE ON PROCEDURE MARKET.NATIONALMARKET.LOAD_SALES_FROM_STAGE() TO ROLE SalesManager;
GRANT USAGE ON PROCEDURE MARKET.NATIONALMARKET.COPY_SALES() TO ROLE SalesManager;
GRANT USAGE ON PROCEDURE MARKET.NATIONALMARKET.REMOVE_FILE_SALES() TO ROLE SalesManager;
GRANT OPERATE ON TASK MARKET.NATIONALMARKET.BULK_LOAD_SALES TO ROLE SalesManager;
-- Create users and assign roles
    CREATE USER IF NOT EXISTS apalaciosprm PASSWORD = '.Goldco2024' DEFAULT_ROLE = ProductManager;
CREATE USER IF NOT EXISTS apalaciosstm PASSWORD = '.Goldco2024' DEFAULT_ROLE = StoreManager;
CREATE USER IF NOT EXISTS apalaciosslm PASSWORD = '.Goldco2024' DEFAULT_ROLE = SalesManager;
-- Assign roles to users
    GRANT ROLE ProductManager TO USER apalaciosprm;
GRANT ROLE StoreManager TO USER apalaciosstm;
GRANT ROLE SalesManager TO USER apalaciosslm;
-- Optional: Set the default warehouse for the users
    ALTER USER apalaciosprm
SET
    DEFAULT_WAREHOUSE = 'SYS_WH';
ALTER USER apalaciosstm
SET
    DEFAULT_WAREHOUSE = 'SYS_WH';
ALTER USER apalaciosslm
SET
    DEFAULT_WAREHOUSE = 'SLM_WH';
-- Optional: Set the default database and schema for the users
    ALTER USER apalaciosprm
SET
    DEFAULT_NAMESPACE = 'MARKET.NATIONALMARKET';
ALTER USER apalaciosstm
SET
    DEFAULT_NAMESPACE = 'MARKET.NATIONALMARKET';
ALTER USER apalaciosslm
SET
    DEFAULT_NAMESPACE = 'MARKET.NATIONALMARKET';
