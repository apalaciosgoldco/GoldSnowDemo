USE MARKET.NATIONALMARKET;
USE ROLE SYSADMIN;
USE WAREHOUSE SYS_WH;

CREATE OR REPLACE PROCEDURE MARKET.NATIONALMARKET.LOAD_PRODUCTS_FROM_STAGE()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS 
$$
DECLARE
    stage_name STRING DEFAULT 'STAGE_PRODUCTS';
    file_format STRING DEFAULT 'MARKET_CSV_FF';
    load_cmd STRING;
    merge_cmd STRING;
    truncate_cmd STRING;

BEGIN
    -- Truncate the staging table before loading new data
    truncate_cmd := 'TRUNCATE TABLE MARKET.NATIONALMARKET.PRODUCT_TEMP';
    EXECUTE IMMEDIATE truncate_cmd;

    -- Load data into temporary stage table
    load_cmd := 'COPY INTO MARKET.NATIONALMARKET.PRODUCT_TEMP
                 FROM @' || stage_name || '/ FILE_FORMAT = ' || file_format || ' ON_ERROR = ABORT_STATEMENT';

    EXECUTE IMMEDIATE load_cmd;

    -- Deduplicate and merge into PRODUCT table
    merge_cmd := 'MERGE INTO MARKET.NATIONALMARKET.PRODUCT AS target
                  USING (
                      SELECT 
                          ID_PRODUCT,
                          PRODUCT_NAME,
                          BRAND,
                          HEIGHT,
                          WIDTH,
                          DEPTH,
                          WEIGHT,
                          PACKAGE_TYPE,
                          PRICE
                      FROM (
                          SELECT 
                              ID_PRODUCT,
                              PRODUCT_NAME,
                              BRAND,
                              HEIGHT,
                              WIDTH,
                              DEPTH,
                              WEIGHT,
                              PACKAGE_TYPE,
                              PRICE,
                              ROW_NUMBER() OVER (PARTITION BY ID_PRODUCT ORDER BY CURRENT_TIMESTAMP DESC) AS row_num
                          FROM MARKET.NATIONALMARKET.PRODUCT_TEMP
                      )
                      WHERE row_num = 1  -- Take only the latest row for each ID_PRODUCT
                  ) AS source
                  ON target.ID_PRODUCT = source.ID_PRODUCT
                  WHEN MATCHED THEN
                      UPDATE SET
                          target.PRODUCT_NAME = source.PRODUCT_NAME,
                          target.BRAND = source.BRAND,
                          target.HEIGHT = source.HEIGHT,
                          target.WIDTH = source.WIDTH,
                          target.DEPTH = source.DEPTH,
                          target.WEIGHT = source.WEIGHT,
                          target.PACKAGE_TYPE = source.PACKAGE_TYPE,
                          target.PRICE = source.PRICE
                  WHEN NOT MATCHED THEN
                      INSERT (ID_PRODUCT, PRODUCT_NAME, BRAND, HEIGHT, WIDTH, DEPTH, WEIGHT, PACKAGE_TYPE, PRICE)
                      VALUES (source.ID_PRODUCT, source.PRODUCT_NAME, source.BRAND, source.HEIGHT, source.WIDTH, source.DEPTH, source.WEIGHT, source.PACKAGE_TYPE, source.PRICE)';

    EXECUTE IMMEDIATE merge_cmd;

    RETURN 'Data loaded and merged successfully from stage to table.';
END;
$$;

CREATE OR REPLACE PROCEDURE MARKET.NATIONALMARKET.LOAD_STORES_FROM_STAGE()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS 
$$
DECLARE
    stage_name STRING DEFAULT 'STAGE_STORES';
    file_format STRING DEFAULT 'MARKET_CSV_FF';
    load_cmd STRING;
    merge_cmd STRING;
    truncate_cmd STRING;
BEGIN
    -- Truncate the staging table before loading new data
    truncate_cmd := 'TRUNCATE TABLE MARKET.NATIONALMARKET.STORE_TEMP';
    EXECUTE IMMEDIATE truncate_cmd;

    -- Load data into temporary stage table
    load_cmd := 'COPY INTO MARKET.NATIONALMARKET.STORE_TEMP
                 FROM @' || stage_name || '/ FILE_FORMAT = ' || file_format || ' ON_ERROR = ABORT_STATEMENT';

    EXECUTE IMMEDIATE load_cmd;

    -- Deduplicate and merge into STORE table
    merge_cmd := 'MERGE INTO MARKET.NATIONALMARKET.STORE AS target
                  USING (
                      SELECT 
                          STORE_NAME,
                          STORE_NUMBER,
                          STORE_DIRECTION,
                          STATUS
                      FROM (
                          SELECT 
                              STORE_NAME,
                              STORE_NUMBER,
                              STORE_DIRECTION,
                              STATUS,
                              ROW_NUMBER() OVER (PARTITION BY STORE_NUMBER ORDER BY CURRENT_TIMESTAMP DESC) AS row_num
                          FROM MARKET.NATIONALMARKET.STORE_TEMP
                      )
                      WHERE row_num = 1  -- Take only the latest row for each STORE_NUMBER
                  ) AS source
                  ON target.STORE_NUMBER = source.STORE_NUMBER
                  WHEN MATCHED THEN
                      UPDATE SET
                          target.STORE_NAME = source.STORE_NAME,
                          target.STORE_DIRECTION = source.STORE_DIRECTION,
                          target.STATUS = source.STATUS
                  WHEN NOT MATCHED THEN
                      INSERT (STORE_NAME, STORE_NUMBER, STORE_DIRECTION, STATUS)
                      VALUES (source.STORE_NAME, source.STORE_NUMBER, source.STORE_DIRECTION, source.STATUS)';

    EXECUTE IMMEDIATE merge_cmd;

    RETURN 'Data loaded and merged successfully from stage to table.';
END;
$$;




CREATE OR REPLACE PROCEDURE MARKET.NATIONALMARKET.LOAD_SALES_FROM_STAGE()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS 
$$
DECLARE
    stage_name STRING DEFAULT 'STAGE_SALES';
    file_format STRING DEFAULT 'MARKET_CSV_FF';
    table_name STRING DEFAULT 'SALES';
    file_name STRING DEFAULT 'SALES.csv';
    load_cmd STRING;
    insert_cmd STRING;
    truncate_cmd STRING;
BEGIN
    -- Truncate the staging table before loading new data
    truncate_cmd := 'TRUNCATE TABLE MARKET.NATIONALMARKET.SALES_TEMP';
    EXECUTE IMMEDIATE truncate_cmd;

    -- Load data into temporary stage table
    load_cmd := 'COPY INTO MARKET.NATIONALMARKET.SALES_TEMP
                 FROM @' || stage_name || '/' || file_name || ' FILE_FORMAT = ' || file_format || ' ON_ERROR = ABORT_STATEMENT';
    EXECUTE IMMEDIATE load_cmd;

    -- Insert distinct records into SALES table
    insert_cmd := 'INSERT INTO MARKET.NATIONALMARKET.SALES (ID_PRODUCT, STORE_NUMBER, AMOUNT, UNITS_SOLD, MONTH_SALE)
                   SELECT 
                       ID_PRODUCT,
                       STORE_NUMBER,
                       AMOUNT,
                       UNITS_SOLD,
                       MONTH_SALE
                   FROM MARKET.NATIONALMARKET.SALES_TEMP AS source
                   WHERE NOT EXISTS (
                       SELECT 1
                       FROM MARKET.NATIONALMARKET.SALES AS target
                       WHERE target.ID_PRODUCT = source.ID_PRODUCT
                         AND target.STORE_NUMBER = source.STORE_NUMBER
                         AND target.MONTH_SALE = source.MONTH_SALE
                   )';
    EXECUTE IMMEDIATE insert_cmd;

    RETURN 'Data loaded and new distinct records inserted successfully from stage to table.';
END;
$$;




CREATE OR REPLACE PROCEDURE COPY_PRODUCTS()
RETURNS STRING
LANGUAGE SQL
AS
DECLARE
    copy_cmd STRING;
    timestamp_suffix STRING;

BEGIN
timestamp_suffix := TO_CHAR(ADD_MONTHS(CURRENT_DATE(), -1), 'MON_YYYY');

copy_cmd := 'COPY FILES 
            INTO @STAGE_HISTORIC_PRODUCTS/'||timestamp_suffix||'_
            FROM @STAGE_PRODUCTS 
            FILES = ("PRODUCTS.csv")';
      
    -- Step 2: Execute the copy command
    EXECUTE IMMEDIATE copy_cmd;

    -- Return success message
    RETURN 'File copied successfully from STAGE_PRODUCTS to STAGE_HISTORIC_PRODUCTS';
END;

CREATE OR REPLACE PROCEDURE MARKET.NATIONALMARKET.REMOVE_FILE_PRODUCTS()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS $$
try {
    var remove_cmd = "REMOVE @STAGE_PRODUCTS/PRODUCTS.csv";
    var statement1 = snowflake.createStatement({sqlText: remove_cmd});
    statement1.execute();
    return 'File PRODUCTS.csv removed successfully from STAGE_PRODUCTS.';
} catch (err) {
    return 'Failed to remove file PRODUCTS.csv: ' + err.message;
}
$$;

CREATE OR REPLACE PROCEDURE COPY_STORES()
RETURNS STRING
LANGUAGE SQL
AS
DECLARE
    copy_cmd STRING;
    timestamp_suffix STRING;

BEGIN
timestamp_suffix := TO_CHAR(ADD_MONTHS(CURRENT_DATE(), -1), 'MON_YYYY');

copy_cmd := 'COPY FILES 
            INTO @STAGE_HISTORIC_STORES/'||timestamp_suffix||'_
            FROM @STAGE_STORES 
            FILES = ("STORES.csv")';
      
    -- Step 2: Execute the copy command
    EXECUTE IMMEDIATE copy_cmd;

    -- Return success message
    RETURN 'File copied successfully from STAGE_STORES to STAGE_HISTORIC_STORES';
END;

CREATE OR REPLACE PROCEDURE MARKET.NATIONALMARKET.REMOVE_FILE_STORES()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS $$
try {
    var remove_cmd = "REMOVE @STAGE_STORES/STORES.csv";
    var statement1 = snowflake.createStatement({sqlText: remove_cmd});
    statement1.execute();
    return 'File STORES.csv removed successfully from STAGE_STORES.';
} catch (err) {
    return 'Failed to remove file STORES.csv: ' + err.message;
}
$$;

CREATE OR REPLACE PROCEDURE COPY_SALES()
RETURNS STRING
LANGUAGE SQL
AS
DECLARE
    copy_cmd STRING;
    timestamp_suffix STRING;

BEGIN
timestamp_suffix := TO_CHAR(ADD_MONTHS(CURRENT_DATE(), -1), 'MON_YYYY');

copy_cmd := 'COPY FILES 
            INTO @STAGE_HISTORIC_SALES/'||timestamp_suffix||'_
            FROM @STAGE_SALES 
            FILES = ("SALES.csv")';
      
    -- Step 2: Execute the copy command
    EXECUTE IMMEDIATE copy_cmd;

    -- Return success message
    RETURN 'File copied successfully from STAGE_SALES to STAGE_HISTORIC_SALES';
END;



CREATE OR REPLACE PROCEDURE MARKET.NATIONALMARKET.REMOVE_FILE_SALES()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
try {
    var remove_cmd = "REMOVE @STAGE_SALES/SALES.csv";
    var statement1 = snowflake.createStatement({sqlText: remove_cmd});
    statement1.execute();
    return ''File SALES.csv removed successfully from STAGE_SALES.'';
} catch (err) {
    return ''Failed to remove file SALES.csv: '' + err.message;
}
';

remove @stage_products
