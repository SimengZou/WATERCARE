CREATE OR REPLACE TABLE SM_CURATED_MEASUREMENT_MISSING_IPS_ACCOUNT(
                        M_UNIT_ID VARCHAR(16777216),
                        HAS_IPS_ACCOUNT BOOLEAN,
                        ETL_LOAD_DATETIME TIMESTAMP_NTZ(9) default current_timestamp());