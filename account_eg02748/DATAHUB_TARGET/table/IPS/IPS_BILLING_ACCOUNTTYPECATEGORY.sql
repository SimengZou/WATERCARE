CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_BILLING_ACCOUNTTYPECATEGORY(
            ACCOUNTTYPECATEGORYKEY integer, 
            ADDBY varchar, 
            ADDDTTM datetime, 
            CATEGORYNAME varchar, 
            DELETED boolean, 
            MODBY varchar, 
            MODDTTM datetime, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 