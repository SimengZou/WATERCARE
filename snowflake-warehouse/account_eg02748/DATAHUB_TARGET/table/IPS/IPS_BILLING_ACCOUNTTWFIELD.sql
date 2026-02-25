CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_BILLING_ACCOUNTTWFIELD(
            ACCOUNTSERVICEKEY integer, 
            ACCOUNTTWFIELDKEY integer, 
            ADDBY varchar, 
            ADDDTTM datetime, 
            DELETED boolean, 
            EFFDATE datetime, 
            EXPDATE datetime, 
            MODBY varchar, 
            MODDTTM datetime, 
            RESULTVALUE numeric(38, 10), 
            TRADEWASTEFIELDKEY integer, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 