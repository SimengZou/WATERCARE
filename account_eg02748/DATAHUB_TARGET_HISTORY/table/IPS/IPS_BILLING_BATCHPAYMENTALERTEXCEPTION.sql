CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_BILLING_BATCHPAYMENTALERTEXCEPTION(
            ACALRTKEY integer, 
            ACBPAYKEY integer, 
            ACCOUNTKEY integer, 
            ADDBY varchar, 
            ADDDTTM datetime, 
            ALERTKEY integer, 
            BATCHKEY integer, 
            DELETED boolean, 
            MODBY varchar, 
            MODDTTM datetime, 
            PAYKEY integer, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 