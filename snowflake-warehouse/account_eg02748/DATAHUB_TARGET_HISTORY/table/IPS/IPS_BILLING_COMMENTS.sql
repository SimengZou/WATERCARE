CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_BILLING_COMMENTS(
            ADDBY varchar, 
            ADDDTTM datetime, 
            COMMENTSKEY integer, 
            DATALAKE_DELETED boolean, 
            MODBY varchar, 
            MODDTTM datetime, 
            TEXT varchar, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 