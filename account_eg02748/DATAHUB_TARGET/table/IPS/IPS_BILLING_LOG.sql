CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_BILLING_LOG(
            ACCOUNTKEY integer, 
            ADDBY varchar, 
            ADDDTTM datetime, 
            DELETED boolean, 
            LOGBY varchar, 
            LOGFROMDATE datetime, 
            LOGKEY integer, 
            LOGTODATE datetime, 
            MODBY varchar, 
            MODDTTM datetime, 
            TYPE varchar, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 