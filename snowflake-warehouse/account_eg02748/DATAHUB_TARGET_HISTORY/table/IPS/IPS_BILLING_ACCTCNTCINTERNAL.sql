CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_BILLING_ACCTCNTCINTERNAL(
            ACCOUNTKEY integer, 
            ACCTCNTCINTERNALKEY integer, 
            ADDBY varchar, 
            ADDDTTM datetime, 
            DELETED boolean, 
            EFFDATE datetime, 
            EMPID varchar, 
            EXPDATE datetime, 
            MODBY varchar, 
            MODDTTM datetime, 
            ROLE varchar, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 