CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_BILLING_APPROVALLEVELSETUP(
            ACCESSID integer, 
            ADDBY varchar, 
            ADDDTTM datetime, 
            APPROVALLEVELSETUPKEY integer, 
            DELETED boolean, 
            DESCRIPTION varchar, 
            LEVELORDER integer, 
            MODBY varchar, 
            MODDTTM datetime, 
            NAME varchar, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 