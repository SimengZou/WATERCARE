CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_BILLING_MAILTOCONFIGURATION(
            ADDBY varchar, 
            ADDDTTM datetime, 
            CAREOFSTRING varchar, 
            DELETED boolean, 
            FIRSTNAMEORDER integer, 
            LASTNAMEORDER integer, 
            MAILTOCONFIGURATIONKEY integer, 
            MIDDLEINITIALORDER integer, 
            MODBY varchar, 
            MODDTTM datetime, 
            TITLEORDER integer, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 