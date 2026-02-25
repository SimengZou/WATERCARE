CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_BILLING_SUSPENDSERVICESETUP(
            ADDBY varchar, 
            ADDDTTM datetime, 
            CREDITADJTYPESUKEY integer, 
            DEBITADJTYPESUKEY integer, 
            DELETED boolean, 
            LOGTYPE varchar, 
            MODBY varchar, 
            MODDTTM datetime, 
            SUSPENDSERVICESETUPKEYKEY integer, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 