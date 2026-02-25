CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_BILLING_DIRECTDEBITRUNACTIVITY(
            ACTIVITYRUNBY varchar, 
            ACTIVITYTYPE integer, 
            ADDBY varchar, 
            ADDDTTM datetime, 
            DELETED boolean, 
            DIRECTDEBITRUNACTIVITYKEY integer, 
            DIRECTDEBITRUNKEY integer, 
            MODBY varchar, 
            MODDTTM datetime, 
            STARTDTTM datetime, 
            STOPDTTM datetime, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 