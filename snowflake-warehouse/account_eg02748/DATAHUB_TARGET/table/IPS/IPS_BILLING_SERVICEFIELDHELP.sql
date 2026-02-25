CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_BILLING_SERVICEFIELDHELP(
            ADDBY varchar, 
            ADDDTTM datetime, 
            DELETED boolean, 
            DISPLAYORDER integer, 
            HELPLINKTEXT varchar, 
            KBKEY integer, 
            MODBY varchar, 
            MODDTTM datetime, 
            SERVICEFIELDHELPKEY integer, 
            SERVICEFIELDKEY integer, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 