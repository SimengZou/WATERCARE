CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_BILLING_QUANTITYFIELDHELP(
            ADDBY varchar, 
            ADDDTTM datetime, 
            DELETED boolean, 
            DISPLAYORDER integer, 
            HELPLINKTEXT varchar, 
            KBKEY integer, 
            MODBY varchar, 
            MODDTTM datetime, 
            QUANTITYFIELDHELPKEY integer, 
            QUANTITYFIELDKEY integer, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 