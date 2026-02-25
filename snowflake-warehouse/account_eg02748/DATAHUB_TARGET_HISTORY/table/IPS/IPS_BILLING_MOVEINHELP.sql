CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_BILLING_MOVEINHELP(
            ADDBY varchar, 
            ADDDTTM datetime, 
            DELETED boolean, 
            DISPLAYORDER integer, 
            HELPLINKTEXT varchar, 
            KBKEY integer, 
            MODBY varchar, 
            MODDTTM datetime, 
            MOVEINHELPKEY integer, 
            MOVEINLINE integer, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 