CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_BILLING_QUANTITYFIELDMOVEOUTHELP(
            ADDBY varchar, 
            ADDDTTM datetime, 
            DELETED boolean, 
            DISPLAYORDER integer, 
            HELPLINKTEXT varchar, 
            KBKEY integer, 
            LINETYPE integer, 
            MODBY varchar, 
            MODDTTM datetime, 
            QTYFIELDMOVEOUTHELPKEY integer, 
            QUANTITYFIELD integer, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 