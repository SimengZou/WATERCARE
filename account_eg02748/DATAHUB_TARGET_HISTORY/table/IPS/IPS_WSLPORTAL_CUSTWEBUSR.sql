CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_WSLPORTAL_CUSTWEBUSR(
            ACCTKEY integer, 
            ADDBY varchar, 
            ADDDTTM datetime, 
            CUSTWEBUSRKEY integer, 
            DATALAKE_DELETED boolean, 
            MODBY varchar, 
            MODDTTM datetime, 
            MULTIACCOUNT integer, 
            SERVNO integer, 
            USRID varchar, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 