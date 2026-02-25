CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_WSLCRM_XTANKERINFODP(
            ADDBY varchar, 
            ADDDTTM datetime, 
            BONDPAID varchar, 
            BONDREFUNDED varchar, 
            DATALAKE_DELETED boolean, 
            MODBY varchar, 
            MODDTTM datetime, 
            MOHEXPIRY datetime, 
            SERVICESTARTDATE datetime, 
            SERVNO integer, 
            VARIATION_ID integer, 
            XTANKERINFODPKEY integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 