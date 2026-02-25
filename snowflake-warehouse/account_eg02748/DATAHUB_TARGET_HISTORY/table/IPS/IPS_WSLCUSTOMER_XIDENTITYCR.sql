CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_WSLCUSTOMER_XIDENTITYCR(
            ADDBY varchar, 
            ADDDTTM datetime, 
            DELETED boolean, 
            EMAIL varchar, 
            IDKEY integer, 
            MODBY varchar, 
            MODDTTM datetime, 
            SMS varchar, 
            VARIATION_ID integer, 
            XIDENTITYCRKEY integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 