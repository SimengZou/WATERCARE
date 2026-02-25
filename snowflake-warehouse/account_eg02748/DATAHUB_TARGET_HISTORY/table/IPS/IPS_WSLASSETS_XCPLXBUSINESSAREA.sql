CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_WSLASSETS_XCPLXBUSINESSAREA(
            ADDBY varchar, 
            ADDDTTM datetime, 
            BUSINESSAREA varchar, 
            COMPKEY integer, 
            COSTCENTRE varchar, 
            DELETED boolean, 
            MODBY varchar, 
            MODDTTM datetime, 
            PROJECTNO varchar, 
            VARIATION_ID integer, 
            XCPLXBUSINESSAREAKEY integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 