CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_RESOURCES_CREDITRATINGLEVEL(
            ADDBY varchar, 
            ADDDTTM datetime, 
            CREDITRATINGLEVELKEY integer, 
            DATALAKE_DELETED boolean, 
            MINIMUMPOINTS integer, 
            MODBY varchar, 
            MODDTTM datetime, 
            RATING varchar, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 