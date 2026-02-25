CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_RESOURCES_RATETABLE_RATECODECATEGORY(
            ADDBY varchar, 
            ADDDTTM datetime, 
            CATEGORYNAME varchar, 
            CHILDKEY integer, 
            DELETED boolean, 
            MODBY varchar, 
            MODDTTM datetime, 
            RATECODECATEGORYKEY integer, 
            RATECODETYPE integer, 
            SORTORDER integer, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 