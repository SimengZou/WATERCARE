CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_RESOURCES_RATETABLE_RATECODE(
            ADDBY varchar, 
            ADDDTTM datetime, 
            CODETYPE integer, 
            DELETED boolean, 
            DESCRIPTION varchar, 
            MODBY varchar, 
            MODDTTM datetime, 
            RATECODE varchar, 
            RATECODECATEGORYKEY integer, 
            RATECODEKEY integer, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 