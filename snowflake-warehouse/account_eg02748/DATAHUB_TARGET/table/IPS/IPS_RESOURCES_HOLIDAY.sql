CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_RESOURCES_HOLIDAY(
            ADDBY varchar, 
            ADDDTTM datetime, 
            COMMENTS varchar, 
            DATALAKE_DELETED boolean, 
            HOLDATE datetime, 
            MODBY varchar, 
            MODDTTM datetime, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 