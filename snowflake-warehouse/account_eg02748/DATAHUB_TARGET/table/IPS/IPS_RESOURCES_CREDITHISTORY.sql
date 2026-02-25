CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_RESOURCES_CREDITHISTORY(
            ADDBY varchar, 
            ADDDTTM datetime, 
            APPLIEDDATE datetime, 
            CREDITHISTORYKEY integer, 
            CREDITHISTORYSOURCE varchar, 
            CREDITRATINGPOINTSCODE varchar, 
            DATALAKE_DELETED boolean, 
            IDENTITYKEY integer, 
            MODBY varchar, 
            MODDTTM datetime, 
            POINTS integer, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 