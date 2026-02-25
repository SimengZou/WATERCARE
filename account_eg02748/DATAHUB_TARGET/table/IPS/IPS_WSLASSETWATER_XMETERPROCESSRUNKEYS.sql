CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_WSLASSETWATER_XMETERPROCESSRUNKEYS(
            ADDBY varchar, 
            ADDDTTM datetime, 
            DELETED boolean, 
            ESTIMATEREADINGKEY integer, 
            LASTBATCHKEY integer, 
            LASTIMPORTACTIVITYKEY integer, 
            MODBY varchar, 
            MODDTTM datetime, 
            VARIATION_ID integer, 
            XMETERPROCESSRUNKEYSKEY integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 