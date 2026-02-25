CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_R5WSRSPHIST_DELETED(
            WSR_ADDRESS varchar, 
            WSR_DATAAREA numeric(38, 10), 
            WSR_LASTSAVED datetime, 
            WSR_MESSAGE varchar, 
            WSR_PROCESS varchar, 
            WSR_RSTATUS varchar, 
            WSR_STATUS varchar, 
            WSR_STATUS_MESSAGE varchar, 
            WSR_TIME datetime, 
            WSR_UPDATECOUNT numeric(38, 10), 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 