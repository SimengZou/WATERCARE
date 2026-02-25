CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5WSREQHIST(
            WSQ_DATAAREA numeric(38, 10), 
            WSQ_LASTSAVED datetime, 
            WSQ_MESSAGE varchar, 
            WSQ_PROCESS varchar, 
            WSQ_RSTATUS varchar, 
            WSQ_STATUS varchar, 
            WSQ_STATUS_MESSAGE varchar, 
            WSQ_TIME datetime, 
            WSQ_UPDATECOUNT numeric(38, 10), 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 