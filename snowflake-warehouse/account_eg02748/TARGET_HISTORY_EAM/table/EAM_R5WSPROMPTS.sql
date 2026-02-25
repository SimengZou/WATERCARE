CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_R5WSPROMPTS_DELETED(
            WST_CODE varchar, 
            WST_DESC varchar, 
            WST_FUNCTION varchar, 
            WST_LASTSAVED datetime, 
            WST_NOTUSED varchar, 
            WST_SYSTEM varchar, 
            WST_TAB varchar, 
            WST_UPDATECOUNT numeric(38, 10), 
            WST_UPDATED datetime, 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 