CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_R5BARESC_DELETED(
            BCE_COLUMN varchar, 
            BCE_ESCAPE varchar, 
            BCE_LASTSAVED datetime, 
            BCE_LINE numeric(38, 10), 
            BCE_TEXT1 varchar, 
            BCE_TEXT2 varchar, 
            BCE_UPDATECOUNT numeric(38, 10), 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 