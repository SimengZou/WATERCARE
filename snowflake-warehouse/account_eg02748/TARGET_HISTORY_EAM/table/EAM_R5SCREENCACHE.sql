CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_R5SCREENCACHE_DELETED(
            SNC_FUNCTION varchar, 
            SNC_LASTSAVED datetime, 
            SNC_UPDATECOUNT numeric(38, 10), 
            SNC_USER varchar, 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 