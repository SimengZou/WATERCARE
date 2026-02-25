CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_R5WSDUP1_DELETED(
            WD1_CODE varchar, 
            WD1_DESC varchar, 
            WD1_LASTSAVED datetime, 
            WD1_TIME datetime, 
            WD1_TYPE varchar, 
            WD1_UPDATECOUNT numeric(38, 10), 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 