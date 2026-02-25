CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_R5WSDUP2_DELETED(
            WD2_CODE varchar, 
            WD2_DESC varchar, 
            WD2_LASTSAVED datetime, 
            WD2_TIME datetime, 
            WD2_TYPE varchar, 
            WD2_UPDATECOUNT numeric(38, 10), 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 