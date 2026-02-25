CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5SHFDAYS(
            SHD_DAY numeric(38, 10), 
            SHD_HOURS numeric(38, 10), 
            SHD_LASTSAVED datetime, 
            SHD_SHIFT varchar, 
            SHD_TIME numeric(38, 10), 
            SHD_UPDATECOUNT numeric(38, 10), 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 