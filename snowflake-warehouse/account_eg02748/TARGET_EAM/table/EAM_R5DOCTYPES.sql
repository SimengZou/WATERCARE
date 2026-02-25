CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5DOCTYPES(
            DOT_EXT varchar, 
            DOT_LASTSAVED datetime, 
            DOT_TYPE varchar, 
            DOT_UPDATECOUNT numeric(38, 10), 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 