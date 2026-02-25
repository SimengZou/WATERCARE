CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5ERRTEXTS(
            ERT_CODE varchar, 
            ERT_LANG varchar, 
            ERT_LASTSAVED datetime, 
            ERT_TEXT varchar, 
            ERT_TRANSLATE varchar, 
            ERT_UPDATECOUNT numeric(38, 10), 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 