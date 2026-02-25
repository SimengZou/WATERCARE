CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_R5ERRTEXTS_DELETED(
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
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 