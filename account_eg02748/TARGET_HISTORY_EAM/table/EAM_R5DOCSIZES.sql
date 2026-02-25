CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_R5DOCSIZES_DELETED(
            DSZ_CODE varchar, 
            DSZ_DESC varchar, 
            DSZ_LASTSAVED datetime, 
            DSZ_UPDATECOUNT numeric(38, 10), 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 