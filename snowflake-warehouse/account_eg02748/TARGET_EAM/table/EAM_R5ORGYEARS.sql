CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5ORGYEARS(
            OYE_END datetime, 
            OYE_LASTSAVED datetime, 
            OYE_ORG varchar, 
            OYE_PK numeric(38, 10), 
            OYE_START datetime, 
            OYE_UPDATECOUNT numeric(38, 10), 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 