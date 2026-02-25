CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_R5IMPORTEXPORTGRPSTATUS_DELETED(
            IMG_END datetime, 
            IMG_GROUP varchar, 
            IMG_LASTSAVED datetime, 
            IMG_PROCESSORDER numeric(38, 10), 
            IMG_SESSIONID numeric(38, 10), 
            IMG_START datetime, 
            IMG_STATUS varchar, 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 