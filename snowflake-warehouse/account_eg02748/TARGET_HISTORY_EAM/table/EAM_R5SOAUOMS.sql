CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_R5SOAUOMS_DELETED(
            SUO_ACTIVE varchar, 
            SUO_CODE varchar, 
            SUO_DESC varchar, 
            SUO_LASTSAVED datetime, 
            SUO_UPDATECOUNT numeric(38, 10), 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 