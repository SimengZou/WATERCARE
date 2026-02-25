CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_R5PRINTERTYPES_DELETED(
            PNT_CODE varchar, 
            PNT_DESC varchar, 
            PNT_LASTSAVED datetime, 
            PNT_UPDATECOUNT numeric(38, 10), 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 