CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_R5FILTERTABCOLUMNS_DELETED(
            FTC_CLOB varchar, 
            FTC_COLUMN varchar, 
            FTC_DATATYPE varchar, 
            FTC_LASTSAVED datetime, 
            FTC_TABLE varchar, 
            FTC_UPDATECOUNT numeric(38, 10), 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 