CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_R5USEGRIDSYSDEFAULT_DELETED(
                "USD_DATASPYFILTER" varchar, 
                "USD_DATASPYID" numeric(38, 10), 
                "USD_GRIDID" numeric(38, 10), 
                "USD_LASTSAVED" datetime, 
                "USD_UPDATECOUNT" numeric(38, 10), 
                "USD_USERID" varchar, 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar,
                ETL_IS_DELETED boolean default false
                ); 