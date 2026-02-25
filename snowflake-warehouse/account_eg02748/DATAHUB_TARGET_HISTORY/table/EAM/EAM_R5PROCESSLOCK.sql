CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_R5PROCESSLOCK_DELETED(
                "PLK_CODE" varchar, 
                "PLK_DESC" varchar, 
                "PLK_LASTSAVED" datetime, 
                "PLK_LOCKTIME" datetime, 
                "PLK_UPDATECOUNT" numeric(38, 10), 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar,
                ETL_IS_DELETED boolean default false
                ); 