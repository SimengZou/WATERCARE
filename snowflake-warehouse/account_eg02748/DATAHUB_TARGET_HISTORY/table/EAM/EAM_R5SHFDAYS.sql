CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_R5SHFDAYS_DELETED(
                "SHD_DAY" numeric(38, 10), 
                "SHD_HOURS" numeric(38, 10), 
                "SHD_LASTSAVED" datetime, 
                "SHD_SHIFT" varchar, 
                "SHD_TIME" numeric(38, 10), 
                "SHD_UPDATECOUNT" numeric(38, 10), 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar,
                ETL_IS_DELETED boolean default false
                ); 