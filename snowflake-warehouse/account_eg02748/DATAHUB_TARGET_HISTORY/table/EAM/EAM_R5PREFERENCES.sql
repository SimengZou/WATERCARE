CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_R5PREFERENCES_DELETED(
                "PRF_CODE" varchar, 
                "PRF_DEFAULT" varchar, 
                "PRF_LASTSAVED" datetime, 
                "PRF_NR" numeric(38, 10), 
                "PRF_UPDATECOUNT" numeric(38, 10), 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar,
                ETL_IS_DELETED boolean default false
                ); 