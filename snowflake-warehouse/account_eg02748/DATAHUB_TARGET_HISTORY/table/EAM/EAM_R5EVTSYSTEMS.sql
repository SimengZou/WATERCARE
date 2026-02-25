CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_R5EVTSYSTEMS_DELETED(
                "ESY_EVENT" varchar, 
                "ESY_LASTSAVED" datetime, 
                "ESY_SYSTEM" varchar, 
                "ESY_SYSTEM_ORG" varchar, 
                "ESY_UPDATECOUNT" numeric(38, 10), 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar,
                ETL_IS_DELETED boolean default false
                ); 