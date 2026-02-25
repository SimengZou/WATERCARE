CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5SCHEDULEDJOBSLOCK(
                "SJL_CODE" varchar, 
                "SJL_LASTSAVED" datetime, 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar
                ); 