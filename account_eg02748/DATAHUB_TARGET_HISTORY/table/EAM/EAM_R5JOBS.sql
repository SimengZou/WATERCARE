CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_R5JOBS_DELETED(
                "JOB_CLASS" varchar, 
                "JOB_DESCRIPTION" varchar, 
                "JOB_LASTSAVED" datetime, 
                "JOB_NAME" varchar, 
                "JOB_PARTNERID" varchar, 
                "JOB_TYPE" varchar, 
                "JOB_UPDATECOUNT" numeric(38, 10), 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar,
                ETL_IS_DELETED boolean default false
                ); 