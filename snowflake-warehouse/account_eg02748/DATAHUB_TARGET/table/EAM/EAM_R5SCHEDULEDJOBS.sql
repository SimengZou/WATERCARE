CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5SCHEDULEDJOBS(
                "SCJ_ACTIVE" varchar, 
                "SCJ_BROKEN" varchar, 
                "SCJ_CODE" varchar, 
                "SCJ_JOBNAME" varchar, 
                "SCJ_LASTRUN" datetime, 
                "SCJ_LASTSAVED" datetime, 
                "SCJ_NEXTRUN" datetime, 
                "SCJ_SCHCODE" varchar, 
                "SCJ_SERVERID" varchar, 
                "SCJ_TYPE" varchar, 
                "SCJ_UPDATECOUNT" numeric(38, 10), 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar
                ); 