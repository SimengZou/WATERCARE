CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_R5JOBTYPEAUTH_DELETED(
                "JTA_DELETE" varchar, 
                "JTA_GROUP" varchar, 
                "JTA_INSERT" varchar, 
                "JTA_JOBTYPE" varchar, 
                "JTA_LASTSAVED" datetime, 
                "JTA_STATUS" varchar, 
                "JTA_UPDATE" varchar, 
                "JTA_UPDATECOUNT" numeric(38, 10), 
                "JTA_UPDATED" datetime, 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar,
                ETL_IS_DELETED boolean default false
                ); 