CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_R5CAUSES_DELETED(
                "CAU_CODE" varchar, 
                "CAU_CREATED" datetime, 
                "CAU_DESC" varchar, 
                "CAU_ENABLEWORKORDERS" varchar, 
                "CAU_GEN" varchar, 
                "CAU_GROUP" varchar, 
                "CAU_LASTSAVED" datetime, 
                "CAU_NOTUSED" varchar, 
                "CAU_PARTFAILURE" varchar, 
                "CAU_UPDATECOUNT" numeric(38, 10), 
                "CAU_UPDATED" datetime, 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar,
                ETL_IS_DELETED boolean default false
                ); 