CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5ALERTSQL(
                "ALS_ABORTONFAILURE" varchar, 
                "ALS_ACTIVE" varchar, 
                "ALS_ALERT" varchar, 
                "ALS_COMMENT" varchar, 
                "ALS_INCLUDEPREVIEW" varchar, 
                "ALS_LASTSAVED" datetime, 
                "ALS_RTYPE" varchar, 
                "ALS_STMT" varchar, 
                "ALS_UPDATECOUNT" numeric(38, 10), 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar
                ); 