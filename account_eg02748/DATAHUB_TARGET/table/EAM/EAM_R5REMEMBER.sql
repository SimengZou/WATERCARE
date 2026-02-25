CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5REMEMBER(
                "RMB_FUNCTION" varchar, 
                "RMB_ITEM" varchar, 
                "RMB_ITEM2" varchar, 
                "RMB_LASTSAVED" datetime, 
                "RMB_RENTITY" varchar, 
                "RMB_UPDATECOUNT" numeric(38, 10), 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar
                ); 