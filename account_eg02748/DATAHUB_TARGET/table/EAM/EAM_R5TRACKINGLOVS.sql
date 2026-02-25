CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5TRACKINGLOVS(
                "TKV_CODE" varchar, 
                "TKV_LASTSAVED" datetime, 
                "TKV_SQL" varchar, 
                "TKV_UPDATECOUNT" numeric(38, 10), 
                "TKV_UPDATED" datetime, 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar
                ); 