CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5SCREENCACHE(
                "SNC_FUNCTION" varchar, 
                "SNC_LASTSAVED" datetime, 
                "SNC_UPDATECOUNT" numeric(38, 10), 
                "SNC_USER" varchar, 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar
                ); 