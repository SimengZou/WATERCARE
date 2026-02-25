CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5FILTERTABLES(
                "FTA_LASTSAVED" datetime, 
                "FTA_TABLE" varchar, 
                "FTA_UPDATECOUNT" numeric(38, 10), 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar
                ); 