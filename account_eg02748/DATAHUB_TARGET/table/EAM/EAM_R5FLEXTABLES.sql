CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5FLEXTABLES(
                "FLT_LASTSAVED" datetime, 
                "FLT_TABLE" varchar, 
                "FLT_UPDATECOUNT" numeric(38, 10), 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar
                ); 