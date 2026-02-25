CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5LEVELS(
                "LVL_LASTSAVED" datetime, 
                "LVL_LEVEL" numeric(38, 10), 
                "LVL_UPDATECOUNT" numeric(38, 10), 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar
                ); 