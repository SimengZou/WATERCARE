CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5EXTENSIBLEFRAMEWORK(
                "EFR_ACTIVE" varchar, 
                "EFR_LASTSAVED" datetime, 
                "EFR_NAME" varchar, 
                "EFR_UPDATECOUNT" numeric(38, 10), 
                "EFR_USERFUNCTION" varchar, 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar
                ); 