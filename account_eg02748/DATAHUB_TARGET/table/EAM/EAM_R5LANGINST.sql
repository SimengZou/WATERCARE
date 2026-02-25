CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5LANGINST(
                "LIN_CODE" varchar, 
                "LIN_LASTSAVED" datetime, 
                "LIN_TXTTYPE" varchar, 
                "LIN_UPDATECOUNT" numeric(38, 10), 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar
                ); 