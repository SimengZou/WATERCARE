CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_R5LANGINST_DELETED(
                "LIN_CODE" varchar, 
                "LIN_LASTSAVED" datetime, 
                "LIN_TXTTYPE" varchar, 
                "LIN_UPDATECOUNT" numeric(38, 10), 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar,
                ETL_IS_DELETED boolean default false
                ); 