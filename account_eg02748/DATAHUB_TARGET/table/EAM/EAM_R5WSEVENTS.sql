CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5WSEVENTS(
                "WSE_BASE_EVENT" varchar, 
                "WSE_CODE" varchar, 
                "WSE_DESC" varchar, 
                "WSE_FILTER_PROCESSOR" varchar, 
                "WSE_LASTSAVED" datetime, 
                "WSE_MEKEY" varchar, 
                "WSE_UPDATECOUNT" numeric(38, 10), 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar
                ); 