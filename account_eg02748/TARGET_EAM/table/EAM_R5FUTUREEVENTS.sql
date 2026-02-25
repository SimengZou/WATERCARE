CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5FUTUREEVENTS(
                "FUT_DATE" datetime, 
                "FUT_EVENT" varchar, 
                "FUT_LASTSAVED" datetime, 
                "FUT_MP_SEQ" numeric(38, 10), 
                "FUT_SEQNO" numeric(38, 10), 
                "FUT_UPDATECOUNT" numeric(38, 10), 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar
                ); 