CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5PRINTERTYPES(
                "PNT_CODE" varchar, 
                "PNT_DESC" varchar, 
                "PNT_LASTSAVED" datetime, 
                "PNT_UPDATECOUNT" numeric(38, 10), 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar
                ); 