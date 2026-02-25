CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5USERPREFERENCES(
                "USP_CODE" varchar, 
                "USP_LASTSAVED" datetime, 
                "USP_TYPE" varchar, 
                "USP_UPDATECOUNT" numeric(38, 10), 
                "USP_USER" varchar, 
                "USP_VALUE" varchar, 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar
                ); 