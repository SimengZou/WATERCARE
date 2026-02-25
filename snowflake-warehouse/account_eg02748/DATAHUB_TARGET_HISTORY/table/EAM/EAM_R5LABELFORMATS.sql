CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_R5LABELFORMATS_DELETED(
                "LBL_CLASS" varchar, 
                "LBL_CLASS_ORG" varchar, 
                "LBL_CODE" varchar, 
                "LBL_DESC" varchar, 
                "LBL_FIELDS" varchar, 
                "LBL_FILENAME" varchar, 
                "LBL_LASTSAVED" datetime, 
                "LBL_PRINTERTYPE" varchar, 
                "LBL_UPDATECOUNT" numeric(38, 10), 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar,
                ETL_IS_DELETED boolean default false
                ); 