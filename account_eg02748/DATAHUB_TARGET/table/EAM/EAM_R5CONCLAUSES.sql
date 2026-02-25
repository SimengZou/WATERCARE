CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5CONCLAUSES(
                "CCL_CLASS" varchar, 
                "CCL_CLASS_ORG" varchar, 
                "CCL_CODE" varchar, 
                "CCL_DESC" varchar, 
                "CCL_LASTSAVED" datetime, 
                "CCL_LINE" numeric(38, 10), 
                "CCL_NOTUSED" varchar, 
                "CCL_ORG" varchar, 
                "CCL_PARENT" varchar, 
                "CCL_SYSTEM" varchar, 
                "CCL_UPDATECOUNT" numeric(38, 10), 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar
                ); 