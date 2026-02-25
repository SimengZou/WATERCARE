CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5CUSTOMTEXTS(
                "CTT_DATE" datetime, 
                "CTT_LANG" varchar, 
                "CTT_LASTSAVED" datetime, 
                "CTT_LENGTH" numeric(38, 10), 
                "CTT_ORIGTEXT" varchar, 
                "CTT_POOL" varchar, 
                "CTT_TEXT" varchar, 
                "CTT_UPDATECOUNT" numeric(38, 10), 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar
                ); 