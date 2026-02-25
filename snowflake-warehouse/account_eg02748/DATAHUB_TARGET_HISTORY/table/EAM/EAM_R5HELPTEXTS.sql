CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_R5HELPTEXTS_DELETED(
                "HEL_CHANGED" varchar, 
                "HEL_DRILLDOWN" varchar, 
                "HEL_FUNCTION" varchar, 
                "HEL_ITEM" varchar, 
                "HEL_LANG" varchar, 
                "HEL_LASTSAVED" datetime, 
                "HEL_POOL" varchar, 
                "HEL_TEXT" varchar, 
                "HEL_UPDATECOUNT" numeric(38, 10), 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar,
                ETL_IS_DELETED boolean default false
                ); 