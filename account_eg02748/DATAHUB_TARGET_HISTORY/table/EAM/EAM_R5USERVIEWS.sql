CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_R5USERVIEWS_DELETED(
                "UVW_ACTIVE" varchar, 
                "UVW_CODE" varchar, 
                "UVW_DESC" varchar, 
                "UVW_LASTSAVED" datetime, 
                "UVW_NAME" varchar, 
                "UVW_NOTE" varchar, 
                "UVW_STMT" varchar, 
                "UVW_UPDATECOUNT" numeric(38, 10), 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar,
                ETL_IS_DELETED boolean default false
                ); 