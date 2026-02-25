CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_R5LOCALEHOTKEYS_DELETED(
                "LHK_CODE" varchar, 
                "LHK_DEFAULT" numeric(38, 10), 
                "LHK_DESC" varchar, 
                "LHK_EXTRA" varchar, 
                "LHK_KEY" numeric(38, 10), 
                "LHK_LASTSAVED" datetime, 
                "LHK_LOCALE" varchar, 
                "LHK_UPDATECOUNT" numeric(38, 10), 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar,
                ETL_IS_DELETED boolean default false
                ); 