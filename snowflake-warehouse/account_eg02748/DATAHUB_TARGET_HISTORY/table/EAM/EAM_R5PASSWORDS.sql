CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_R5PASSWORDS_DELETED(
                "PWD_LASTSAVED" datetime, 
                "PWD_LASTUSED" datetime, 
                "PWD_PASSWORD" varchar, 
                "PWD_UPDATECOUNT" numeric(38, 10), 
                "PWD_USER" varchar, 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar,
                ETL_IS_DELETED boolean default false
                ); 