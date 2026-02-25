CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_R5VIOLATIONS_DELETED(
                "VIO_DATE" datetime, 
                "VIO_LASTSAVED" datetime, 
                "VIO_OSMACHINE" varchar, 
                "VIO_OSUSER" varchar, 
                "VIO_PASSWORD" varchar, 
                "VIO_UPDATECOUNT" numeric(38, 10), 
                "VIO_USER" varchar, 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar,
                ETL_IS_DELETED boolean default false
                ); 