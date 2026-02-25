CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_R5USERLPFTYPES_DELETED(
                "LPT_LASTSAVED" datetime, 
                "LPT_LINEARPREFERENCE" varchar, 
                "LPT_RTYPE" varchar, 
                "LPT_SEQUENCE" numeric(38, 10), 
                "LPT_TYPE" varchar, 
                "LPT_UPDATECOUNT" numeric(38, 10), 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar,
                ETL_IS_DELETED boolean default false
                ); 