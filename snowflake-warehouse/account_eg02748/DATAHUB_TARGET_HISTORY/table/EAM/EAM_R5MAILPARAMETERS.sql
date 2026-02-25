CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_R5MAILPARAMETERS_DELETED(
                "MAP_ATTRIBPK" varchar, 
                "MAP_COLUMN" varchar, 
                "MAP_LASTSAVED" datetime, 
                "MAP_PARAMETER" numeric(38, 10), 
                "MAP_REPORTPARAMETER" numeric(38, 10), 
                "MAP_TABLE" varchar, 
                "MAP_TEMPLATE" varchar, 
                "MAP_UPDATECOUNT" numeric(38, 10), 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar,
                ETL_IS_DELETED boolean default false
                ); 