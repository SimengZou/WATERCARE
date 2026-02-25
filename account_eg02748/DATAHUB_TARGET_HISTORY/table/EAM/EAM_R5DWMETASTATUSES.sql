CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_R5DWMETASTATUSES_DELETED(
                "DMS_ENTITY" varchar, 
                "DMS_ENTITYBOT" numeric(38, 10), 
                "DMS_FIELD" varchar, 
                "DMS_LASTSAVED" datetime, 
                "DMS_STATUSENTITY" varchar, 
                "DMS_STATUSENTITYBOT" numeric(38, 10), 
                "DMS_TABLE" varchar, 
                "DMS_UPDATED" datetime, 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar,
                ETL_IS_DELETED boolean default false
                ); 