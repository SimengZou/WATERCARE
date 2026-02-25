CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_R5PROPERTIES_DELETED(
                "PRO_CODE" varchar, 
                "PRO_CREATED" datetime, 
                "PRO_INCLUDEINGRIDS" varchar, 
                "PRO_LASTSAVED" datetime, 
                "PRO_MAX" varchar, 
                "PRO_MIN" varchar, 
                "PRO_RENTITY" varchar, 
                "PRO_TEXT" varchar, 
                "PRO_TYPE" varchar, 
                "PRO_UPDATECOUNT" numeric(38, 10), 
                "PRO_UPDATED" datetime, 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar,
                ETL_IS_DELETED boolean default false
                ); 