CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_R5PREFILTERLOOKUP_DELETED(
                "PFE_ELEMENTID" varchar, 
                "PFE_FILTERSTRXML" varchar, 
                "PFE_LASTSAVED" datetime, 
                "PFE_PAGENAME" varchar, 
                "PFE_UPDATECOUNT" numeric(38, 10), 
                "PFE_USERFILTER" varchar, 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar,
                ETL_IS_DELETED boolean default false
                ); 