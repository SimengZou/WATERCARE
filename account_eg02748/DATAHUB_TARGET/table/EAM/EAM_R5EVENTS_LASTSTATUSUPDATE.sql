CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5EVENTS_LASTSTATUSUPDATE(
                "ELU_CODE" varchar, 
                "ELU_LASTSAVED" datetime, 
                "ELU_LASTSTATUSUPDATE" datetime, 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar
                ); 