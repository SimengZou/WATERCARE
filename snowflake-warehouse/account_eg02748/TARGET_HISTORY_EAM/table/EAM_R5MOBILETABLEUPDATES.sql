CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_R5MOBILETABLEUPDATES_DELETED(
                "MTU_LASTSAVED" datetime, 
                "MTU_RENTITY" varchar, 
                "MTU_TABLENAME" varchar, 
                "MTU_UPDATED" datetime, 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar,
                ETL_IS_DELETED boolean default false
                ); 