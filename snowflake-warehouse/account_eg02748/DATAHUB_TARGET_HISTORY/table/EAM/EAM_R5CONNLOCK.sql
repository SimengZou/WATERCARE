CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_R5CONNLOCK_DELETED(
                "CLK_LASTSAVED" datetime, 
                "CLK_OPERATIONID" varchar, 
                "CLK_SESSIONID" varchar, 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar,
                ETL_IS_DELETED boolean default false
                ); 