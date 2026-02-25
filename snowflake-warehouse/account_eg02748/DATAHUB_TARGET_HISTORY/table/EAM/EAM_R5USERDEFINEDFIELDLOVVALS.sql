CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_R5USERDEFINEDFIELDLOVVALS_DELETED(
                "UDL_CODE" varchar, 
                "UDL_DESC" varchar, 
                "UDL_FIELD" varchar, 
                "UDL_LASTSAVED" datetime, 
                "UDL_NOTUSED" varchar, 
                "UDL_RENTITY" varchar, 
                "UDL_UPDATECOUNT" numeric(38, 10), 
                "UDL_UPDATED" datetime, 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar,
                ETL_IS_DELETED boolean default false
                ); 