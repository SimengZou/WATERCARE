CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5OPVARATTRIBUTES(
                "OAT_AUDITTIMESTAMP" datetime, 
                "OAT_AUDITUSER" varchar, 
                "OAT_DESC" varchar, 
                "OAT_ID" numeric(38, 10), 
                "OAT_LABEL" varchar, 
                "OAT_LASTSAVED" datetime, 
                "OAT_TYPE" numeric(38, 10), 
                "OAT_UPDATECOUNT" numeric(38, 10), 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar
                ); 