CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5ENTAPPHEADER(
                "EAH_APPDATE" datetime, 
                "EAH_APPLIST" varchar, 
                "EAH_CODE" varchar, 
                "EAH_DATE" datetime, 
                "EAH_ENTITY" varchar, 
                "EAH_LASTSAVED" datetime, 
                "EAH_PK" numeric(38, 10), 
                "EAH_REASON" varchar, 
                "EAH_RENTITY" varchar, 
                "EAH_REVISION" numeric(38, 10), 
                "EAH_UPDATECOUNT" numeric(38, 10), 
                "EAH_USER" varchar, 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar
                ); 