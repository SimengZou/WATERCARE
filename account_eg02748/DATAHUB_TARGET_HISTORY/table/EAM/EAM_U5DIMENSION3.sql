CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_U5DIMENSION3_DELETED(
                "CREATED" datetime, 
                "CREATEDBY" varchar, 
                "DESCRIPTION" varchar, 
                "DIM3_CODE" varchar, 
                "DIM3_NOTUSED" varchar, 
                "LASTSAVED" datetime, 
                "UPDATECOUNT" numeric(38, 10), 
                "UPDATED" datetime, 
                "UPDATEDBY" varchar, 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar,
                ETL_IS_DELETED boolean default false
                ); 