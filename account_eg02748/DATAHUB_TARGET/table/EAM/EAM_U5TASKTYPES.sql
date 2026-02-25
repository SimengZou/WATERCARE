CREATE OR REPLACE TABLE TARGET_EAM.EAM_U5TASKTYPES(
                "CREATED" datetime, 
                "CREATEDBY" varchar, 
                "DESCRIPTION" varchar, 
                "LASTSAVED" datetime, 
                "TSK_AUTO" varchar, 
                "TSK_CODE" varchar, 
                "UPDATECOUNT" numeric(38, 10), 
                "UPDATED" datetime, 
                "UPDATEDBY" varchar, 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar
                ); 