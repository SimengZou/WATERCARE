CREATE OR REPLACE TABLE TARGET_EAM.EAM_U5ASSETCLASS_ACTIVITY(
                "ACAA_ASSETCLASS" varchar, 
                "ACAA_ASSETCLASS_DESC" varchar, 
                "ACAA_NOTUSED" varchar, 
                "ACAA_TASKTYPE" varchar, 
                "ACAA_TASKTYPE_DESC" varchar, 
                "CREATED" datetime, 
                "CREATEDBY" varchar, 
                "LASTSAVED" datetime, 
                "UPDATECOUNT" numeric(38, 10), 
                "UPDATED" datetime, 
                "UPDATEDBY" varchar, 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar
                ); 