CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5TASKPRICES(
                "TPR_LASTSAVED" datetime, 
                "TPR_ORG" varchar, 
                "TPR_PRICE" numeric(38, 10), 
                "TPR_REVISION" numeric(38, 10), 
                "TPR_TASK" varchar, 
                "TPR_UPDATECOUNT" numeric(38, 10), 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar
                ); 