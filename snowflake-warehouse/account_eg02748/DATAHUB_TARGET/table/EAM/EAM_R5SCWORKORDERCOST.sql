CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5SCWORKORDERCOST(
                "CWS_COST" numeric(38, 10), 
                "CWS_COSTDEFCURR" numeric(38, 10), 
                "CWS_DATE" datetime, 
                "CWS_JOBTYPE" varchar, 
                "CWS_LASTSAVED" datetime, 
                "CWS_ORG" varchar, 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar
                ); 