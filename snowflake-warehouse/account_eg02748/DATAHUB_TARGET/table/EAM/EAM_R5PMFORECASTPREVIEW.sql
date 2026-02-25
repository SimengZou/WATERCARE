CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5PMFORECASTPREVIEW(
                "PFV_LASTSAVED" datetime, 
                "PFV_OBJECT" varchar, 
                "PFV_OBJECT_ORG" varchar, 
                "PFV_PARENT" varchar, 
                "PFV_PARENT_ORG" varchar, 
                "PFV_SELECT" varchar, 
                "PFV_SESSIONID" numeric(38, 10), 
                "PFV_UPDATECOUNT" numeric(38, 10), 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar
                ); 