CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5ORGANIZATIONOPTIONS(
                "OPA_CODE" varchar, 
                "OPA_COMMENT" varchar, 
                "OPA_DESC" varchar, 
                "OPA_FIXED" varchar, 
                "OPA_LASTSAVED" datetime, 
                "OPA_MODULE" varchar, 
                "OPA_ORG" varchar, 
                "OPA_SYSTEM" varchar, 
                "OPA_TYPE" varchar, 
                "OPA_UPDATECOUNT" numeric(38, 10), 
                "OPA_VALIDVALUES" varchar, 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar
                ); 