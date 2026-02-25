CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5DESCRIPTIONS(
                "DES_CODE" varchar, 
                "DES_ENTITY" varchar, 
                "DES_LANG" varchar, 
                "DES_LASTSAVED" datetime, 
                "DES_ORG" varchar, 
                "DES_RENTITY" varchar, 
                "DES_RTYPE" varchar, 
                "DES_TEXT" varchar, 
                "DES_TRANS" varchar, 
                "DES_TYPE" varchar, 
                "DES_UPDATECOUNT" numeric(38, 10), 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar
                ); 