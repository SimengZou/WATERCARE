CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5OBJECTUPDATE(
                "OUP_DATE" datetime, 
                "OUP_LASTSAVED" datetime, 
                "OUP_NEWCODE" varchar, 
                "OUP_OLDCODE" varchar, 
                "OUP_ORG" varchar, 
                "OUP_PK" numeric(38, 10), 
                "OUP_USER" varchar, 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar
                ); 