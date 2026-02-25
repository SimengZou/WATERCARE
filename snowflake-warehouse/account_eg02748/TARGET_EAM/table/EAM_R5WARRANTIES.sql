CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5WARRANTIES(
                "WAR_DESC" varchar, 
                "WAR_EXPIRATION" datetime, 
                "WAR_ID" varchar, 
                "WAR_LASTSAVED" datetime, 
                "WAR_OBJECT" varchar, 
                "WAR_OBJECT_ORG" varchar, 
                "WAR_OBTYPE" varchar, 
                "WAR_START" datetime, 
                "WAR_SUPPLIER" varchar, 
                "WAR_SUPPLIER_ORG" varchar, 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar
                ); 