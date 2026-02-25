CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5FUNCVIEWMAP(
                "FVM_FUNCTION" varchar, 
                "FVM_FUNCVIEW" varchar, 
                "FVM_GROUP" varchar, 
                "FVM_LASTSAVED" datetime, 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar
                ); 