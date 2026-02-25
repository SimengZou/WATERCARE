CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5MOBILETABLEUPDATES(
                "MTU_LASTSAVED" datetime, 
                "MTU_RENTITY" varchar, 
                "MTU_TABLENAME" varchar, 
                "MTU_UPDATED" datetime, 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar
                ); 