CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5DWINDEXES(
                "IDX_FIELDS" varchar, 
                "IDX_INDEXNAME" varchar, 
                "IDX_INDEXTYPE" varchar, 
                "IDX_LASTSAVED" datetime, 
                "IDX_TABLE" varchar, 
                "IDX_TABLESPACE" varchar, 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar
                ); 