CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_R5DWDIMENSIONS_DELETED(
                "DIM_BOTNUMBER" numeric(38, 10), 
                "DIM_CREATEKEYSEQUENCE" varchar, 
                "DIM_DESC" varchar, 
                "DIM_KEYFIELD" varchar, 
                "DIM_LASTSAVED" datetime, 
                "DIM_SURROGATEKEYLOOKUPTBL" varchar, 
                "DIM_TABLE" varchar, 
                "DIM_TABLETYPE" varchar, 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar,
                ETL_IS_DELETED boolean default false
                ); 