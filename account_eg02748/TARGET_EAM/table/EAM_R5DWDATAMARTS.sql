CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5DWDATAMARTS(
                "DMT_BOTNUMBER" numeric(38, 10), 
                "DMT_DESC" varchar, 
                "DMT_GRAIN" varchar, 
                "DMT_LASTSAVED" datetime, 
                "DMT_TABLE" varchar, 
                "DMT_TABLETYPE" varchar, 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar
                ); 