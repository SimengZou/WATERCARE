CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5DWFACTFIELDS(
                "FFL_ADDITIVITY" varchar, 
                "FFL_BOTNUMBER" numeric(38, 10), 
                "FFL_DESC" varchar, 
                "FFL_DMTTABLE" varchar, 
                "FFL_FIELD" varchar, 
                "FFL_FIELDID" numeric(38, 10), 
                "FFL_LASTSAVED" datetime, 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar
                ); 