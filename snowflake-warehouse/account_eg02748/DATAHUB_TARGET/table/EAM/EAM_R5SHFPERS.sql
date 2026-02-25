CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5SHFPERS(
                "SHP_END" datetime, 
                "SHP_LASTSAVED" datetime, 
                "SHP_PERSON" varchar, 
                "SHP_SHIFT" varchar, 
                "SHP_START" datetime, 
                "SHP_UPDATECOUNT" numeric(38, 10), 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar
                ); 