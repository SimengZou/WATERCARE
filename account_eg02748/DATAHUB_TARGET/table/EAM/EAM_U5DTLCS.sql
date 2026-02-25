CREATE OR REPLACE TABLE TARGET_EAM.EAM_U5DTLCS(
                "COLUMNNAME" varchar, 
                "CONSTRAINTNAME" varchar, 
                "CONSTRAINTTYPE" varchar, 
                "LASTSAVED" datetime, 
                "SEARCHCONDITION" varchar, 
                "TABLENAME" varchar, 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar
                ); 