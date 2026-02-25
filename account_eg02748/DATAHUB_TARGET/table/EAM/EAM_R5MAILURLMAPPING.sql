CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5MAILURLMAPPING(
                "MUM_FUNCTION" varchar, 
                "MUM_JSPFIELD" varchar, 
                "MUM_LASTSAVED" datetime, 
                "MUM_TABLE" varchar, 
                "MUM_TABLECOLUMN" varchar, 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar
                ); 