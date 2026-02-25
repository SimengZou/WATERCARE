CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5ORGLOCATION(
                "OGL_BODGROUP" varchar, 
                "OGL_ENTERPRISELOCATION" varchar, 
                "OGL_LASTSAVED" datetime, 
                "OGL_ORG" varchar, 
                "OGL_UPDATECOUNT" numeric(38, 10), 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar
                ); 