CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5JSPFIELDS(
                "JFD_JSINCLUDES" varchar, 
                "JFD_JSPFILE" varchar, 
                "JFD_LASTSAVED" datetime, 
                "JFD_UPDATECOUNT" numeric(38, 10), 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar
                ); 