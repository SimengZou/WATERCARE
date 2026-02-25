CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5FUNNOPERM(
                "FPN_FUNCTION" varchar, 
                "FPN_LASTSAVED" datetime, 
                "FPN_NODELETE" varchar, 
                "FPN_NOINSERT" varchar, 
                "FPN_NOSELECT" varchar, 
                "FPN_NOUPDATE" varchar, 
                "FPN_UPDATECOUNT" numeric(38, 10), 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar
                ); 