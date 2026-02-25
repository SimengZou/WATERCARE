CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_R5MAPSCONSENT_DELETED(
                "MCT_APPID" varchar, 
                "MCT_DEVICEID" varchar, 
                "MCT_LASTSAVED" datetime, 
                "MCT_LASTUPDATED" datetime, 
                "MCT_PRODUCT" varchar, 
                "MCT_UPDATECOUNT" numeric(38, 10), 
                "MCT_USERID" varchar, 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar,
                ETL_IS_DELETED boolean default false
                ); 