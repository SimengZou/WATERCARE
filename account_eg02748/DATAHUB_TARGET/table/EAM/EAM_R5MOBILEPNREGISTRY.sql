CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5MOBILEPNREGISTRY(
                "MPN_APPID" varchar, 
                "MPN_CREATED" datetime, 
                "MPN_CREATEDBY" varchar, 
                "MPN_DEVICEID" varchar, 
                "MPN_LASTSAVED" datetime, 
                "MPN_PLATFORM" varchar, 
                "MPN_UPDATECOUNT" numeric(38, 10), 
                "MPN_UPDATED" datetime, 
                "MPN_UPDATEDBY" varchar, 
                "MPN_USER" varchar, 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar
                ); 