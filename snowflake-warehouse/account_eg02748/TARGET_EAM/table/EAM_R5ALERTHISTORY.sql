CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5ALERTHISTORY(
                "ALH_ALERT" varchar, 
                "ALH_CREATED" datetime, 
                "ALH_ENTITYCODE" varchar, 
                "ALH_ENTITYORG" varchar, 
                "ALH_ERRORMESSAGE" varchar, 
                "ALH_LASTSAVED" datetime, 
                "ALH_RESULTCODE" varchar, 
                "ALH_RESULTORG" varchar, 
                "ALH_RSTATUS" varchar, 
                "ALH_RTYPE" varchar, 
                "ALH_TEMPLATE" varchar, 
                "ALH_UPDATECOUNT" numeric(38, 10), 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar
                ); 