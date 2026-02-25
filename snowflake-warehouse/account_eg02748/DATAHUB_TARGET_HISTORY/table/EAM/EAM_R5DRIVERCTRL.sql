CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_R5DRIVERCTRL_DELETED(
                "DRV_ACTION" varchar, 
                "DRV_CODE" varchar, 
                "DRV_FREQUENCY" numeric(38, 10), 
                "DRV_LASTRAN" datetime, 
                "DRV_LASTSAVED" datetime, 
                "DRV_NEXTRUN" datetime, 
                "DRV_QUEUE" varchar, 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar,
                ETL_IS_DELETED boolean default false
                ); 