CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_R5UOMS_DELETED(
                "UOM_CLASS" varchar, 
                "UOM_CLASS_ORG" varchar, 
                "UOM_CODE" varchar, 
                "UOM_CREATED" datetime, 
                "UOM_DESC" varchar, 
                "UOM_LASTSAVED" datetime, 
                "UOM_NOTUSED" varchar, 
                "UOM_SOAUOM" varchar, 
                "UOM_UPDATECOUNT" numeric(38, 10), 
                "UOM_UPDATED" datetime, 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar,
                ETL_IS_DELETED boolean default false
                ); 