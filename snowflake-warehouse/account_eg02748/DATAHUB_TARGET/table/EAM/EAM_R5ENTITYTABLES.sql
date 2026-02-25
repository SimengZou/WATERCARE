CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5ENTITYTABLES(
                "ETT_LASTSAVED" datetime, 
                "ETT_MOS" varchar, 
                "ETT_RENTITY" varchar, 
                "ETT_UPDATECOUNT" numeric(38, 10), 
                "ETT_UPDATED" datetime, 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar
                ); 