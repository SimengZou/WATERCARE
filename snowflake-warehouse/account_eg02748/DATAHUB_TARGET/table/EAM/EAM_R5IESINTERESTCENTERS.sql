CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5IESINTERESTCENTERS(
                "ENI_CATEGORY" varchar, 
                "ENI_CODE" varchar, 
                "ENI_DESC" varchar, 
                "ENI_LASTSAVED" datetime, 
                "ENI_NOTUSED" varchar, 
                "ENI_UPDATECOUNT" numeric(38, 10), 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar
                ); 