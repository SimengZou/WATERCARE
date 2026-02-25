CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5ALERTGRIDDATA(
                "AGD_ALERT" varchar, 
                "AGD_DATASPYID" numeric(38, 10), 
                "AGD_DATE" datetime, 
                "AGD_DESCRIPTION" varchar, 
                "AGD_GRIDID" numeric(38, 10), 
                "AGD_LASTSAVED" datetime, 
                "AGD_PK" varchar, 
                "AGD_RECIPIENT" varchar, 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar
                ); 