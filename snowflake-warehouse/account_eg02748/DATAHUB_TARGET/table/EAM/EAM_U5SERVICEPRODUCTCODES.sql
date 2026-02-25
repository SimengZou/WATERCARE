CREATE OR REPLACE TABLE TARGET_EAM.EAM_U5SERVICEPRODUCTCODES(
                "CREATED" datetime, 
                "CREATEDBY" varchar, 
                "LASTSAVED" datetime, 
                "SPC_CODE" varchar, 
                "SPC_DESC" varchar, 
                "UPDATECOUNT" numeric(38, 10), 
                "UPDATED" datetime, 
                "UPDATEDBY" varchar, 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar
                ); 