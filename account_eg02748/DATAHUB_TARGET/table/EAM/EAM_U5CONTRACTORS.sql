CREATE OR REPLACE TABLE TARGET_EAM.EAM_U5CONTRACTORS(
                "CON_CONTRACTORCODE" varchar, 
                "CON_CONTRACTORDESC" varchar, 
                "CON_SERVICEAREA" varchar, 
                "CREATED" datetime, 
                "CREATEDBY" varchar, 
                "LASTSAVED" datetime, 
                "UPDATECOUNT" numeric(38, 10), 
                "UPDATED" datetime, 
                "UPDATEDBY" varchar, 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar
                ); 