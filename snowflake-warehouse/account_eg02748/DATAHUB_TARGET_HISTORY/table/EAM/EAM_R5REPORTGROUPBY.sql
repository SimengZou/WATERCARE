CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_R5REPORTGROUPBY_DELETED(
                "RGP_DEFAULT" varchar, 
                "RGP_FUNCTION" varchar, 
                "RGP_GROUPFIELDS" varchar, 
                "RGP_LASTSAVED" datetime, 
                "RGP_LINE" numeric(38, 10), 
                "RGP_UPDATECOUNT" numeric(38, 10), 
                "RGP_UPDATED" datetime, 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar,
                ETL_IS_DELETED boolean default false
                ); 