CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_R5IPFUNCTIONS_DELETED(
                "IPF_CODE" numeric(38, 10), 
                "IPF_DESC" varchar, 
                "IPF_FIELD" varchar, 
                "IPF_LASTSAVED" datetime, 
                "IPF_UPDATECOUNT" numeric(38, 10), 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar,
                ETL_IS_DELETED boolean default false
                ); 