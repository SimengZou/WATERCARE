CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_R5POINTTYPES_DELETED(
                "PTP_CLASS" varchar, 
                "PTP_CLASS_ORG" varchar, 
                "PTP_CODE" varchar, 
                "PTP_CREATED" datetime, 
                "PTP_DESC" varchar, 
                "PTP_LASTSAVED" datetime, 
                "PTP_UPDATECOUNT" numeric(38, 10), 
                "PTP_UPDATED" datetime, 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar,
                ETL_IS_DELETED boolean default false
                ); 