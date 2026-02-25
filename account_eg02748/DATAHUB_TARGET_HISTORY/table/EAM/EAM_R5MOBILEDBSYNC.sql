CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_R5MOBILEDBSYNC_DELETED(
                "MDS_DBLASTUPDATEDDATE" datetime, 
                "MDS_DOWNLOAD" varchar, 
                "MDS_FILENAME" varchar, 
                "MDS_GRIDS_PROCESSED" numeric(38, 10), 
                "MDS_LASTSAVED" datetime, 
                "MDS_ORG" varchar, 
                "MDS_STATUS" varchar, 
                "MDS_STATUS_MSG" varchar, 
                "MDS_SYNCID" numeric(38, 10), 
                "MDS_UPDATECOUNT" numeric(38, 10), 
                "MDS_USER" varchar, 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar,
                ETL_IS_DELETED boolean default false
                ); 