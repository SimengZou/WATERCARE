CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_R5WSPRETURNPROMPTS_DELETED(
                "WPR_LASTSAVED" datetime, 
                "WPR_MOBILEQUERYCODE" varchar, 
                "WPR_QUERYCODE" varchar, 
                "WPR_SOURCESEQ" numeric(38, 10), 
                "WPR_TARGETSEQ" numeric(38, 10), 
                "WPR_UPDATECOUNT" numeric(38, 10), 
                "WPR_UPDATED" datetime, 
                "WPR_WSPCODE" varchar, 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar,
                ETL_IS_DELETED boolean default false
                ); 