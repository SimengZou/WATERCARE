CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_R5HOMEGROUPS_DELETED(
                "HMG_AUTOFRESH" varchar, 
                "HMG_GROUP" varchar, 
                "HMG_HOMCODE" varchar, 
                "HMG_HOMTYPE" varchar, 
                "HMG_LASTSAVED" datetime, 
                "HMG_SEQ" numeric(38, 10), 
                "HMG_TAB" varchar, 
                "HMG_UPDATECOUNT" numeric(38, 10), 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar,
                ETL_IS_DELETED boolean default false
                ); 