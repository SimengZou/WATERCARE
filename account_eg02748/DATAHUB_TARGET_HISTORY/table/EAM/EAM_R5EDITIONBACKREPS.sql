CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_R5EDITIONBACKREPS_DELETED(
                "EBR_FUNCTION" varchar, 
                "EBR_LASTSAVED" datetime, 
                "EBR_MEFLAG" varchar, 
                "EBR_REPORT" varchar, 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar,
                ETL_IS_DELETED boolean default false
                ); 