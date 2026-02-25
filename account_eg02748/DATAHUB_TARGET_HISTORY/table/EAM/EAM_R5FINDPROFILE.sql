CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_R5FINDPROFILE_DELETED(
                "FPF_CODE" varchar, 
                "FPF_LASTSAVED" datetime, 
                "FPF_PROFILE" varchar, 
                "FPF_PROFILE_ORG" varchar, 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar,
                ETL_IS_DELETED boolean default false
                ); 