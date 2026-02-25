CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_R5SEQINSTALLMAPPING_DELETED(
                "SIM_INSTALLPARAMETER" varchar, 
                "SIM_LASTSAVED" datetime, 
                "SIM_SEQUENCE" varchar, 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar,
                ETL_IS_DELETED boolean default false
                ); 