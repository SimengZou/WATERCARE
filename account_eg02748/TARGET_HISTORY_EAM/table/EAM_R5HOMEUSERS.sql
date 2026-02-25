CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_R5HOMEUSERS_DELETED(
            HMU_AUTOFRESH varchar, 
            HMU_HOMCODE varchar, 
            HMU_HOMTYPE varchar, 
            HMU_LASTSAVED datetime, 
            HMU_SEQ numeric(38, 10), 
            HMU_TAB varchar, 
            HMU_UPDATECOUNT numeric(38, 10), 
            HMU_USER varchar, 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 