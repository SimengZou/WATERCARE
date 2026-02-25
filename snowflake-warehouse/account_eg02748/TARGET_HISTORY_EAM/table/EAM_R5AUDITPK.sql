CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_R5AUDITPK_DELETED(
            APK_COLUMN varchar, 
            APK_DATATYPE varchar, 
            APK_LASTSAVED datetime, 
            APK_SEQNO numeric(38, 10), 
            APK_TABLE varchar, 
            APK_UPDATECOUNT numeric(38, 10), 
            APK_UPDATED datetime, 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 