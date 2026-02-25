CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_R5INSTALL_DELETED(
            INS_CODE varchar, 
            INS_COMMENT varchar, 
            INS_DESC varchar, 
            INS_EDCOMMENT varchar, 
            INS_EXTENDED varchar, 
            INS_FIXED varchar, 
            INS_LASTSAVED datetime, 
            INS_MODULE varchar, 
            INS_UPDATECOUNT numeric(38, 10), 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 