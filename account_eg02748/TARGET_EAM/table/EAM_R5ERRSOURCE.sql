CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5ERRSOURCE(
            ERS_CODE varchar, 
            ERS_DESC varchar, 
            ERS_LASTSAVED datetime, 
            ERS_NAME varchar, 
            ERS_NUMBER numeric(38, 10), 
            ERS_SOURCE varchar, 
            ERS_TYPE varchar, 
            ERS_UPDATECOUNT numeric(38, 10), 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 