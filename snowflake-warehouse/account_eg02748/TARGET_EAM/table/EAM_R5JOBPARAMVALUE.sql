CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5JOBPARAMVALUE(
            JPV_CODE varchar, 
            JPV_KEY varchar, 
            JPV_LASTSAVED datetime, 
            JPV_LINE numeric(38, 10), 
            JPV_UPDATECOUNT numeric(38, 10), 
            JPV_VALUE varchar, 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 