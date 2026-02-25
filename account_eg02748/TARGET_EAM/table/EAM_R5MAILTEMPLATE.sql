CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5MAILTEMPLATE(
            MAT_CODE varchar, 
            MAT_DESC varchar, 
            MAT_EMAIL varchar, 
            MAT_FROMEMAIL varchar, 
            MAT_LASTSAVED datetime, 
            MAT_MAIL varchar, 
            MAT_NOTEBOOKEMAILS varchar, 
            MAT_PUSHNOTIFICATION varchar, 
            MAT_REPORT varchar, 
            MAT_SUBJECT varchar, 
            MAT_TEXT varchar, 
            MAT_UPDATECOUNT numeric(38, 10), 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 