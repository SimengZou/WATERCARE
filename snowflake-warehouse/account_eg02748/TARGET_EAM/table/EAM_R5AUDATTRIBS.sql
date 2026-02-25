CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5AUDATTRIBS(
            AAT_CODE numeric(38, 10), 
            AAT_COLUMN varchar, 
            AAT_COMMENT varchar, 
            AAT_DELETE varchar, 
            AAT_ENTEREDBY varchar, 
            AAT_INSERT varchar, 
            AAT_LASTSAVED datetime, 
            AAT_TABLE varchar, 
            AAT_TEXT varchar, 
            AAT_UPDATE varchar, 
            AAT_UPDATECOUNT numeric(38, 10), 
            AAT_UPDATED datetime, 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 