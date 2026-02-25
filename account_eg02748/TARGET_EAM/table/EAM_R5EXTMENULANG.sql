CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5EXTMENULANG(
            EML_EXTMENU varchar, 
            EML_LANG varchar, 
            EML_LASTSAVED datetime, 
            EML_TEXT varchar, 
            EML_TRANS varchar, 
            EML_UPDATECOUNT numeric(38, 10), 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 