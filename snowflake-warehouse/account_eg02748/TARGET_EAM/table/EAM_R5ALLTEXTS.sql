CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5ALLTEXTS(
            ATX_CODE varchar, 
            ATX_LANG varchar, 
            ATX_LASTMODIFIED datetime, 
            ATX_LASTSAVED datetime, 
            ATX_TEXT varchar, 
            ATX_TEXTTYPE varchar, 
            ATX_UPDATECOUNT numeric(38, 10), 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 