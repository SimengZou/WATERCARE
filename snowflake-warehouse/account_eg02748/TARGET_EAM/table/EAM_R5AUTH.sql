CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5AUTH(
            AUT_CREATED datetime, 
            AUT_ENTITY varchar, 
            AUT_GROUP varchar, 
            AUT_LASTSAVED datetime, 
            AUT_RENTITY varchar, 
            AUT_STATNEW varchar, 
            AUT_STATUS varchar, 
            AUT_TYPE varchar, 
            AUT_UPDATECOUNT numeric(38, 10), 
            AUT_UPDATED datetime, 
            AUT_USER varchar, 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 