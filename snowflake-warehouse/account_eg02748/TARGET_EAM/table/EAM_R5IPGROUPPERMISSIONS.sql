CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5IPGROUPPERMISSIONS(
            IPG_ACTIVE varchar, 
            IPG_GROUP varchar, 
            IPG_LASTSAVED datetime, 
            IPG_PERMISSION numeric(38, 10), 
            IPG_UPDATECOUNT numeric(38, 10), 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 