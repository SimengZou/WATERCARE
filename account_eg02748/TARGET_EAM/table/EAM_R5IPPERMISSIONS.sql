CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5IPPERMISSIONS(
            IPP_CODE numeric(38, 10), 
            IPP_FUNCTION numeric(38, 10), 
            IPP_LASTSAVED datetime, 
            IPP_MNEMONIC varchar, 
            IPP_UPDATECOUNT numeric(38, 10), 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 