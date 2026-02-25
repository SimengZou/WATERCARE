CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5RCODES(
            RCO_DESC varchar, 
            RCO_LASTSAVED datetime, 
            RCO_RCODE varchar, 
            RCO_RENTITY varchar, 
            RCO_TEXTCODE varchar, 
            RCO_UPDATECOUNT numeric(38, 10), 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 