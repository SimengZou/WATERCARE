CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5CONNLOCK(
            CLK_LASTSAVED datetime, 
            CLK_OPERATIONID varchar, 
            CLK_SESSIONID varchar, 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 