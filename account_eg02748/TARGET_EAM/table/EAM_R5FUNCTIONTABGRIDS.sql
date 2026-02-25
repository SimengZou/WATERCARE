CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5FUNCTIONTABGRIDS(
            FTG_FUNCTION varchar, 
            FTG_GRIDID numeric(38, 10), 
            FTG_LASTSAVED datetime, 
            FTG_TAB varchar, 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 