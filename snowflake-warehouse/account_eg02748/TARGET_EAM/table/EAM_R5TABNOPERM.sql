CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5TABNOPERM(
            TPN_FUNCTION varchar, 
            TPN_LASTSAVED datetime, 
            TPN_NODELETE varchar, 
            TPN_NOINSERT varchar, 
            TPN_NOSELECT varchar, 
            TPN_NOUPDATE varchar, 
            TPN_TAB varchar, 
            TPN_UPDATECOUNT numeric(38, 10), 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 