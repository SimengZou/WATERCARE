CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5AVAILABILITY(
            AVL_DATE datetime, 
            AVL_HIRHOURS numeric(38, 10), 
            AVL_LASTSAVED datetime, 
            AVL_MRC varchar, 
            AVL_NETHIRED numeric(38, 10), 
            AVL_NETHOURS numeric(38, 10), 
            AVL_OWNHOURS numeric(38, 10), 
            AVL_SPECIAL varchar, 
            AVL_TRADE varchar, 
            AVL_UPDATECOUNT numeric(38, 10), 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 