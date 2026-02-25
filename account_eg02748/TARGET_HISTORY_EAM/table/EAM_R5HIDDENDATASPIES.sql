CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_R5HIDDENDATASPIES_DELETED(
            HDS_DATASPYID numeric(38, 10), 
            HDS_GROUP varchar, 
            HDS_LASTSAVED datetime, 
            HDS_UPDATECOUNT numeric(38, 10), 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 