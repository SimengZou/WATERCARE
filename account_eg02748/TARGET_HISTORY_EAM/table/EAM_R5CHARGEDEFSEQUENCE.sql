CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_R5CHARGEDEFSEQUENCE_DELETED(
            CDS_ACTUALSUBCAT varchar, 
            CDS_CATEGORY varchar, 
            CDS_LASTSAVED datetime, 
            CDS_LEVEL varchar, 
            CDS_SEQUENCE numeric(38, 10), 
            CDS_SUBCATEGORY varchar, 
            CDS_UPDATECOUNT numeric(38, 10), 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 