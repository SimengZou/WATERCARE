CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5SCWORKORDERSREPORTED(
            CWR_DATE datetime, 
            CWR_LASTSAVED datetime, 
            CWR_MRC varchar, 
            CWR_ORG varchar, 
            CWR_WOSREPORTED numeric(38, 10), 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 