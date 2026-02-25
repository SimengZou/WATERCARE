CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5DEPARTMENTSECURITY(
            DSE_LASTSAVED datetime, 
            DSE_MRC varchar, 
            DSE_READONLY varchar, 
            DSE_UPDATECOUNT numeric(38, 10), 
            DSE_UPDATED datetime, 
            DSE_USER varchar, 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 