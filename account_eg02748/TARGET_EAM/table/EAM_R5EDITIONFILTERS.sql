CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5EDITIONFILTERS(
            EDF_CODE varchar, 
            EDF_FILTER varchar, 
            EDF_LASTSAVED datetime, 
            EDF_MEFLAG varchar, 
            EDF_TYPE varchar, 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 