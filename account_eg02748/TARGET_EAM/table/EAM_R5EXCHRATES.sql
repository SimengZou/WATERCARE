CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5EXCHRATES(
            CRR_ACTIVE varchar, 
            CRR_CODE varchar, 
            CRR_CURR varchar, 
            CRR_END datetime, 
            CRR_EXCH numeric(38, 10), 
            CRR_INTERFACE datetime, 
            CRR_LASTSAVED datetime, 
            CRR_ORGCURR varchar, 
            CRR_SOURCECODE varchar, 
            CRR_SOURCESYSTEM varchar, 
            CRR_START datetime, 
            CRR_UPDATECOUNT numeric(38, 10), 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 