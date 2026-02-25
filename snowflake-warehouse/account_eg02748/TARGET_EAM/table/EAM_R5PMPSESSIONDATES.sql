CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5PMPSESSIONDATES(
            PPD_DUEDATE datetime, 
            PPD_ISCALDATE varchar, 
            PPD_ISPROJECTED varchar, 
            PPD_LASTSAVED datetime, 
            PPD_LINE numeric(38, 10), 
            PPD_PPSPK numeric(38, 10), 
            PPD_UPDATECOUNT numeric(38, 10), 
            PPD_WORKORDER varchar, 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 