CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_R5PMPSESSIONLINES_DELETED(
            PPL_LASTSAVED datetime, 
            PPL_LINE numeric(38, 10), 
            PPL_NESTINGREF varchar, 
            PPL_OBJECT varchar, 
            PPL_OBJORG varchar, 
            PPL_PPM varchar, 
            PPL_PPOPK numeric(38, 10), 
            PPL_SESSIONID numeric(38, 10), 
            PPL_UPDATECOUNT numeric(38, 10), 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 