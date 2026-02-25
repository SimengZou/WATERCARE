CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_R5TRACKINGCTRL_DELETED(
            TKC_COLUMN varchar, 
            TKC_DATARTYPE varchar, 
            TKC_DATATYPE varchar, 
            TKC_DISPLAYSEQ numeric(38, 10), 
            TKC_INTERFACERTYPE varchar, 
            TKC_INTERFACETYPE varchar, 
            TKC_LASTSAVED datetime, 
            TKC_LENGTH numeric(38, 10), 
            TKC_RCOLUMN varchar, 
            TKC_UPLOADCOLUMN varchar, 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 