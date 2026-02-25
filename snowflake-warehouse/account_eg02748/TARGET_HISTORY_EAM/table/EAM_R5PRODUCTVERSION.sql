CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_R5PRODUCTVERSION_DELETED(
            PVS_BUILD varchar, 
            PVS_LASTSAVED datetime, 
            PVS_PATCH varchar, 
            PVS_PRODUCTCODE varchar, 
            PVS_PRODUCTDESC varchar, 
            PVS_UPDATECOUNT numeric(38, 10), 
            PVS_VERSION varchar, 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 