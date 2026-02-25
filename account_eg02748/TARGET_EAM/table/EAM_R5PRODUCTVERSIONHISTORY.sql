CREATE OR REPLACE TABLE TARGET_EAM.EAM_R5PRODUCTVERSIONHISTORY(
            PVH_BUILD varchar, 
            PVH_BUILDDATE varchar, 
            PVH_CHANGED datetime, 
            PVH_DESC varchar, 
            PVH_LASTSAVED datetime, 
            PVH_PATCH varchar, 
            PVH_PRODUCTCODE varchar, 
            PVH_UPDATECOUNT numeric(38, 10), 
            PVH_VERSION varchar, 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 