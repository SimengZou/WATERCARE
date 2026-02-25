CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_R5LOTS_DELETED(
            LOT_BREAKUPKITTRANS varchar, 
            LOT_BUILDKITTRANS varchar, 
            LOT_CLASS varchar, 
            LOT_CLASS_ORG varchar, 
            LOT_CODE varchar, 
            LOT_DESC varchar, 
            LOT_EXPIRED datetime, 
            LOT_LASTSAVED datetime, 
            LOT_MANLOT varchar, 
            LOT_ORG varchar, 
            LOT_UPDATECOUNT numeric(38, 10), 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 