CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_R5MOBILESYNCDELETE_DELETED(
            MSD_DELETED datetime, 
            MSD_KEYS varchar, 
            MSD_LASTSAVED datetime, 
            MSD_ORG varchar, 
            MSD_PK numeric(38, 10), 
            MSD_RENTITY varchar, 
            MSD_TABLENAME varchar, 
            MSD_UPDATECOUNT numeric(38, 10), 
            MSD_VALUES varchar, 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 