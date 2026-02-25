CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_R5DWDATAMARTDIMENSIONS_DELETED(
                "DMD_DIMBUSINESSKEYFIELD" varchar, 
                "DMD_DIMTABLE" varchar, 
                "DMD_DMTTABLE" varchar, 
                "DMD_FACTJOINFIELD" varchar, 
                "DMD_LASTSAVED" datetime, 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar,
                ETL_IS_DELETED boolean default false
                ); 