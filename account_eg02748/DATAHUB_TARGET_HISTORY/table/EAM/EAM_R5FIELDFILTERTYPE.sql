CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_R5FIELDFILTERTYPE_DELETED(
                "FFT_DEFAULTSCREENTYPE" varchar, 
                "FFT_FUNCTION" varchar, 
                "FFT_LASTSAVED" datetime, 
                "FFT_RTYPE" varchar, 
                "FFT_TYPE" varchar, 
                "FFT_UPDATECOUNT" numeric(38, 10), 
                "_DELETED" boolean, 
                ETL_DELETED boolean,
                ETL_LASTSAVED datetime, 
                etl_load_datetime timestamp,
                etl_load_metadatafilename varchar,
                ETL_IS_DELETED boolean default false
                ); 