CREATE OR REPLACE TABLE TARGET_HISTORY_EAM.EAM_R5ALERTWOFIELDMAPPING_DELETED(
            AWF_ALERT varchar, 
            AWF_BOILERTEXTNUMBER numeric(38, 10), 
            AWF_GRIDFIELD numeric(38, 10), 
            AWF_GRIDFIELDDATATYPE varchar, 
            AWF_LASTSAVED datetime, 
            AWF_UPDATECOUNT numeric(38, 10), 
            AWF_WOFIELD varchar, 
            _DELETED boolean, 
            ETL_DELETED boolean,
            ETL_LASTSAVED datetime, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 