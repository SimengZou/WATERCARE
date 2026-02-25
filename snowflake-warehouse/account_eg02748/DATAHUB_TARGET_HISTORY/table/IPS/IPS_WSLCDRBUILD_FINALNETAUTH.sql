CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_WSLCDRBUILD_FINALNETAUTH(
            ADDBY varchar, 
            ADDDTTM datetime, 
            APBLDGAPPLDTLKEY integer, 
            ASBUILTS varchar, 
            CS3 varchar, 
            DATALAKE_DELETED boolean, 
            FINALNETAUTHKEY integer, 
            MODBY varchar, 
            MODDTTM datetime, 
            PHOTOS varchar, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 