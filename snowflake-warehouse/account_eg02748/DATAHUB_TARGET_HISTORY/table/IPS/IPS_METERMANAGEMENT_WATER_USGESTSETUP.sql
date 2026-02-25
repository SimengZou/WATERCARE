CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_METERMANAGEMENT_WATER_USGESTSETUP(
            ADDBY varchar, 
            ADDDTTM datetime, 
            DEFCYCLEROUTERULEGRP integer, 
            DEFEXCHREMOVERULEGRP integer, 
            DEFSINGLECYCLERULEGRP integer, 
            DEFSINGLENONCYCLERULEGRP integer, 
            DELETED boolean, 
            MAXNUMBEREST integer, 
            MODBY varchar, 
            MODDTTM datetime, 
            USGESTSETUPKEY integer, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 