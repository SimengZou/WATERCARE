CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_ASSETMANAGEMENT_COMPTYPE(
            ADDBY varchar, 
            ADDDTTM datetime, 
            CLASS integer, 
            COMPCODE varchar, 
            COMPDESC varchar, 
            COMPTYPE integer, 
            DATALAKE_DELETED boolean, 
            DETAILPAGECLASS varchar, 
            EXPDATE datetime, 
            MODBY varchar, 
            MODDTTM datetime, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 