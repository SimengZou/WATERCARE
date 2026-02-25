CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_ASSETMANAGEMENT_ASSLOC(
            ADDBY varchar, 
            ADDDTTM datetime, 
            ADDRKEY integer, 
            ASSLOCKEY integer, 
            COMPKEY integer, 
            DATALAKE_DELETED boolean, 
            INSTDT datetime, 
            MODBY varchar, 
            MODDTTM datetime, 
            POSITION integer, 
            REMDT datetime, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 