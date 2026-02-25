CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_WSLASSETWATER_XMETERSTREAM(
            ADDBY varchar, 
            ADDDTTM datetime, 
            ADDRESS varchar, 
            COMPKEY integer, 
            DATALAKE_DELETED boolean, 
            MODBY varchar, 
            MODDTTM datetime, 
            OTHERMETER integer, 
            RELATIONSHIP varchar, 
            VARIATION_ID integer, 
            XMETERSTREAMKEY integer, 
            XWATERMETERDETAILKEY integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 