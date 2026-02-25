CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_WSLCRM_XPLANNEDAUDITGD(
            ADDBY varchar, 
            ADDDTTM datetime, 
            CHANGEDBY varchar, 
            CHANGEDDATETIME datetime, 
            CHANGEDVALUE varchar, 
            DELETED boolean, 
            MODBY varchar, 
            MODDTTM datetime, 
            ORIGINALVALUE varchar, 
            TYPE varchar, 
            VARIATION_ID integer, 
            XBILLINGDATADPKEY integer, 
            XPLANNEDAUDITGDKEY integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 