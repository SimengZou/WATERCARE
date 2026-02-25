CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_CDR_APRELATEDLINK(
            ADDBY varchar, 
            ADDDTTM datetime, 
            APKEY1 integer, 
            APKEY2 integer, 
            APPLICATIONRELATEDLINKKEY integer, 
            CAPACITY varchar, 
            DELETED boolean, 
            MODBY varchar, 
            MODDTTM datetime, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 