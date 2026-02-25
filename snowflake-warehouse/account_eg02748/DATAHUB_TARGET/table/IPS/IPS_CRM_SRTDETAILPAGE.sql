CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_CRM_SRTDETAILPAGE(
            ADDBY varchar, 
            ADDDTTM datetime, 
            DELETED boolean, 
            DETAILPAGEKEY integer, 
            MODBY varchar, 
            MODDTTM datetime, 
            SRTDETAILPAGEKEY integer, 
            SRTYPE integer, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 