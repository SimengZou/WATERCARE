CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_WSLCDRBUILD_XBUILDINSPESTDP(
            ADDBY varchar, 
            ADDDTTM datetime, 
            APBLDGINSPDTLKEY integer, 
            DELETED boolean, 
            ESTTOTAL numeric(38, 10), 
            MODBY varchar, 
            MODDTTM datetime, 
            VARIATION_ID integer, 
            XBUILDINSPESTDPKEY integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 