CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_WSLCDRBUILD_XBUILDAPPESTDP(
            ADDBY varchar, 
            ADDDTTM datetime, 
            APBLDGAPPLDTLKEY integer, 
            APPESTTOTAL numeric(38, 10), 
            DELETED boolean, 
            MODBY varchar, 
            MODDTTM datetime, 
            PREQUOTETOTAL numeric(38, 10), 
            QUOTECOMPLETED varchar, 
            VARIATION_ID integer, 
            XBUILDAPPESTDPKEY integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 