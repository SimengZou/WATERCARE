CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_WSLCDRBUILD_XBUILDINSPESTGD(
            ADDBY varchar, 
            ADDDTTM datetime, 
            DELETED boolean, 
            FEEAMOUNT numeric(38, 10), 
            FEECODE varchar, 
            FEEQUANTITY numeric(38, 10), 
            FEEUOM varchar, 
            FEEVALUE numeric(38, 10), 
            MODBY varchar, 
            MODDTTM datetime, 
            VARIATION_ID integer, 
            XBUILDINSPESTDPKEY integer, 
            XBUILDINSPESTGDKEY integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 