CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_WSLCDRPROJ_XPROJCOMPFEESGD(
            ADDBY varchar, 
            ADDDTTM datetime, 
            DELETED boolean, 
            FEEAMOUNT numeric(38, 10), 
            FEECODE varchar, 
            FEEQUANTITY numeric(38, 10), 
            FEEVALUE numeric(38, 10), 
            MODBY varchar, 
            MODDTTM datetime, 
            VARIATION_ID integer, 
            XPROJCOMPFEESDPKEY integer, 
            XPROJCOMPFEESGDKEY integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 