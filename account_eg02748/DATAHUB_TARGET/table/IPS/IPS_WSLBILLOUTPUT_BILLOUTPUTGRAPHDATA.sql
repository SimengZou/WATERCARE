CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_WSLBILLOUTPUT_BILLOUTPUTGRAPHDATA(
            ADDBY varchar, 
            ADDDTTM datetime, 
            AVRGCONSUMPTION numeric(38, 10), 
            BILLKEY integer, 
            BILLOUTPUTGRAPHDATAKEY integer, 
            DATALAKE_DELETED boolean, 
            GRCONSUMPTION numeric(38, 10), 
            GRDAYSBILLPER integer, 
            GRPERIOD varchar, 
            MODBY varchar, 
            MODDTTM datetime, 
            TYPE varchar, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 