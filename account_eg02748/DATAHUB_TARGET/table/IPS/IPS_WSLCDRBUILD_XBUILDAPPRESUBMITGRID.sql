CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_WSLCDRBUILD_XBUILDAPPRESUBMITGRID(
            ADDBY varchar, 
            ADDDTTM datetime, 
            DATALAKE_DELETED boolean, 
            DESCRIPTION varchar, 
            MODBY varchar, 
            MODDTTM datetime, 
            ONHOLDCODE varchar, 
            ONHOLDCOMMENTS varchar, 
            VARIATION_ID integer, 
            XBUILDAPPRESUBMITDPKEY integer, 
            XBUILDAPPRESUBMITGRIDKEY integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 