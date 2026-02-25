CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_CDR_BUILDING_BLDGLICREQ(
            ADDBY varchar, 
            ADDDTTM datetime, 
            APBLDGKEY integer, 
            APBLDGLICREQKEY integer, 
            DELETED boolean, 
            GROUPNO integer, 
            LICTYPE varchar, 
            MODBY varchar, 
            MODDTTM datetime, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 