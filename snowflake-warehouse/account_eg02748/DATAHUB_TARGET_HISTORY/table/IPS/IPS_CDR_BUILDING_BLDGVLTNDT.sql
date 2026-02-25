CREATE OR REPLACE TABLE DATAHUB_TARGET_HISTORY.IPS_DELETED_CDR_BUILDING_BLDGVLTNDT(
            ADDBY varchar, 
            ADDDTTM datetime, 
            APBLDGVLTNDTLKEY integer, 
            APBLDGVLTNKEY integer, 
            DELETED boolean, 
            DTLTYPE varchar, 
            MODBY varchar, 
            MODDTTM datetime, 
            QTY numeric(38, 10), 
            TYPE varchar, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar,
            ETL_IS_DELETED boolean default false
            ); 