CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_CDR_PLANNING_PLANHEARDETAILLOG(
            ADDBY varchar, 
            ADDDTTM datetime, 
            DELETED boolean, 
            DTLMODBY varchar, 
            DTLMODDTTM datetime, 
            MODBY varchar, 
            MODDTTM datetime, 
            PLANHEARDETAILLOGKEY integer, 
            PLANHEARDTLKEY integer, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 