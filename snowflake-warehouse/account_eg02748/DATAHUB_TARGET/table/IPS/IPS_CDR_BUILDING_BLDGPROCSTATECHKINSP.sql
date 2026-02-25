CREATE OR REPLACE TABLE DATAHUB_TARGET.IPS_CDR_BUILDING_BLDGPROCSTATECHKINSP(
            ADDBY varchar, 
            ADDDTTM datetime, 
            APBLDGINSPTYPEKEY integer, 
            APBLDGPROCSTATECHKINSPKEY integer, 
            APBLDGPROCSTATECHKKEY integer, 
            DATALAKE_DELETED boolean, 
            MODBY varchar, 
            MODDTTM datetime, 
            RESULT varchar, 
            VARIATION_ID integer, 
            ETL_DELETED boolean,
            ETL_SEQUENCE_NUMBER integer, 
            etl_load_datetime timestamp,
            etl_load_metadatafilename varchar
            ); 