CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_BILLING_JOURNALSETUP AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:CRBGTNOFRMLA::integer AS CRBGTNOFRMLA, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:DRBGTNOFRMLA::integer AS DRBGTNOFRMLA, 
                        src:JOURNALSETUPKEY::integer AS JOURNALSETUPKEY, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:TRANSACTIONDESIGNATOR::integer AS TRANSACTIONDESIGNATOR, 
                        src:TRANSACTIONDESIGNATORDESC::varchar AS TRANSACTIONDESIGNATORDESC, 
                        src:TRANSACTIONTYPE::integer AS TRANSACTIONTYPE, 
                        src:TRANSACTIONTYPEDESC::varchar AS TRANSACTIONTYPEDESC, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
            CASE
                WHEN 'IPS' = 'LN'
                THEN src:"deleted"::BOOLEAN
                WHEN 'IPS' = 'IPS'
                THEN src:"DATALAKE_DELETED"::BOOLEAN
                ELSE src:"_DELETED"::BOOLEAN
            END as ETL_DELETED,
            etl_load_datetime,
            etl_load_metadatafilename,
            ETL_RANK,
            IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) as ETL_RANK_TIMESTAMP
            FROM 
            (
            select 
                src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROWNUMBER as ETL_RANK
                from
                (
                    SELECT
    
                        
                src:JOURNALSETUPKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:JOURNALSETUPKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_BILLING_JOURNALSETUP as strm))