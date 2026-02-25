CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_BILLING_DEBITSCHEDULE AS SELECT
                        src:ACCOUNTDIRECTDEBITKEY::integer AS ACCOUNTDIRECTDEBITKEY, 
                        src:ACCOUNTKEY::integer AS ACCOUNTKEY, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:BILLKEYS::varchar AS BILLKEYS, 
                        src:DEBITSCHEDULEKEY::integer AS DEBITSCHEDULEKEY, 
                        src:DELETED::boolean AS DELETED, 
                        src:DIRECTDEBITRUN::integer AS DIRECTDEBITRUN, 
                        src:EXTRACTDATE::datetime AS EXTRACTDATE, 
                        src:LASTEXTRACTDATE::datetime AS LASTEXTRACTDATE, 
                        src:LATESTELIGIBLEBILL::integer AS LATESTELIGIBLEBILL, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:NEXTEXTRACTDATE::datetime AS NEXTEXTRACTDATE, 
                        src:PAYKEY::integer AS PAYKEY, 
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
    
                        
                src:DEBITSCHEDULEKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:DEBITSCHEDULEKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_BILLING_DEBITSCHEDULE as strm))