CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_BILLING_REFUND AS SELECT
                        src:ACCOUNTKEY::integer AS ACCOUNTKEY, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:APPROVALLEVEL::integer AS APPROVALLEVEL, 
                        src:COMMENTKEY::integer AS COMMENTKEY, 
                        src:DELETED::boolean AS DELETED, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:PRIORITY::varchar AS PRIORITY, 
                        src:REFUNDAMOUNT::numeric(38, 10) AS REFUNDAMOUNT, 
                        src:REFUNDEDBY::varchar AS REFUNDEDBY, 
                        src:REFUNDEDDTTM::datetime AS REFUNDEDDTTM, 
                        src:REFUNDKEY::integer AS REFUNDKEY, 
                        src:REFUNDLINEITEMSETUPKEY::integer AS REFUNDLINEITEMSETUPKEY, 
                        src:REFUNDREASON::varchar AS REFUNDREASON, 
                        src:REFUNDSTATUS::varchar AS REFUNDSTATUS, 
                        src:REFUNDSTATUSBY::varchar AS REFUNDSTATUSBY, 
                        src:REFUNDSTATUSDTTM::datetime AS REFUNDSTATUSDTTM, 
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
    
                        
                src:REFUNDKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:REFUNDKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_BILLING_REFUND as strm))