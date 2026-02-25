CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_BILLING_LINEITEM AS SELECT
                        src:ACCOUNTSERVICEKEY::integer AS ACCOUNTSERVICEKEY, 
                        src:ACTUALAMT::numeric(38, 10) AS ACTUALAMT, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:BILLKEY::integer AS BILLKEY, 
                        src:BUDGETEDAMOUNTFLAG::varchar AS BUDGETEDAMOUNTFLAG, 
                        src:COMMENTSKEY::integer AS COMMENTSKEY, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:DISCOUNTEXPIREDATE::datetime AS DISCOUNTEXPIREDATE, 
                        src:LINEITEMAMT::numeric(38, 10) AS LINEITEMAMT, 
                        src:LINEITEMKEY::integer AS LINEITEMKEY, 
                        src:LINEITEMSETUPKEY::integer AS LINEITEMSETUPKEY, 
                        src:LINEITEMUNITS::numeric(38, 10) AS LINEITEMUNITS, 
                        src:LINEITEMUSAGE::numeric(38, 10) AS LINEITEMUSAGE, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:PAIDSTAT::varchar AS PAIDSTAT, 
                        src:PENALIZEDFLAG::varchar AS PENALIZEDFLAG, 
                        src:PENALTYDATE::datetime AS PENALTYDATE, 
                        src:PENALTYPAYORDER::integer AS PENALTYPAYORDER, 
                        src:PENDTYPE::varchar AS PENDTYPE, 
                        src:PRINCIPALPAYORDER::integer AS PRINCIPALPAYORDER, 
                        src:PRINTORDER::integer AS PRINTORDER, 
                        src:PRINTTEXT::varchar AS PRINTTEXT, 
                        src:VARIANTFLAG::varchar AS VARIANTFLAG, 
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
    
                        
                src:LINEITEMKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:LINEITEMKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_BILLING_LINEITEM as strm))