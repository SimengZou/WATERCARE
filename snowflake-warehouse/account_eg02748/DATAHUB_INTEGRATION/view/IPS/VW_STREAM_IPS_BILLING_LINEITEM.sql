CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_BILLING_LINEITEM AS SELECT
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
                        src:VARIATION_ID::integer AS VARIATION_ID, src:VARIATION_ID::integer as ETL_SEQUENCE_NUMBER,
            CASE
                WHEN 'IPS' = 'LN'
                THEN src:"deleted"::BOOLEAN
                WHEN 'IPS' = 'IPS'
                THEN src:"DATALAKE_DELETED"::BOOLEAN
                ELSE src:"_DELETED"::BOOLEAN
            END as ETL_DELETED, 
            etl_load_datetime,
            etl_load_metadatafilename
            FROM 
            (
            select 
                src,
                etl_load_datetime,
                etl_load_metadatafilename
                from
                (
                    SELECT
                        
                src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:LINEITEMKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_LINEITEM as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACCOUNTSERVICEKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ACTUALAMT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:BILLKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:COMMENTSKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:DISCOUNTEXPIREDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:LINEITEMAMT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:LINEITEMKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:LINEITEMSETUPKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:LINEITEMUNITS), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:LINEITEMUSAGE), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PENALTYDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PENALTYPAYORDER), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PRINCIPALPAYORDER), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PRINTORDER), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null