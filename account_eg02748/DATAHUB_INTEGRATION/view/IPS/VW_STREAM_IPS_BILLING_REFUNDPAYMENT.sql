CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_BILLING_REFUNDPAYMENT AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:BANKINGKEY::integer AS BANKINGKEY, 
                        src:CONTACTKEY::integer AS CONTACTKEY, 
                        src:DELETED::boolean AS DELETED, 
                        src:ESCROWACCOUNTKEY::integer AS ESCROWACCOUNTKEY, 
                        src:MAILCITY::varchar AS MAILCITY, 
                        src:MAILCOUNTRY::varchar AS MAILCOUNTRY, 
                        src:MAILINGADDRESSLINE1::varchar AS MAILINGADDRESSLINE1, 
                        src:MAILINGADDRESSLINE2::varchar AS MAILINGADDRESSLINE2, 
                        src:MAILSTATE::varchar AS MAILSTATE, 
                        src:MAILZIP::varchar AS MAILZIP, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:PAYAMOUNT::numeric(38, 10) AS PAYAMOUNT, 
                        src:PAYMENTMETHOD::varchar AS PAYMENTMETHOD, 
                        src:REFUNDCHECKISSUEDDTTM::datetime AS REFUNDCHECKISSUEDDTTM, 
                        src:REFUNDCHECKISSUEDTO::varchar AS REFUNDCHECKISSUEDTO, 
                        src:REFUNDCHECKNO::varchar AS REFUNDCHECKNO, 
                        src:REFUNDKEY::integer AS REFUNDKEY, 
                        src:REFUNDPAYMENTKEY::integer AS REFUNDPAYMENTKEY, 
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
                                        
                src:REFUNDPAYMENTKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_REFUNDPAYMENT as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:BANKINGKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CONTACTKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ESCROWACCOUNTKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PAYAMOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:REFUNDCHECKISSUEDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:REFUNDKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:REFUNDPAYMENTKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null