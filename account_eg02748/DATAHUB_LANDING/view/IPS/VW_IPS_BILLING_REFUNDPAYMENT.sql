CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_BILLING_REFUNDPAYMENT AS SELECT
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
    
                        
                src:REFUNDPAYMENTKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:REFUNDPAYMENTKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_BILLING_REFUNDPAYMENT as strm))