CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_CASHIERING_MERCHANTACCOUNT AS SELECT
                        src:ACCOUNTID::varchar AS ACCOUNTID, 
                        src:ACCOUNTNAME::varchar AS ACCOUNTNAME, 
                        src:ACCOUNTPASSWORD::varchar AS ACCOUNTPASSWORD, 
                        src:ACCOUNTUSERNAME::varchar AS ACCOUNTUSERNAME, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:AUTHHANDLERKEY::integer AS AUTHHANDLERKEY, 
                        src:AUTHORIZECONFIGKEY::integer AS AUTHORIZECONFIGKEY, 
                        src:AUTHORIZEPAYMENTFORMULAKEY::integer AS AUTHORIZEPAYMENTFORMULAKEY, 
                        src:CARDNUMBERFORMAT::integer AS CARDNUMBERFORMAT, 
                        src:CARDNUMBERFORMATOVR::varchar AS CARDNUMBERFORMATOVR, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:DESCRIPTION::varchar AS DESCRIPTION, 
                        src:EFFECTIVEDATE::datetime AS EFFECTIVEDATE, 
                        src:ENCRYPTCUSTPROP::varchar AS ENCRYPTCUSTPROP, 
                        src:EXPIREDATE::datetime AS EXPIREDATE, 
                        src:HOSTADDR::varchar AS HOSTADDR, 
                        src:HOSTPORT::integer AS HOSTPORT, 
                        src:INQCONFIGKEY::integer AS INQCONFIGKEY, 
                        src:INQHANDLERKEY::integer AS INQHANDLERKEY, 
                        src:INQREVERSEDCONFIGKEY::integer AS INQREVERSEDCONFIGKEY, 
                        src:INQREVERSEDHANDLERKEY::integer AS INQREVERSEDHANDLERKEY, 
                        src:ISCHECKING::varchar AS ISCHECKING, 
                        src:ISCREDIT::varchar AS ISCREDIT, 
                        src:ISDEBIT::varchar AS ISDEBIT, 
                        src:ISECOMMERCE::varchar AS ISECOMMERCE, 
                        src:MERCHANTACCOUNTKEY::integer AS MERCHANTACCOUNTKEY, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:PAYMENTGATEWAYPROVIDER::varchar AS PAYMENTGATEWAYPROVIDER, 
                        src:PAYMENTPROCESSING::integer AS PAYMENTPROCESSING, 
                        src:PREAUTHHANDLERKEY::integer AS PREAUTHHANDLERKEY, 
                        src:PREAUTHORIZECONFIGKEY::integer AS PREAUTHORIZECONFIGKEY, 
                        src:PREPCONFIGKEY::integer AS PREPCONFIGKEY, 
                        src:PREPHANDLERKEY::integer AS PREPHANDLERKEY, 
                        src:REDIRECTURLCONFIGKEY::integer AS REDIRECTURLCONFIGKEY, 
                        src:REDIRECTURLHANDLERKEY::integer AS REDIRECTURLHANDLERKEY, 
                        src:REVERSECONFIGKEY::integer AS REVERSECONFIGKEY, 
                        src:REVERSEHANDLERKEY::integer AS REVERSEHANDLERKEY, 
                        src:REVERSEPAYMENTFORMULAKEY::integer AS REVERSEPAYMENTFORMULAKEY, 
                        src:TIMEOUT::integer AS TIMEOUT, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:VOIDAPPROVEDTRANSACTIONONLY::varchar AS VOIDAPPROVEDTRANSACTIONONLY, 
                        src:VOIDCONFIGKEY::integer AS VOIDCONFIGKEY, 
                        src:VOIDHANDLERKEY::integer AS VOIDHANDLERKEY, 
                        src:WEBFORMCONFIGKEY::integer AS WEBFORMCONFIGKEY, 
                        src:WEBFORMHANDLERKEY::integer AS WEBFORMHANDLERKEY, src:VARIATION_ID::integer as ETL_SEQUENCE_NUMBER,
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
                                        
                src:MERCHANTACCOUNTKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_CASHIERING_MERCHANTACCOUNT as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:AUTHHANDLERKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:AUTHORIZECONFIGKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:AUTHORIZEPAYMENTFORMULAKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CARDNUMBERFORMAT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EFFECTIVEDATE), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EXPIREDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:HOSTPORT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:INQCONFIGKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:INQHANDLERKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:INQREVERSEDCONFIGKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:INQREVERSEDHANDLERKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:MERCHANTACCOUNTKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PAYMENTPROCESSING), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PREAUTHHANDLERKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PREAUTHORIZECONFIGKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PREPCONFIGKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PREPHANDLERKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:REDIRECTURLCONFIGKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:REDIRECTURLHANDLERKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:REVERSECONFIGKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:REVERSEHANDLERKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:REVERSEPAYMENTFORMULAKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TIMEOUT), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VOIDCONFIGKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VOIDHANDLERKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:WEBFORMCONFIGKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:WEBFORMHANDLERKEY), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null