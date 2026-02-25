CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_CASHIERING_MERCHANTACCOUNT_ERROR AS SELECT src, 'IPS_CASHIERING_MERCHANTACCOUNT' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AUTHHANDLERKEY), '0'), 38, 10) is null and 
                    src:AUTHHANDLERKEY is not null) THEN
                    'AUTHHANDLERKEY contains a non-numeric value : \'' || AS_VARCHAR(src:AUTHHANDLERKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AUTHORIZECONFIGKEY), '0'), 38, 10) is null and 
                    src:AUTHORIZECONFIGKEY is not null) THEN
                    'AUTHORIZECONFIGKEY contains a non-numeric value : \'' || AS_VARCHAR(src:AUTHORIZECONFIGKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AUTHORIZEPAYMENTFORMULAKEY), '0'), 38, 10) is null and 
                    src:AUTHORIZEPAYMENTFORMULAKEY is not null) THEN
                    'AUTHORIZEPAYMENTFORMULAKEY contains a non-numeric value : \'' || AS_VARCHAR(src:AUTHORIZEPAYMENTFORMULAKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CARDNUMBERFORMAT), '0'), 38, 10) is null and 
                    src:CARDNUMBERFORMAT is not null) THEN
                    'CARDNUMBERFORMAT contains a non-numeric value : \'' || AS_VARCHAR(src:CARDNUMBERFORMAT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EFFECTIVEDATE), '1900-01-01')) is null and 
                    src:EFFECTIVEDATE is not null) THEN
                    'EFFECTIVEDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:EFFECTIVEDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EXPIREDATE), '1900-01-01')) is null and 
                    src:EXPIREDATE is not null) THEN
                    'EXPIREDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:EXPIREDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HOSTPORT), '0'), 38, 10) is null and 
                    src:HOSTPORT is not null) THEN
                    'HOSTPORT contains a non-numeric value : \'' || AS_VARCHAR(src:HOSTPORT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INQCONFIGKEY), '0'), 38, 10) is null and 
                    src:INQCONFIGKEY is not null) THEN
                    'INQCONFIGKEY contains a non-numeric value : \'' || AS_VARCHAR(src:INQCONFIGKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INQHANDLERKEY), '0'), 38, 10) is null and 
                    src:INQHANDLERKEY is not null) THEN
                    'INQHANDLERKEY contains a non-numeric value : \'' || AS_VARCHAR(src:INQHANDLERKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INQREVERSEDCONFIGKEY), '0'), 38, 10) is null and 
                    src:INQREVERSEDCONFIGKEY is not null) THEN
                    'INQREVERSEDCONFIGKEY contains a non-numeric value : \'' || AS_VARCHAR(src:INQREVERSEDCONFIGKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INQREVERSEDHANDLERKEY), '0'), 38, 10) is null and 
                    src:INQREVERSEDHANDLERKEY is not null) THEN
                    'INQREVERSEDHANDLERKEY contains a non-numeric value : \'' || AS_VARCHAR(src:INQREVERSEDHANDLERKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MERCHANTACCOUNTKEY), '0'), 38, 10) is null and 
                    src:MERCHANTACCOUNTKEY is not null) THEN
                    'MERCHANTACCOUNTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:MERCHANTACCOUNTKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYMENTPROCESSING), '0'), 38, 10) is null and 
                    src:PAYMENTPROCESSING is not null) THEN
                    'PAYMENTPROCESSING contains a non-numeric value : \'' || AS_VARCHAR(src:PAYMENTPROCESSING) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PREAUTHHANDLERKEY), '0'), 38, 10) is null and 
                    src:PREAUTHHANDLERKEY is not null) THEN
                    'PREAUTHHANDLERKEY contains a non-numeric value : \'' || AS_VARCHAR(src:PREAUTHHANDLERKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PREAUTHORIZECONFIGKEY), '0'), 38, 10) is null and 
                    src:PREAUTHORIZECONFIGKEY is not null) THEN
                    'PREAUTHORIZECONFIGKEY contains a non-numeric value : \'' || AS_VARCHAR(src:PREAUTHORIZECONFIGKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PREPCONFIGKEY), '0'), 38, 10) is null and 
                    src:PREPCONFIGKEY is not null) THEN
                    'PREPCONFIGKEY contains a non-numeric value : \'' || AS_VARCHAR(src:PREPCONFIGKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PREPHANDLERKEY), '0'), 38, 10) is null and 
                    src:PREPHANDLERKEY is not null) THEN
                    'PREPHANDLERKEY contains a non-numeric value : \'' || AS_VARCHAR(src:PREPHANDLERKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REDIRECTURLCONFIGKEY), '0'), 38, 10) is null and 
                    src:REDIRECTURLCONFIGKEY is not null) THEN
                    'REDIRECTURLCONFIGKEY contains a non-numeric value : \'' || AS_VARCHAR(src:REDIRECTURLCONFIGKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REDIRECTURLHANDLERKEY), '0'), 38, 10) is null and 
                    src:REDIRECTURLHANDLERKEY is not null) THEN
                    'REDIRECTURLHANDLERKEY contains a non-numeric value : \'' || AS_VARCHAR(src:REDIRECTURLHANDLERKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REVERSECONFIGKEY), '0'), 38, 10) is null and 
                    src:REVERSECONFIGKEY is not null) THEN
                    'REVERSECONFIGKEY contains a non-numeric value : \'' || AS_VARCHAR(src:REVERSECONFIGKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REVERSEHANDLERKEY), '0'), 38, 10) is null and 
                    src:REVERSEHANDLERKEY is not null) THEN
                    'REVERSEHANDLERKEY contains a non-numeric value : \'' || AS_VARCHAR(src:REVERSEHANDLERKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REVERSEPAYMENTFORMULAKEY), '0'), 38, 10) is null and 
                    src:REVERSEPAYMENTFORMULAKEY is not null) THEN
                    'REVERSEPAYMENTFORMULAKEY contains a non-numeric value : \'' || AS_VARCHAR(src:REVERSEPAYMENTFORMULAKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TIMEOUT), '0'), 38, 10) is null and 
                    src:TIMEOUT is not null) THEN
                    'TIMEOUT contains a non-numeric value : \'' || AS_VARCHAR(src:TIMEOUT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) THEN
                    'VARIATION_ID contains a non-numeric value : \'' || AS_VARCHAR(src:VARIATION_ID) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VOIDCONFIGKEY), '0'), 38, 10) is null and 
                    src:VOIDCONFIGKEY is not null) THEN
                    'VOIDCONFIGKEY contains a non-numeric value : \'' || AS_VARCHAR(src:VOIDCONFIGKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VOIDHANDLERKEY), '0'), 38, 10) is null and 
                    src:VOIDHANDLERKEY is not null) THEN
                    'VOIDHANDLERKEY contains a non-numeric value : \'' || AS_VARCHAR(src:VOIDHANDLERKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WEBFORMCONFIGKEY), '0'), 38, 10) is null and 
                    src:WEBFORMCONFIGKEY is not null) THEN
                    'WEBFORMCONFIGKEY contains a non-numeric value : \'' || AS_VARCHAR(src:WEBFORMCONFIGKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WEBFORMHANDLERKEY), '0'), 38, 10) is null and 
                    src:WEBFORMHANDLERKEY is not null) THEN
                    'WEBFORMHANDLERKEY contains a non-numeric value : \'' || AS_VARCHAR(src:WEBFORMHANDLERKEY) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null) THEN
                'VARIATION_ID contains a non-timestamp value : \'' || AS_VARCHAR(src:VARIATION_ID) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AUTHHANDLERKEY), '0'), 38, 10) is null and 
                    src:AUTHHANDLERKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AUTHORIZECONFIGKEY), '0'), 38, 10) is null and 
                    src:AUTHORIZECONFIGKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:AUTHORIZEPAYMENTFORMULAKEY), '0'), 38, 10) is null and 
                    src:AUTHORIZEPAYMENTFORMULAKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CARDNUMBERFORMAT), '0'), 38, 10) is null and 
                    src:CARDNUMBERFORMAT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EFFECTIVEDATE), '1900-01-01')) is null and 
                    src:EFFECTIVEDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EXPIREDATE), '1900-01-01')) is null and 
                    src:EXPIREDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HOSTPORT), '0'), 38, 10) is null and 
                    src:HOSTPORT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INQCONFIGKEY), '0'), 38, 10) is null and 
                    src:INQCONFIGKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INQHANDLERKEY), '0'), 38, 10) is null and 
                    src:INQHANDLERKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INQREVERSEDCONFIGKEY), '0'), 38, 10) is null and 
                    src:INQREVERSEDCONFIGKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INQREVERSEDHANDLERKEY), '0'), 38, 10) is null and 
                    src:INQREVERSEDHANDLERKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MERCHANTACCOUNTKEY), '0'), 38, 10) is null and 
                    src:MERCHANTACCOUNTKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYMENTPROCESSING), '0'), 38, 10) is null and 
                    src:PAYMENTPROCESSING is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PREAUTHHANDLERKEY), '0'), 38, 10) is null and 
                    src:PREAUTHHANDLERKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PREAUTHORIZECONFIGKEY), '0'), 38, 10) is null and 
                    src:PREAUTHORIZECONFIGKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PREPCONFIGKEY), '0'), 38, 10) is null and 
                    src:PREPCONFIGKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PREPHANDLERKEY), '0'), 38, 10) is null and 
                    src:PREPHANDLERKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REDIRECTURLCONFIGKEY), '0'), 38, 10) is null and 
                    src:REDIRECTURLCONFIGKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REDIRECTURLHANDLERKEY), '0'), 38, 10) is null and 
                    src:REDIRECTURLHANDLERKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REVERSECONFIGKEY), '0'), 38, 10) is null and 
                    src:REVERSECONFIGKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REVERSEHANDLERKEY), '0'), 38, 10) is null and 
                    src:REVERSEHANDLERKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REVERSEPAYMENTFORMULAKEY), '0'), 38, 10) is null and 
                    src:REVERSEPAYMENTFORMULAKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TIMEOUT), '0'), 38, 10) is null and 
                    src:TIMEOUT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VOIDCONFIGKEY), '0'), 38, 10) is null and 
                    src:VOIDCONFIGKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VOIDHANDLERKEY), '0'), 38, 10) is null and 
                    src:VOIDHANDLERKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WEBFORMCONFIGKEY), '0'), 38, 10) is null and 
                    src:WEBFORMCONFIGKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WEBFORMHANDLERKEY), '0'), 38, 10) is null and 
                    src:WEBFORMHANDLERKEY is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)