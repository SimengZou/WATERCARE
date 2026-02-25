CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_BILLING_BILLTYPE_ERROR AS SELECT src, 'IPS_BILLING_BILLTYPE' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPLYCREDITTYPE), '0'), 38, 10) is null and 
                    src:APPLYCREDITTYPE is not null) THEN
                    'APPLYCREDITTYPE contains a non-numeric value : \'' || AS_VARCHAR(src:APPLYCREDITTYPE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLINGFAMILY), '0'), 38, 10) is null and 
                    src:BILLINGFAMILY is not null) THEN
                    'BILLINGFAMILY contains a non-numeric value : \'' || AS_VARCHAR(src:BILLINGFAMILY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLMAXIMUM), '0'), 38, 10) is null and 
                    src:BILLMAXIMUM is not null) THEN
                    'BILLMAXIMUM contains a non-numeric value : \'' || AS_VARCHAR(src:BILLMAXIMUM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLMINIMUM), '0'), 38, 10) is null and 
                    src:BILLMINIMUM is not null) THEN
                    'BILLMINIMUM contains a non-numeric value : \'' || AS_VARCHAR(src:BILLMINIMUM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLTYPEKEY), '0'), 38, 10) is null and 
                    src:BILLTYPEKEY is not null) THEN
                    'BILLTYPEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:BILLTYPEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMMENTSKEY), '0'), 38, 10) is null and 
                    src:COMMENTSKEY is not null) THEN
                    'COMMENTSKEY contains a non-numeric value : \'' || AS_VARCHAR(src:COMMENTSKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CONDITIONFORMULAKEY), '0'), 38, 10) is null and 
                    src:CONDITIONFORMULAKEY is not null) THEN
                    'CONDITIONFORMULAKEY contains a non-numeric value : \'' || AS_VARCHAR(src:CONDITIONFORMULAKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CORRPROCESSSETUP), '0'), 38, 10) is null and 
                    src:CORRPROCESSSETUP is not null) THEN
                    'CORRPROCESSSETUP contains a non-numeric value : \'' || AS_VARCHAR(src:CORRPROCESSSETUP) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DELINQUENCYTYPE), '0'), 38, 10) is null and 
                    src:DELINQUENCYTYPE is not null) THEN
                    'DELINQUENCYTYPE contains a non-numeric value : \'' || AS_VARCHAR(src:DELINQUENCYTYPE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DELINQUENTDATEDAYS), '0'), 38, 10) is null and 
                    src:DELINQUENTDATEDAYS is not null) THEN
                    'DELINQUENTDATEDAYS contains a non-numeric value : \'' || AS_VARCHAR(src:DELINQUENTDATEDAYS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DELINQUENTDATEFORMULAKEY), '0'), 38, 10) is null and 
                    src:DELINQUENTDATEFORMULAKEY is not null) THEN
                    'DELINQUENTDATEFORMULAKEY contains a non-numeric value : \'' || AS_VARCHAR(src:DELINQUENTDATEFORMULAKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DISCNTPENDAYFORMULAKEY), '0'), 38, 10) is null and 
                    src:DISCNTPENDAYFORMULAKEY is not null) THEN
                    'DISCNTPENDAYFORMULAKEY contains a non-numeric value : \'' || AS_VARCHAR(src:DISCNTPENDAYFORMULAKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DISCOUNTPENALTYDAYS), '0'), 38, 10) is null and 
                    src:DISCOUNTPENALTYDAYS is not null) THEN
                    'DISCOUNTPENALTYDAYS contains a non-numeric value : \'' || AS_VARCHAR(src:DISCOUNTPENALTYDAYS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DISCOUNTPENALTYLISUKEY), '0'), 38, 10) is null and 
                    src:DISCOUNTPENALTYLISUKEY is not null) THEN
                    'DISCOUNTPENALTYLISUKEY contains a non-numeric value : \'' || AS_VARCHAR(src:DISCOUNTPENALTYLISUKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DISCOUNTPENALTYVARIANCE), '0'), 38, 10) is null and 
                    src:DISCOUNTPENALTYVARIANCE is not null) THEN
                    'DISCOUNTPENALTYVARIANCE contains a non-numeric value : \'' || AS_VARCHAR(src:DISCOUNTPENALTYVARIANCE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DUEDATEDAYS), '0'), 38, 10) is null and 
                    src:DUEDATEDAYS is not null) THEN
                    'DUEDATEDAYS contains a non-numeric value : \'' || AS_VARCHAR(src:DUEDATEDAYS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DUEDATEFORMULAKEY), '0'), 38, 10) is null and 
                    src:DUEDATEFORMULAKEY is not null) THEN
                    'DUEDATEFORMULAKEY contains a non-numeric value : \'' || AS_VARCHAR(src:DUEDATEFORMULAKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EFFDATE), '1900-01-01')) is null and 
                    src:EFFDATE is not null) THEN
                    'EFFDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:EFFDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EXPDATE), '1900-01-01')) is null and 
                    src:EXPDATE is not null) THEN
                    'EXPDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:EXPDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INCLUDESERVICEDATES), '0'), 38, 10) is null and 
                    src:INCLUDESERVICEDATES is not null) THEN
                    'INCLUDESERVICEDATES contains a non-numeric value : \'' || AS_VARCHAR(src:INCLUDESERVICEDATES) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MINCHARGEAMOUNT), '0'), 38, 10) is null and 
                    src:MINCHARGEAMOUNT is not null) THEN
                    'MINCHARGEAMOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:MINCHARGEAMOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYMENTAPPLICATIONRULE), '0'), 38, 10) is null and 
                    src:PAYMENTAPPLICATIONRULE is not null) THEN
                    'PAYMENTAPPLICATIONRULE contains a non-numeric value : \'' || AS_VARCHAR(src:PAYMENTAPPLICATIONRULE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYMENTORDER), '0'), 38, 10) is null and 
                    src:PAYMENTORDER is not null) THEN
                    'PAYMENTORDER contains a non-numeric value : \'' || AS_VARCHAR(src:PAYMENTORDER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PENALTYCODEKEY), '0'), 38, 10) is null and 
                    src:PENALTYCODEKEY is not null) THEN
                    'PENALTYCODEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:PENALTYCODEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:READINGSTRACKED), '0'), 38, 10) is null and 
                    src:READINGSTRACKED is not null) THEN
                    'READINGSTRACKED contains a non-numeric value : \'' || AS_VARCHAR(src:READINGSTRACKED) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) THEN
                    'VARIATION_ID contains a non-numeric value : \'' || AS_VARCHAR(src:VARIATION_ID) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VIEWBILLIMAGEFORMULA), '0'), 38, 10) is null and 
                    src:VIEWBILLIMAGEFORMULA is not null) THEN
                    'VIEWBILLIMAGEFORMULA contains a non-numeric value : \'' || AS_VARCHAR(src:VIEWBILLIMAGEFORMULA) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WAIVEPENALTYDEFAULTCODE), '0'), 38, 10) is null and 
                    src:WAIVEPENALTYDEFAULTCODE is not null) THEN
                    'WAIVEPENALTYDEFAULTCODE contains a non-numeric value : \'' || AS_VARCHAR(src:WAIVEPENALTYDEFAULTCODE) || '\'' WHEN 
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
                                    
                src:BILLTYPEKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_BILLTYPE as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPLYCREDITTYPE), '0'), 38, 10) is null and 
                    src:APPLYCREDITTYPE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLINGFAMILY), '0'), 38, 10) is null and 
                    src:BILLINGFAMILY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLMAXIMUM), '0'), 38, 10) is null and 
                    src:BILLMAXIMUM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLMINIMUM), '0'), 38, 10) is null and 
                    src:BILLMINIMUM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLTYPEKEY), '0'), 38, 10) is null and 
                    src:BILLTYPEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMMENTSKEY), '0'), 38, 10) is null and 
                    src:COMMENTSKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CONDITIONFORMULAKEY), '0'), 38, 10) is null and 
                    src:CONDITIONFORMULAKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CORRPROCESSSETUP), '0'), 38, 10) is null and 
                    src:CORRPROCESSSETUP is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DELINQUENCYTYPE), '0'), 38, 10) is null and 
                    src:DELINQUENCYTYPE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DELINQUENTDATEDAYS), '0'), 38, 10) is null and 
                    src:DELINQUENTDATEDAYS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DELINQUENTDATEFORMULAKEY), '0'), 38, 10) is null and 
                    src:DELINQUENTDATEFORMULAKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DISCNTPENDAYFORMULAKEY), '0'), 38, 10) is null and 
                    src:DISCNTPENDAYFORMULAKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DISCOUNTPENALTYDAYS), '0'), 38, 10) is null and 
                    src:DISCOUNTPENALTYDAYS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DISCOUNTPENALTYLISUKEY), '0'), 38, 10) is null and 
                    src:DISCOUNTPENALTYLISUKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DISCOUNTPENALTYVARIANCE), '0'), 38, 10) is null and 
                    src:DISCOUNTPENALTYVARIANCE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DUEDATEDAYS), '0'), 38, 10) is null and 
                    src:DUEDATEDAYS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DUEDATEFORMULAKEY), '0'), 38, 10) is null and 
                    src:DUEDATEFORMULAKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EFFDATE), '1900-01-01')) is null and 
                    src:EFFDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EXPDATE), '1900-01-01')) is null and 
                    src:EXPDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INCLUDESERVICEDATES), '0'), 38, 10) is null and 
                    src:INCLUDESERVICEDATES is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:MINCHARGEAMOUNT), '0'), 38, 10) is null and 
                    src:MINCHARGEAMOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYMENTAPPLICATIONRULE), '0'), 38, 10) is null and 
                    src:PAYMENTAPPLICATIONRULE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PAYMENTORDER), '0'), 38, 10) is null and 
                    src:PAYMENTORDER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PENALTYCODEKEY), '0'), 38, 10) is null and 
                    src:PENALTYCODEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:READINGSTRACKED), '0'), 38, 10) is null and 
                    src:READINGSTRACKED is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VIEWBILLIMAGEFORMULA), '0'), 38, 10) is null and 
                    src:VIEWBILLIMAGEFORMULA is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WAIVEPENALTYDEFAULTCODE), '0'), 38, 10) is null and 
                    src:WAIVEPENALTYDEFAULTCODE is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)