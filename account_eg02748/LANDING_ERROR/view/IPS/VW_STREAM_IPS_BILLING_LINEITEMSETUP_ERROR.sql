CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_BILLING_LINEITEMSETUP_ERROR AS SELECT src, 'IPS_BILLING_LINEITEMSETUP' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPROVALGROUP), '0'), 38, 10) is null and 
                    src:APPROVALGROUP is not null) THEN
                    'APPROVALGROUP contains a non-numeric value : \'' || AS_VARCHAR(src:APPROVALGROUP) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPROVALSCHEME), '0'), 38, 10) is null and 
                    src:APPROVALSCHEME is not null) THEN
                    'APPROVALSCHEME contains a non-numeric value : \'' || AS_VARCHAR(src:APPROVALSCHEME) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLINGFAMILY), '0'), 38, 10) is null and 
                    src:BILLINGFAMILY is not null) THEN
                    'BILLINGFAMILY contains a non-numeric value : \'' || AS_VARCHAR(src:BILLINGFAMILY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMMENTSKEY), '0'), 38, 10) is null and 
                    src:COMMENTSKEY is not null) THEN
                    'COMMENTSKEY contains a non-numeric value : \'' || AS_VARCHAR(src:COMMENTSKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CONDITIONFORMULAKEY), '0'), 38, 10) is null and 
                    src:CONDITIONFORMULAKEY is not null) THEN
                    'CONDITIONFORMULAKEY contains a non-numeric value : \'' || AS_VARCHAR(src:CONDITIONFORMULAKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DEFAULTADJUSTMENTSETUPKEY), '0'), 38, 10) is null and 
                    src:DEFAULTADJUSTMENTSETUPKEY is not null) THEN
                    'DEFAULTADJUSTMENTSETUPKEY contains a non-numeric value : \'' || AS_VARCHAR(src:DEFAULTADJUSTMENTSETUPKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DEFAULTQUANTITY), '0'), 38, 10) is null and 
                    src:DEFAULTQUANTITY is not null) THEN
                    'DEFAULTQUANTITY contains a non-numeric value : \'' || AS_VARCHAR(src:DEFAULTQUANTITY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EFFDATE), '1900-01-01')) is null and 
                    src:EFFDATE is not null) THEN
                    'EFFDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:EFFDATE) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EXPDATE), '1900-01-01')) is null and 
                    src:EXPDATE is not null) THEN
                    'EXPDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:EXPDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FEETYPEKEY), '0'), 38, 10) is null and 
                    src:FEETYPEKEY is not null) THEN
                    'FEETYPEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:FEETYPEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LINEITEMSETUPKEY), '0'), 38, 10) is null and 
                    src:LINEITEMSETUPKEY is not null) THEN
                    'LINEITEMSETUPKEY contains a non-numeric value : \'' || AS_VARCHAR(src:LINEITEMSETUPKEY) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMBEROFPROMPTPAIDBILLS), '0'), 38, 10) is null and 
                    src:NUMBEROFPROMPTPAIDBILLS is not null) THEN
                    'NUMBEROFPROMPTPAIDBILLS contains a non-numeric value : \'' || AS_VARCHAR(src:NUMBEROFPROMPTPAIDBILLS) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ONEOFFOPTIONINDICATOR), '0'), 38, 10) is null and 
                    src:ONEOFFOPTIONINDICATOR is not null) THEN
                    'ONEOFFOPTIONINDICATOR contains a non-numeric value : \'' || AS_VARCHAR(src:ONEOFFOPTIONINDICATOR) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PENALTYCODEKEY), '0'), 38, 10) is null and 
                    src:PENALTYCODEKEY is not null) THEN
                    'PENALTYCODEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:PENALTYCODEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PROMPTPAIDPERIOD), '0'), 38, 10) is null and 
                    src:PROMPTPAIDPERIOD is not null) THEN
                    'PROMPTPAIDPERIOD contains a non-numeric value : \'' || AS_VARCHAR(src:PROMPTPAIDPERIOD) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RATECHANGEALGORITHM), '0'), 38, 10) is null and 
                    src:RATECHANGEALGORITHM is not null) THEN
                    'RATECHANGEALGORITHM contains a non-numeric value : \'' || AS_VARCHAR(src:RATECHANGEALGORITHM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RATECODEKEY), '0'), 38, 10) is null and 
                    src:RATECODEKEY is not null) THEN
                    'RATECODEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:RATECODEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ROUNDTO), '0'), 38, 10) is null and 
                    src:ROUNDTO is not null) THEN
                    'ROUNDTO contains a non-numeric value : \'' || AS_VARCHAR(src:ROUNDTO) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SERVICEOPTIONSKEY), '0'), 38, 10) is null and 
                    src:SERVICEOPTIONSKEY is not null) THEN
                    'SERVICEOPTIONSKEY contains a non-numeric value : \'' || AS_VARCHAR(src:SERVICEOPTIONSKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SETTLEMENTLINEITEMSETUPKEY), '0'), 38, 10) is null and 
                    src:SETTLEMENTLINEITEMSETUPKEY is not null) THEN
                    'SETTLEMENTLINEITEMSETUPKEY contains a non-numeric value : \'' || AS_VARCHAR(src:SETTLEMENTLINEITEMSETUPKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SRVBASEDOPTIONINDICATOR), '0'), 38, 10) is null and 
                    src:SRVBASEDOPTIONINDICATOR is not null) THEN
                    'SRVBASEDOPTIONINDICATOR contains a non-numeric value : \'' || AS_VARCHAR(src:SRVBASEDOPTIONINDICATOR) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TYPEINDICATOR), '0'), 38, 10) is null and 
                    src:TYPEINDICATOR is not null) THEN
                    'TYPEINDICATOR contains a non-numeric value : \'' || AS_VARCHAR(src:TYPEINDICATOR) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VALUEFORMULAKEY), '0'), 38, 10) is null and 
                    src:VALUEFORMULAKEY is not null) THEN
                    'VALUEFORMULAKEY contains a non-numeric value : \'' || AS_VARCHAR(src:VALUEFORMULAKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) THEN
                    'VARIATION_ID contains a non-numeric value : \'' || AS_VARCHAR(src:VARIATION_ID) || '\'' WHEN 
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
                                    
                src:LINEITEMSETUPKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_LINEITEMSETUP as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPROVALGROUP), '0'), 38, 10) is null and 
                    src:APPROVALGROUP is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPROVALSCHEME), '0'), 38, 10) is null and 
                    src:APPROVALSCHEME is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BILLINGFAMILY), '0'), 38, 10) is null and 
                    src:BILLINGFAMILY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMMENTSKEY), '0'), 38, 10) is null and 
                    src:COMMENTSKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CONDITIONFORMULAKEY), '0'), 38, 10) is null and 
                    src:CONDITIONFORMULAKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DEFAULTADJUSTMENTSETUPKEY), '0'), 38, 10) is null and 
                    src:DEFAULTADJUSTMENTSETUPKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:DEFAULTQUANTITY), '0'), 38, 10) is null and 
                    src:DEFAULTQUANTITY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EFFDATE), '1900-01-01')) is null and 
                    src:EFFDATE is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:EXPDATE), '1900-01-01')) is null and 
                    src:EXPDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:FEETYPEKEY), '0'), 38, 10) is null and 
                    src:FEETYPEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LINEITEMSETUPKEY), '0'), 38, 10) is null and 
                    src:LINEITEMSETUPKEY is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:NUMBEROFPROMPTPAIDBILLS), '0'), 38, 10) is null and 
                    src:NUMBEROFPROMPTPAIDBILLS is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ONEOFFOPTIONINDICATOR), '0'), 38, 10) is null and 
                    src:ONEOFFOPTIONINDICATOR is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PENALTYCODEKEY), '0'), 38, 10) is null and 
                    src:PENALTYCODEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PROMPTPAIDPERIOD), '0'), 38, 10) is null and 
                    src:PROMPTPAIDPERIOD is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RATECHANGEALGORITHM), '0'), 38, 10) is null and 
                    src:RATECHANGEALGORITHM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:RATECODEKEY), '0'), 38, 10) is null and 
                    src:RATECODEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ROUNDTO), '0'), 38, 10) is null and 
                    src:ROUNDTO is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SERVICEOPTIONSKEY), '0'), 38, 10) is null and 
                    src:SERVICEOPTIONSKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SETTLEMENTLINEITEMSETUPKEY), '0'), 38, 10) is null and 
                    src:SETTLEMENTLINEITEMSETUPKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SRVBASEDOPTIONINDICATOR), '0'), 38, 10) is null and 
                    src:SRVBASEDOPTIONINDICATOR is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:TYPEINDICATOR), '0'), 38, 10) is null and 
                    src:TYPEINDICATOR is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VALUEFORMULAKEY), '0'), 38, 10) is null and 
                    src:VALUEFORMULAKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)