CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_BILLING_APPROVALSCHEME_ERROR AS SELECT src, 'IPS_BILLING_APPROVALSCHEME' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPROVALGROUP), '0'), 38, 10) is null and 
                    src:APPROVALGROUP is not null) THEN
                    'APPROVALGROUP contains a non-numeric value : \'' || AS_VARCHAR(src:APPROVALGROUP) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPROVALSCHEMEKEY), '0'), 38, 10) is null and 
                    src:APPROVALSCHEMEKEY is not null) THEN
                    'APPROVALSCHEMEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:APPROVALSCHEMEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPROVALSTYLE), '0'), 38, 10) is null and 
                    src:APPROVALSTYLE is not null) THEN
                    'APPROVALSTYLE contains a non-numeric value : \'' || AS_VARCHAR(src:APPROVALSTYLE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EARLYFINALAPPROVALFORMULA), '0'), 38, 10) is null and 
                    src:EARLYFINALAPPROVALFORMULA is not null) THEN
                    'EARLYFINALAPPROVALFORMULA contains a non-numeric value : \'' || AS_VARCHAR(src:EARLYFINALAPPROVALFORMULA) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EARLYREJECTIONFORMULA), '0'), 38, 10) is null and 
                    src:EARLYREJECTIONFORMULA is not null) THEN
                    'EARLYREJECTIONFORMULA contains a non-numeric value : \'' || AS_VARCHAR(src:EARLYREJECTIONFORMULA) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INITIALLEVELFORMULA), '0'), 38, 10) is null and 
                    src:INITIALLEVELFORMULA is not null) THEN
                    'INITIALLEVELFORMULA contains a non-numeric value : \'' || AS_VARCHAR(src:INITIALLEVELFORMULA) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:POSTFINALAPPROVALFORMULA), '0'), 38, 10) is null and 
                    src:POSTFINALAPPROVALFORMULA is not null) THEN
                    'POSTFINALAPPROVALFORMULA contains a non-numeric value : \'' || AS_VARCHAR(src:POSTFINALAPPROVALFORMULA) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:POSTREJECTIONFORMULA), '0'), 38, 10) is null and 
                    src:POSTREJECTIONFORMULA is not null) THEN
                    'POSTREJECTIONFORMULA contains a non-numeric value : \'' || AS_VARCHAR(src:POSTREJECTIONFORMULA) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PRIORITYORDER), '0'), 38, 10) is null and 
                    src:PRIORITYORDER is not null) THEN
                    'PRIORITYORDER contains a non-numeric value : \'' || AS_VARCHAR(src:PRIORITYORDER) || '\'' WHEN 
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
                                    
                src:APPROVALSCHEMEKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_APPROVALSCHEME as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPROVALGROUP), '0'), 38, 10) is null and 
                    src:APPROVALGROUP is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPROVALSCHEMEKEY), '0'), 38, 10) is null and 
                    src:APPROVALSCHEMEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPROVALSTYLE), '0'), 38, 10) is null and 
                    src:APPROVALSTYLE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EARLYFINALAPPROVALFORMULA), '0'), 38, 10) is null and 
                    src:EARLYFINALAPPROVALFORMULA is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EARLYREJECTIONFORMULA), '0'), 38, 10) is null and 
                    src:EARLYREJECTIONFORMULA is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INITIALLEVELFORMULA), '0'), 38, 10) is null and 
                    src:INITIALLEVELFORMULA is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:POSTFINALAPPROVALFORMULA), '0'), 38, 10) is null and 
                    src:POSTFINALAPPROVALFORMULA is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:POSTREJECTIONFORMULA), '0'), 38, 10) is null and 
                    src:POSTREJECTIONFORMULA is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PRIORITYORDER), '0'), 38, 10) is null and 
                    src:PRIORITYORDER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)