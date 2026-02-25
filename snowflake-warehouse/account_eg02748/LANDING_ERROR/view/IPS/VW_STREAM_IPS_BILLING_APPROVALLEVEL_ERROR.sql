CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_BILLING_APPROVALLEVEL_ERROR AS SELECT src, 'IPS_BILLING_APPROVALLEVEL' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPROPTIONSPECIFIC), '0'), 38, 10) is null and 
                    src:APPROPTIONSPECIFIC is not null) THEN
                    'APPROPTIONSPECIFIC contains a non-numeric value : \'' || AS_VARCHAR(src:APPROPTIONSPECIFIC) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPROVALLEVELKEY), '0'), 38, 10) is null and 
                    src:APPROVALLEVELKEY is not null) THEN
                    'APPROVALLEVELKEY contains a non-numeric value : \'' || AS_VARCHAR(src:APPROVALLEVELKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPROVALOPTION), '0'), 38, 10) is null and 
                    src:APPROVALOPTION is not null) THEN
                    'APPROVALOPTION contains a non-numeric value : \'' || AS_VARCHAR(src:APPROVALOPTION) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPROVALOPTIONFORMULA), '0'), 38, 10) is null and 
                    src:APPROVALOPTIONFORMULA is not null) THEN
                    'APPROVALOPTIONFORMULA contains a non-numeric value : \'' || AS_VARCHAR(src:APPROVALOPTIONFORMULA) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPROVALSCHEME), '0'), 38, 10) is null and 
                    src:APPROVALSCHEME is not null) THEN
                    'APPROVALSCHEME contains a non-numeric value : \'' || AS_VARCHAR(src:APPROVALSCHEME) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPROVEUPTOAMOUNT), '0'), 38, 10) is null and 
                    src:APPROVEUPTOAMOUNT is not null) THEN
                    'APPROVEUPTOAMOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:APPROVEUPTOAMOUNT) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BYPASSAMOUNT), '0'), 38, 10) is null and 
                    src:BYPASSAMOUNT is not null) THEN
                    'BYPASSAMOUNT contains a non-numeric value : \'' || AS_VARCHAR(src:BYPASSAMOUNT) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REJECTIONOPTION), '0'), 38, 10) is null and 
                    src:REJECTIONOPTION is not null) THEN
                    'REJECTIONOPTION contains a non-numeric value : \'' || AS_VARCHAR(src:REJECTIONOPTION) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REJOPTIONSPECIFIC), '0'), 38, 10) is null and 
                    src:REJOPTIONSPECIFIC is not null) THEN
                    'REJOPTIONSPECIFIC contains a non-numeric value : \'' || AS_VARCHAR(src:REJOPTIONSPECIFIC) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REJPTIONFORMULA), '0'), 38, 10) is null and 
                    src:REJPTIONFORMULA is not null) THEN
                    'REJPTIONFORMULA contains a non-numeric value : \'' || AS_VARCHAR(src:REJPTIONFORMULA) || '\'' WHEN 
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
                                    
                src:APPROVALLEVELKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_BILLING_APPROVALLEVEL as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPROPTIONSPECIFIC), '0'), 38, 10) is null and 
                    src:APPROPTIONSPECIFIC is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPROVALLEVELKEY), '0'), 38, 10) is null and 
                    src:APPROVALLEVELKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPROVALOPTION), '0'), 38, 10) is null and 
                    src:APPROVALOPTION is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPROVALOPTIONFORMULA), '0'), 38, 10) is null and 
                    src:APPROVALOPTIONFORMULA is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPROVALSCHEME), '0'), 38, 10) is null and 
                    src:APPROVALSCHEME is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:APPROVEUPTOAMOUNT), '0'), 38, 10) is null and 
                    src:APPROVEUPTOAMOUNT is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BYPASSAMOUNT), '0'), 38, 10) is null and 
                    src:BYPASSAMOUNT is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REJECTIONOPTION), '0'), 38, 10) is null and 
                    src:REJECTIONOPTION is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REJOPTIONSPECIFIC), '0'), 38, 10) is null and 
                    src:REJOPTIONSPECIFIC is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REJPTIONFORMULA), '0'), 38, 10) is null and 
                    src:REJPTIONFORMULA is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)